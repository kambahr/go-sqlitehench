package sqlitehench

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	collc "github.com/kambahr/go-collections"
)

func (d *DBAccess) GetDataTable(sqlQuery string, dbFilePath string) (*collc.Table, error) {

	return d.getDataTable(sqlQuery, dbFilePath, "")
}

func (d *DBAccess) GetDataTableJSON(tbl *collc.Table) string {
	cols := tbl.Cols.Get()
	rows := tbl.Rows.GetMap()

	var jsnArry []string

	for i := 0; i < len(rows); i++ {
		var sa []string
		for j := 0; j < len(cols); j++ {
			v := rows[i][cols[j].Name]
			if fmt.Sprintf("%v", cols[j].Type) == "string" {
				s := fmt.Sprintf("%v", v)
				if !strings.HasPrefix(s, "{") {
					v = fmt.Sprintf(`"%v"`, s)
				}
			}
			sa = append(sa, fmt.Sprintf(`"%s":%v`, cols[j].Name, v))
		}
		oneJsn := fmt.Sprintf(`{%s}`, strings.Join(sa, ","))

		jsnArry = append(jsnArry, oneJsn)
	}
	allJson := fmt.Sprintf(`[%s]`, strings.Join(jsnArry, ","))

	return allJson
}

func (d *DBAccess) GetDataTableWithTag(sqlQuery string, dbFilePath string, tag string) (*collc.Table, error) {

	return d.getDataTable(sqlQuery, dbFilePath, tag)
}

func (dc *DBAccess) GetDataTableLongQuery(sqlQuery string, dbFilePath string, pageSize int, notify func(status LonqQueryArgs)) (*collc.Table, error) {

	if pageSize < 1 {
		pageSize = 1
	}

	var lqArgs LonqQueryArgs
	var sqlRCount string
	var coll = collc.NewCollection()

	// Make a new instance for this.
	var pragma []string = []string{
		"PRAGMA journal_mode = WALL;",
		"PRAGMA synchronous = OFF;",
	}

	d := NewDBAccess(DBAccess{
		MaxIdleConns: 100, MaxOpenConns: 100,
		ShrinkDatabaseFiles: false,
		PRAGMA:              pragma,
	})

	if !fileOrDirExists(dbFilePath) {
		return nil, errors.New(Err_DatabaseFileNotExists)
	}
	sqlQuery = strings.ReplaceAll(sqlQuery, "\n", " ")
	sqlQuery = strings.ReplaceAll(sqlQuery, "\t", " ")
	sqlRCount = fmt.Sprintf("select count(*) from (%s)", sqlQuery)

	// Get the record count
	mx, err := d.ExecuteScalare(sqlRCount, dbFilePath)
	if err != nil {
		return nil, err
	}
	if mx == nil || mx.(int64) < 1 {
		return nil, errors.New(Err_NoRowsFound)
	}
	recCnt := int(mx.(int64))

	// GetPageOffset returns totalPages, offset, pageNo
	totalPages, offset, _ := dc.GetPageOffset(recCnt, pageSize, 1)

	if recCnt < 1 {
		return nil, errors.New("source data-table has no rows")
	}

	dtLng, err := coll.Table.Create("x23343")
	if err != nil {
		return nil, err
	}

	var allRowsAffected int64
	var dt *collc.Table
	tstart := time.Now()
	for i := 0; i < totalPages; i++ {

		_, offset, _ = dc.GetPageOffset(recCnt, pageSize, i+1)

		sqlx := fmt.Sprintf("select * from (%s) limit %d offset %d", sqlQuery, pageSize, offset)
		dt, err = d.GetDataTable(sqlx, dbFilePath)
		if err != nil {
			return nil, err
		}
		dtLng, err = dc.AppendDataTable(dt, dtLng)
		allRowsAffected += int64(dt.Rows.Count())
		if notify != nil {

			lqArgs.ResultTable = dtLng
			lqArgs.RowsFetched = allRowsAffected
			lqArgs.TotalToFetch = int64(recCnt)

			msg := fmt.Sprintf("copied => rows: %s of %s, elapsed: %v",
				formatNumber(allRowsAffected), formatNumber(int64(recCnt)), durationToString(time.Since(tstart)))

			lqArgs.Status = msg

			go notify(lqArgs)
		}
		offset += pageSize + 1
	}

	return dtLng, nil
}

// CloneDataTable retuns a copy of a DataTable.
func (d *DBAccess) CloneDataTable(dtSrce *collc.Table) (*collc.Table, error) {

	var coll = collc.NewCollection()
	var err error

	tableName := dtSrce.Name
	if tableName == "" {
		return nil, errors.New("malfomred DataTable; table-name not found")
	}

	dtDest, err := coll.Table.Create(tableName)
	if err != nil {
		return nil, errors.New(err.Error())
	}
	srcCols := dtSrce.Cols.Get()
	rows := dtSrce.Rows.GetMap()

	for i := 0; i < len(srcCols); i++ {
		dtDest.Cols.Add(srcCols[i].Name)
	}

	for i := 0; i < len(srcCols); i++ {
		dtDest.Cols.Add(srcCols[i].Name)
	}

	for i := 0; i < len(rows); i++ {
		oneRow := dtDest.Rows.Add()
		for j := 0; j < len(srcCols); j++ {
			oneRow[srcCols[j].Name] = rows[i][srcCols[j].Name]
		}
	}

	return dtDest, err
}

// GetDataTableRange selects a ragne from a DataTable.
func (d *DBAccess) GetDataTableRange(dtSrc *collc.Table, from int, to int) (*collc.Table, error) {

	var coll = collc.NewCollection()
	var err error

	tableName := dtSrc.Name
	if tableName == "" {
		return nil, errors.New("malfomred DataTable; table-name not found")
	}

	dtDest, err := coll.Table.Create(tableName)
	if err != nil {
		return nil, err
	}

	srcCols := dtSrc.Cols.Get()
	rows := dtSrc.Rows.GetMap()

	for i := 0; i < len(srcCols); i++ {
		dtDest.Cols.Add(srcCols[i].Name)
	}

	srcRowCnt := dtSrc.Rows.Count()
	if to >= srcRowCnt {
		to = srcRowCnt - 1
	}

	for i := 0; i < len(srcCols); i++ {
		dtDest.Cols.Add(srcCols[i].Name)
	}

	for i := from; i <= to; i++ {
		oneRow := dtDest.Rows.Add()
		for j := 0; j < len(srcCols); j++ {
			oneRow[srcCols[j].Name] = rows[i][srcCols[j].Name]
		}
	}

	return dtDest, err
}

func (d *DBAccess) AppendDataTable(dtSrce *collc.Table, dtDest *collc.Table) (*collc.Table, error) {

	var err error

	tableName := dtSrce.Name
	if tableName == "" {
		return nil, errors.New("malfomred DataTable; table-name not found")
	}

	destCols := dtDest.Cols.Get()
	srcCols := dtSrce.Cols.Get()
	rows := dtSrce.Rows.GetMap()

	if dtDest.Cols.Count() == 0 {
		for i := 0; i < len(srcCols); i++ {
			dtDest.Cols.Add(srcCols[i].Name)
		}
		destCols = dtDest.Cols.Get()
	}

	// src and dest cols have to match; in the same order.
	for i := 0; i < len(srcCols); i++ {
		if srcCols[i].Name != destCols[i].Name {
			return nil, errors.New("source and destination columns must match")
		}
	}

	for i := 0; i < len(rows); i++ {
		oneRow := dtDest.Rows.Add()
		for j := 0; j < len(srcCols); j++ {
			oneRow[srcCols[j].Name] = rows[i][srcCols[j].Name]
		}
	}

	return dtDest, err
}

// InsertDataTable inserts a DataTable collection into a datbase table..
func (d *DBAccess) InsertDataTable(t *collc.Table, dbFilePath string) (int64, error) {

	var err error

	rowCount := t.Rows.Count()

	if rowCount < 1 {
		return -1, errors.New(Err_NoRowsFound)
	}

	if err = d.validateInsertEntry(t, dbFilePath); err != nil {
		return -1, err
	}

	t.Name = strings.Trim(t.Name, " ")

	// Get the DataTable columns
	cols := t.Cols.Get()

	colName := make([]string, len(cols))
	for i := 0; i < len(cols); i++ {
		if !strings.HasPrefix(cols[i].Name, "[") {
			colName[i] = fmt.Sprintf("[%s]", cols[i].Name)
		} else {
			colName[i] = cols[i].Name
		}
	}

	// Wrap the table name to avoid keyword clashes (i.e. Group)
	tName := t.Name
	if !strings.HasPrefix(t.Name, "[") {
		tName = fmt.Sprintf("[%s]", t.Name)
	}

	// Get the database columns.
	sqlx := fmt.Sprintf("select * from %s limit 1", tName)
	tMaster, err := d.GetDataTable(sqlx, dbFilePath)
	if err != nil {
		return -1, err
	}

	destCols := tMaster.Cols.Get()

	var allRowsAffected int64
	var rowsAffected int64

	db, err := d.GetDB(dbFilePath)
	if err != nil {
		return -1, err
	}
	defer db.Close()

	ctx := context.Background()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return -1, err
	}

	for k := 0; k < rowCount; k++ {
		rowsAffected, err = d.insertOneDataTableRowPointToDB(ctx, tx, t, tName, k, cols, destCols, db)
		if err != nil {
			tx.Rollback()
			return -1, err
		}

		allRowsAffected = allRowsAffected + rowsAffected
	}

	tx.Commit()

	return allRowsAffected, nil
}
func (d *DBAccess) insertOneDataTableRow(t *collc.Table, tName string, k int, cols []collc.Column, destCols []collc.Column, dbFilePath string, wg *sync.WaitGroup) (int64, error) {

	if wg != nil {
		defer wg.Done()
	}

	var err error
	var rowsAffected int64

	var inserts []string
	var values []string

	// A row must have atleat one none-null value.
	atleastOneNoneNULL := false

	for i := 0; i < len(cols); i++ {

		// Omit the columns that do not exist in the table
		ignoreCase := true
		existsInDb := valueExistsInArry(destCols, cols[i].Name, ignoreCase)

		// skip - if the col name from source, does not exist in the database.
		// Here is where SQL injection gets rejected!
		if !existsInDb {
			continue
		}

		// get one value
		iValue := t.Rows.GetArray()[k][i]

		// column name for insert
		ins := fmt.Sprintf("[%s]", cols[i].Name)

		inserts = append(inserts, ins)
		var p string
		t := fmt.Sprintf("%v", cols[i].Type)
		if t == "string" {
			if iValue == nil {
				p = "NULL"
			} else {
				var s string
				atleastOneNoneNULL = true
				s = strings.ReplaceAll(iValue.(string), "<nil>", "")
				p = fmt.Sprintf("'%s'", strings.ReplaceAll(s, "'", "''"))
			}
		} else {
			if iValue == nil {
				p = "NULL"
			} else {
				atleastOneNoneNULL = true
				p = fmt.Sprintf("%v", iValue)
			}
		}
		values = append(values, p)
	}
	if len(inserts) == 0 || !atleastOneNoneNULL {
		// Nothing was found to insert
		return 0, nil

	}
	sqlx := fmt.Sprintf("insert into %s (%s) values(%s)", tName, strings.Join(inserts, ","), strings.Join(values, ","))
	rowsAffected = 0

	rowsAffected, err = d.ExecuteNonQuery(sqlx, dbFilePath)
	if err != nil {
		return -1, err
	}

	return rowsAffected, err
}
func (d *DBAccess) insertOneDataTableRowPointToDB(ctx context.Context, tx *sql.Tx, t *collc.Table, tName string, k int,
	cols []collc.Column, destCols []collc.Column, db *sql.DB) (int64, error) {

	var err error
	var rowsAffected int64

	var inserts []string
	var values []string

	// A row must have atleat one none-null value.
	atleastOneNoneNULL := false

	for i := 0; i < len(cols); i++ {

		// Omit the columns that do not exist in the table
		ignoreCase := true
		existsInDb := valueExistsInArry(destCols, cols[i].Name, ignoreCase)

		// skip - if the col name from source, does not exist in the database.
		// Here is where SQL injection gets rejected!
		if !existsInDb {
			continue
		}

		// get one value
		iValue := t.Rows.GetArray()[k][i]

		// column name for insert
		ins := fmt.Sprintf("[%s]", cols[i].Name)

		inserts = append(inserts, ins)
		var p string
		t := fmt.Sprintf("%v", cols[i].Type)
		if t == "string" {
			if iValue == nil {
				p = "NULL"
			} else {
				var s string
				atleastOneNoneNULL = true
				s = strings.ReplaceAll(iValue.(string), "<nil>", "")
				p = fmt.Sprintf("'%s'", strings.ReplaceAll(s, "'", "''"))
			}
		} else {
			if iValue == nil {
				p = "NULL"
			} else {
				if reflect.TypeOf(iValue).Kind() == reflect.String {
					// possible sql-injection attempt; pass as string.
					// Let SQLite engine throw error if any.
					s := strings.ReplaceAll(iValue.(string), "<nil>", "")
					p = fmt.Sprintf("'%s'", strings.ReplaceAll(s, "'", "''"))
					p = fmt.Sprintf("'%s'", s)
				} else {
					atleastOneNoneNULL = true
					p = fmt.Sprintf("%v", iValue)

				}
			}
		}
		values = append(values, p)
	}
	if len(inserts) == 0 || !atleastOneNoneNULL {
		// Nothing was found to insert
		return 0, nil

	}
	sqlx := fmt.Sprintf("insert into %s (%s) values(%s)", tName, strings.Join(inserts, ","), strings.Join(values, ","))
	rowsAffected = 0

	result, err := tx.ExecContext(ctx, sqlx)
	if err != nil {
		tx.Rollback()
		return -1, err
	}

	if result != nil {
		rowsAffected, err = result.RowsAffected()
		if err != nil {
			tx.Rollback()
			return -1, err
		}
	}

	return rowsAffected, err
}

// GetDataMap gets a selected range of table in form of rows and columns.
func (d *DBAccess) getDataTable(sqlQuery string, dbFilePath string, tag string) (*collc.Table, error) {

	if d.ShrinkDatabaseFiles {
		// Read operation; but still add to the list - as some
		// write operations may have taken a long time... and still
		// good to check on those files to be shrunk.
		go d.AddDBFileToShrinkWatchList(dbFilePath)
	}

	var coll = collc.NewCollection()
	var db *sql.DB
	var err error

	if db, err = d.GetDB(dbFilePath); err != nil {
		return nil, err
	}

	tableName := d.getTableNameFromSQLQuery(sqlQuery)
	if tableName == "" {
		return nil, errors.New("malfomred query; table-name not found")
	}

	tbl, err := coll.Table.Create(tableName)
	if err != nil {
		return nil, errors.New(err.Error())
	}

	sqlQuery = fixSQLQuery(sqlQuery)

	rows, err := db.Query(sqlQuery)
	if err != nil {
		return nil, err
	}
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	for i := 0; i < len(cols); i++ {
		tbl.Cols.Add(cols[i])
	}

	columns := make([]interface{}, len(cols))
	columnPointers := make([]interface{}, len(cols))

	for i := 0; i < len(columns); i++ {
		columnPointers[i] = &columns[i]
	}

	for rows.Next() {

		// tag is initially blank. The caller will tag selected rows
		// after receiving the table.
		oneRow := tbl.Rows.AddWithTag(tag)

		if err := rows.Scan(columnPointers...); err != nil {
			return nil, err
		}

		for i := 0; i < len(cols); i++ {
			val := columnPointers[i].(*interface{})
			oneRow[cols[i]] = *val
		}
	}

	rows.Close()

	db.Close()

	return tbl, nil
}
