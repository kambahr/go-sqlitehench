// Copyright (c) 2021 Kamiar Bahri
package sqlitehench

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	collc "github.com/kambahr/go-collections"

	// Copyright (c) 2014 Yasuhiro Matsumoto
	// MIT License can be found on:
	// https://github.com/mattn/go-sqlite3/blob/master/LICENSE
	_ "github.com/mattn/go-sqlite3"
)

// GetDB opens a database, while attempting to clear a lingering lock on an
// sqlite database file. If the db is locked or the previous call did not
// close the db after writing, this will close the db -- and reset the db
// mode for read/write operations.
func (d *DBAccess) GetDB(dbFilePath string) (*sql.DB, error) {

	db, err := sql.Open(d.driverName, dbFilePath)
	if db != nil {
		// Close first.
		db.Close()

		// Re-open.
		db, err = sql.Open(d.driverName, dbFilePath)
		if err != nil {
			return nil, err
		}
	}

	if err != nil {
		if strings.Contains(err.Error(), Err_DatabaseIsLocked) {
			// Try to close the lingering connection once; as the lock
			// might have already been removed.
			db.Close()
			db, err = sql.Open(d.driverName, dbFilePath)
			if err != nil {
				db.Close()
				return db, err
			}
			// Succeeded; db lock is gone.
		} else {
			db.Close()
			return db, err
		}
	}

	// Apply the PRAGMA here.
	for i := 0; i < len(d.PRAGMA); i++ {
		db.Exec(d.PRAGMA[i])
	}

	db.SetMaxIdleConns(int(d.MaxIdleConns))
	db.SetMaxOpenConns(int(d.MaxOpenConns))

	return db, nil
}

// DatabaseExists checks existance of the db file.
func (d *DBAccess) DatabaseExists(path string) bool {
	if path == "" {
		return false
	}

	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false
	}

	return true
}

// ExecuteScalare returns one value and closes the database.
func (d *DBAccess) ExecuteScalare(sqlStatement string, dbFilePath string) (interface{}, error) {

	if d.ShrinkDatabaseFiles {
		// ExecuteScalare is a read operation; but still add
		// to the list - as some write operations may have taken
		// a long time... and still good to check on those files to be shrunk.
		if !d.itemExists(dbFilePath) {
			go d.AddDBFileToShrinkWatchList(dbFilePath)
		}
	}

	var db *sql.DB
	var err error
	var item interface{}

	if db, err = d.GetDB(dbFilePath); err != nil {
		return nil, err
	}

	item, err = executeScalare(sqlStatement, db)

	db.Close()

	return item, err
}

func (d *DBAccess) GetTableCount(tableName string, dbFilePath string) (int64, error) {

	sqlx := fmt.Sprintf("select count(*) from [%s]", tableName)
	m, err := d.ExecuteScalare(sqlx, dbFilePath)
	if err != err {
		return -1, err
	}

	return m.(int64), nil
}

// ExecuteScalare returns one value and closes the database.
func (d *DBAccess) ExecuteScalarePointToDB(sqlStatement string, db *sql.DB) (interface{}, error) {

	item, err := executeScalare(sqlStatement, db)

	return item, err
}

func (d *DBAccess) fixQuery(sqlx string) string {
	sqlx = strings.ReplaceAll(sqlx, "\n", " ")
	sqlx = strings.ReplaceAll(sqlx, "\t", " ")

	return sqlx
}

// ExecuteNonQuery inserts data. It uses a transaction context so that
// the operation is rolled back on failures and then closes the database.
// Closing the databases has the following advantages for an SQLite database:
//  1. It reduces chances of file corruption.
//  2. It reduces chances of "database locked" errors.
//  3. It reduces lingering locks, where the database file stays locked
//     albite closing all database handles.
func (d *DBAccess) ExecuteNonQuery(sqlStatement string, dbFilePath string) (int64, error) {

	if d.ShrinkDatabaseFiles && !d.itemExists(dbFilePath) {
		go d.AddDBFileToShrinkWatchList(dbFilePath)
	}

	var db *sql.DB
	var err error

	if db, err = d.GetDB(dbFilePath); err != nil {
		return -1, err
	}

	sqlStatement = d.fixQuery(sqlStatement)

	// this is a rough estimate (https://sqlite.org/limits.html),
	// but it'd be good to prevent this to go thru.
	if len(sqlStatement) > 1000000000 {
		db.Close()
		return -1, errors.New("query length exceeded max length of 1000000000 bytes")
	}

	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		db.Close()
		return -1, err
	}

	result, err := tx.ExecContext(ctx, sqlStatement)
	if err != nil {
		tx.Rollback()
		db.Close()
		return -1, err
	}

	var rowsAffected int64 = -1

	if result != nil {
		rowsAffected, err = result.RowsAffected()
		if err != nil {
			tx.Rollback()
			db.Close()
			return -1, err
		}
	}
	tx.Commit()

	db.Close()

	return rowsAffected, err
}

func (d *DBAccess) ExecuteNonQueryNoTxPointToDB(sqlStatement string, db *sql.DB) (int64, error) {

	if len(sqlStatement) > 1000000000 {
		return -1, errors.New("query length exceeded max length of 1000000000 bytes")
	}

	result, err := db.Exec(sqlStatement)
	if err != nil {
		return -1, err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return -1, err
	}

	return rowsAffected, nil
}

// ExecuteNonQueryNoTx uses no transaction context to insert data.
func (d *DBAccess) ExecuteNonQueryNoTx(sqlStatement string, dbFilePath string) (int64, error) {

	if d.ShrinkDatabaseFiles && !d.itemExists(dbFilePath) {
		go d.AddDBFileToShrinkWatchList(dbFilePath)
	}

	var db *sql.DB
	var err error

	if db, err = d.GetDB(dbFilePath); err != nil {
		return -1, err
	}

	result, err := db.Exec(sqlStatement)
	if err != nil {
		db.Close()
		return -1, err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		db.Close()
		return -1, err
	}

	db.Close()

	return rowsAffected, nil
}

// ExecuteNonQueryPointToDB inserts data. It does not close
// the database after operation is completed.
func (d *DBAccess) ExecuteNonQueryPointToDB(sqlStatement string, db *sql.DB) (int64, error) {

	rowsAffected, err := executeNonQuery(sqlStatement, db)

	// Keep the db open.

	return rowsAffected, err
}

// getTableNameFromSQLQuery parses the tables name out of an SQL statement.
func (d *DBAccess) getTableNameFromSQLQuery(sqlQuery string) string {
	sqlQueryLower := strings.ToLower(sqlQuery)
	x := strings.Split(sqlQueryLower, " ")

	y := make([]interface{}, len(x))
	for i, v := range x {
		y[i] = v
	}

	vx := removeElmFrmArry(y, "")

	count := len(vx)
	for i := 0; i < count; i++ {
		vxstr := vx[i].(string)
		if vxstr == "from" || vxstr == "into" {
			if (i + 1) < count {
				v := vx[i+1].(string)
				if strings.HasSuffix(v, ";") {
					v = v[:len(v)-1]
				}

				tblName := strings.TrimSpace(strings.Split(v, "\n")[0])
				return tblName
			}
		}
	}
	return ""
}

// validateInsertEntry
func (d *DBAccess) validateInsertEntry(t *collc.Table, dbFilePath string) error {
	if t.Name == "" {
		return errors.New("table name is reuiqred; and it must match the table-name in the database")
	}
	if t.Rows.Count() < 1 {
		return errors.New("table contains no rows")
	}

	if !fileOrDirExists(dbFilePath) {
		return errors.New("database file does not exist")
	}

	// Find columns in the datbase table
	sqlx := `SELECT name FROM sqlite_master WHERE type IN ('table','view') AND name NOT LIKE 'sqlite_%' ORDER BY 1;`
	tMaster, err := d.GetDataTable(sqlx, dbFilePath)

	if err != nil {
		return err
	}

	tTbleName := strings.ReplaceAll(t.Name, "[", "")
	tTbleName = strings.ToLower(strings.ReplaceAll(tTbleName, "]", ""))

	tblFound := false
	cols := tMaster.Cols.Get()
	rowArry := tMaster.Rows.GetRows()
	for i := 0; i < len(rowArry); i++ {
		for j := 0; j < len(cols); j++ {
			tblName := rowArry[i][cols[j].Name]
			if tblName == nil {
				continue
			}
			sl := strings.ToLower(tblName.(string))

			if sl == tTbleName {
				tblFound = true
				goto lblContinue
			}
		}
	}
lblContinue:

	if !tblFound {
		return errors.New(fmt.Sprintf("could not find table %s in database file %s", t.Name, dbFilePath))
	}

	return nil
}

func valueExistsInArry(arry []collc.Column, e string, ignoreCase bool) bool {
	for i := 0; i < len(arry); i++ {
		if ignoreCase {
			if strings.ToLower(arry[i].Name) == strings.ToLower(e) {
				return true
			}
		} else {
			if arry[i].Name == e {
				return true
			}
		}
	}
	return false
}

// BulkInsert inserts a DataTable into a database.
func (dc *DBAccess) BulkInsert(dtSrc *collc.Table, dbFilePath string /*fast bool,*/, notify func(status string)) error {

	var err error

	// Make a new instance for this.
	var pragma []string = []string{
		"PRAGMA journal_mode = MEMORY;",
		"PRAGMA synchronous = OFF;",
	}

	d := NewDBAccess(DBAccess{
		MaxIdleConns: 100, MaxOpenConns: 100,
		ShrinkDatabaseFiles: false,
		PRAGMA:              pragma,
	})

	_, err = d.CreateNewDatabase(dtSrc, dbFilePath)
	if err != nil {
		return err
	}

	// anything more than one will acutally slow thing down!
	pageSize := 1

	tblSrcRecordCount := dtSrc.Rows.Count()

	totalPages := tblSrcRecordCount / pageSize
	if tblSrcRecordCount%pageSize != 0 {
		totalPages++
	}

	if tblSrcRecordCount < 1 {
		return errors.New("source data-table has no rows")
	}

	from := 0
	to := pageSize - 1
	tstart := time.Now()

	var allRowsAffected, rowsAffected int64
	fmtTblRecCnt := formatNumber(int64(tblSrcRecordCount))

	for i := 0; i < tblSrcRecordCount; i++ {
		if to > tblSrcRecordCount {
			to = tblSrcRecordCount
		}

		var coll = collc.NewCollection()
		dtDest, err := coll.Table.Create(dtSrc.Name)
		if err != nil {
			return err
		}

		err = d.GetDataTableRange(dtSrc, dtDest, from, to)
		if err != nil {
			return err
		}
		rowsAffected, err = d.InsertDataTable(dtDest, dbFilePath, nil)
		if err != nil {
			if err.Error() == Err_NoRowsFound {
				return nil
			}
			return err
		}

		from += pageSize
		to = from + (pageSize - 1)

		if notify != nil {
			allRowsAffected += rowsAffected
			go createNotifyMsg(allRowsAffected, fmtTblRecCnt, tstart, notify)
		}
	}

	return nil
}
func createNotifyMsg(allRowsAffected int64, fmtTblRecCnt string, tstart time.Time, notify func(status string)) {
	msg := fmt.Sprintf("copied => rows: %s of %s, elapsed: %v",
		formatNumber(allRowsAffected), fmtTblRecCnt, durationToString(time.Since(tstart)))

	notify(msg)
}

// CloneDatabase copies one database to the other.
func (dc *DBAccess) CloneDatabase(srcFilePath string, destFilePath string, notify func(status string)) error {

	// Make a new instance for this.
	var prag []string = []string{
		//"PRAGMA journal_mode = MEMORY;",
		"PRAGMA synchronous = OFF;",
	}

	d := NewDBAccess(DBAccess{
		MaxIdleConns: 100, MaxOpenConns: 100,
		ShrinkDatabaseFiles: false,
		PRAGMA:              prag,
	})

	if !fileOrDirExists(srcFilePath) {
		return errors.New("source file does not exist")
	}

	if srcFilePath == destFilePath {
		return errors.New("source and destination cannot be the same")
	}

	if fileOrDirExists(destFilePath) {
		err := os.Remove(destFilePath)
		if err != nil {
			return err
		}
	}

	tstart := time.Now()

	// Get the count
	sqlx := "SELECT [sql],[name],[type] FROM sqlite_master"
	m, err := d.GetDataMap(sqlx, srcFilePath)
	if err != nil {
		return err
	}

	var tables []string
	pageSize := 30
	colName := "_rowid_"

	for i := 0; i < len(m); i++ {
		sqlx = fmt.Sprintf("%v", m[i]["sql"])
		if sqlx == "<nil>" {
			continue
		}
		if !strings.Contains(sqlx, " sqlite_sequence") {
			_, err := d.ExecuteNonQuery(sqlx, destFilePath)
			if err != nil {
				return err
			}
		}

		if m[i]["type"].(string) == "table" {
			tables = append(tables, m[i]["name"].(string))
		}
	}

	// Go through all tables page by page
	var allRowsCopied int64
	for k := 0; k < len(tables); k++ {

		p, offset, ci := d.GetPagingInfo(pageSize, 1, tables[k], colName, "", srcFilePath)
		var rowsCopiedTable int64
		for i := 0; i < ci.TotalPages; i++ {

			pageSize, offset, ci = d.GetPagingInfo(p, (i + 1), tables[k], colName, "", srcFilePath)

			sqlx = fmt.Sprintf("select * from [%s] order by %s limit %d offset %d", tables[k], colName, pageSize, offset)

			dt, err := d.GetDataTable(sqlx, srcFilePath)
			if err != nil {
				return err
			}
			rowsAffected, err := d.InsertDataTable(dt, destFilePath, nil)
			if err != nil {
				return err
			}

			if notify != nil {
				rowsCopiedTable += rowsAffected
				allRowsCopied += rowsAffected

				msg := fmt.Sprintf("rows copied => %s: %v, total: %v, elapsed: %v", dt.Name,
					formatNumber(rowsCopiedTable), formatNumber(allRowsCopied), durationToString(time.Since(tstart)))

				go notify(msg)

			}
		}
	}

	return nil
}

// GetDataMapPage returns a map of query by page.
func (d *DBAccess) GetDataMapPage(sqlQuery string, pageNo int, pageSize int, dbFilePath string) ([]map[string]interface{}, error) {

	tblName := d.getTableNameFromSQLQuery(sqlQuery)

	sqlx := fmt.Sprintf("select count(_rowid_) from %s", tblName)
	m, err := d.ExecuteScalare(sqlx, dbFilePath)
	if err != nil {
		return nil, err
	}
	recordCount := int(m.(int64))

	if pageNo < 1 {
		pageNo = 1
	}

	if pageSize < 1 {
		pageSize = 1
	}

	_, offset, _ := d.GetPageOffset(recordCount, pageSize, pageNo)

	// pageSize: how many records to return
	// offset: from what position in the data-set
	sqlx = fmt.Sprintf("%s limit %d offset %d", sqlQuery, pageSize, offset)
	mx, err := d.GetDataMap(sqlx, dbFilePath)

	if err != nil {
		return nil, err
	}

	return mx, nil
}

// GetDataMap gets a selected range of table in form of rows and columns.
func (d *DBAccess) GetDataMap(sqlQuery string, dbFilePath string) ([]map[string]interface{}, error) {

	if d.ShrinkDatabaseFiles {
		// Read operation; but still add to the list - as some
		// write operations may have taken a long time... and still
		// good to check on those files to be shrunk.
		if !d.itemExists(dbFilePath) {
			go d.AddDBFileToShrinkWatchList(dbFilePath)
		}
	}

	var db *sql.DB
	var err error
	var valueSlice []map[string]interface{}

	if db, err = d.GetDB(dbFilePath); err != nil {
		return nil, err
	}

	valueSlice, err = getDataMap(sqlQuery, db)

	db.Close()

	return valueSlice, err
}

// GetDataMapPointToDB gets a selected range of table in form of rows
// and columns and keep the keeps the database open.
func (d *DBAccess) GetDataMapPointToDB(sqlQuery string, db *sql.DB) ([]map[string]interface{}, error) {

	var err error
	var valueSlice []map[string]interface{}

	valueSlice, err = getDataMap(sqlQuery, db)

	return valueSlice, err
}

func (d *DBAccess) isFileSQLiteDB(dbFilePath string) bool {

	f := strings.ToLower(dbFilePath)
	if f == "" || strings.HasSuffix(f, ".sqlite-journal") || strings.HasSuffix(f, ".sqlite-shm") || strings.HasSuffix(f, ".sqlite-wal") {
		return false
	}

	return true
}

// ShrinkDB compresses an SQLite database file by removing spaces.
func (d *DBAccess) ShrinkDB(dbFilePath string) error {

	var db *sql.DB
	var err error

	if dbFilePath == "" {
		return nil
	}

	if !d.isFileSQLiteDB(dbFilePath) {
		return nil
	}

	_, err = os.Stat(dbFilePath)
	if os.IsNotExist(err) {
		return nil
	}

	db, err = sql.Open(d.driverName, dbFilePath)
	if err != nil {
		if err.Error() == Err_FileIsNotDatabase {
		}
		return nil
	}
	if _, err = db.Exec("VACUUM;"); err != nil {
		db.Close()
		return err
	}

	db.Close()

	return nil
}

// AddDBFileToShrinkWatchList adds an SQLite database file path
// to a watch list for monitoring.
func (d *DBAccess) AddDBFileToShrinkWatchList(dbFilePath string) {

	if d.itemExists(dbFilePath) {
		return
	}

	if !d.isFileSQLiteDB(dbFilePath) {
		return
	}

	d.ShrinkWatchList = append(d.ShrinkWatchList, dbFilePath)
}

// GetShrinkWatchList return the global string array of the db file paths.
func (d *DBAccess) GetShrinkWatchList() []string {
	return d.ShrinkWatchList
}

func (d *DBAccess) Encrypt(data []byte, pwdPhrase string) ([]byte, error) {
	return Encrypt(data, pwdPhrase)
}

func (d *DBAccess) Decrypt(data []byte, pwdPhrase string) ([]byte, error) {
	return Decrypt(data, pwdPhrase)
}

func (d *DBAccess) EncryptDatabase(dbFilePath string, pwdPhrase string) error {
	return EncryptFile(dbFilePath, pwdPhrase)
}
func (d *DBAccess) DecryptDatabase(dbFilePath string, pwdPhrase string) error {
	return DecryptFile(dbFilePath, pwdPhrase)
}

// GetColumnNames gets the column names of a table.
func (d *DBAccess) GetColumnNames(dbFilePath string, tblName string) ([]string, error) {
	var col []string

	sqlx := fmt.Sprintf("PRAGMA table_info(%s);", strings.ToLower(tblName))
	t, err := d.GetDataTable(sqlx, dbFilePath)
	if err != nil {
		return col, err
	}
	if t == nil {
		return col, nil
	}

	c := t.Cols.Get()

	for i := 0; i < t.Cols.Count(); i++ {
		col = append(col, c[i].Name)
	}

	return col, nil
}
