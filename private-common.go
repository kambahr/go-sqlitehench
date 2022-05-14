package sqlitehench

import (
	"context"
	"database/sql"
	"errors"
	"strings"
)

// executeScalare returns one value and closes the database.
func executeScalare(sqlStatement string, db *sql.DB) (interface{}, error) {

	var rows *sql.Rows
	var err error
	var item interface{}

	if rows, err = db.Query(sqlStatement); err != nil {
		return nil, err
	}

	if rows.Next() {
		rows.Scan(&item)
	}
	rows.Close()

	if item == nil {
		return nil, nil
	}

	return item, nil
}

func executeNonQuery(sqlStatement string, db *sql.DB) (int64, error) {

	// this is a rough estimate (https://sqlite.org/limits.html),
	// but it'd be good to prevent this to go thru.
	if len(sqlStatement) > 1000000000 {
		return -1, errors.New("query length exceeded max length of 1000000000 bytes")
	}

	var err error

	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return -1, err
	}

	result, err := tx.ExecContext(ctx, sqlStatement)
	if err != nil {
		tx.Rollback()
		return -1, err
	}

	var rowsAffected int64 = -1

	if result != nil {
		rowsAffected, err = result.RowsAffected()
		if err != nil {
			tx.Rollback()
			return -1, err
		}
	}
	tx.Commit()

	return rowsAffected, nil
}
func fixSQLQuery(sqlQuery string) string {

	sqlQuery = strings.ReplaceAll(strings.ToLower(sqlQuery), " group ", " [Group] ")
	sqlQuery = strings.ReplaceAll(strings.ToLower(sqlQuery), " group;", " [Group];")

	return sqlQuery
}

// getDataMap gets a selected range of table in form of rows and columns.
func getDataMap(sqlQuery string, db *sql.DB) ([]map[string]interface{}, error) {

	var err error

	var mRet []map[string]interface{}

	rows, err := db.Query(sqlQuery)
	if err != nil {
		return nil, err
	}
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	columns := make([]interface{}, len(cols))
	columnPointers := make([]interface{}, len(cols))

	for i := 0; i < len(columns); i++ {
		columnPointers[i] = &columns[i]
	}

	for rows.Next() {
		m := make(map[string]interface{})

		if err := rows.Scan(columnPointers...); err != nil {
			return nil, err
		}

		for i := 0; i < len(cols); i++ {
			val := columnPointers[i].(*interface{})
			m[cols[i]] = *val
		}

		mRet = append(mRet, m)
	}

	rows.Close()

	return mRet, nil
}
