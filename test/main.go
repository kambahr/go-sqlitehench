// (c) Kamiar Bahri
package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/kambahr/go-sqlitehench"
)

func createDatabase(filePath string, d *sqlitehench.DBAccess) {
	// Create a test tables
	sqlx :=
		`
	CREATE TABLE IF NOT EXISTS DBTest (
		DBTest INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
		Message TEXT NOT NULL,
		DateTimeCreated TEXT NOT NULL
	);
	CREATE UNIQUE INDEX INX_DBTest_Message ON DBTest (Message);
	CREATE INDEX INX_DBTest_DateTimeCreated ON DBTest (DateTimeCreated);
	insert into DBTest(Message,DateTimeCreated)values('Hello World',strftime('%Y-%m-%d %H:%M:%f','now'));
	`
	if rowsAffected, err := d.ExecuteNonQuery(sqlx, filePath); err != nil {
		log.Fatal(err)
	} else {
		fmt.Println("--- ExecuteNonQuery()")
		fmt.Println("      created table DBTest; rowsAffected:", rowsAffected, "\n")
	}
}
func cleanup() {
	var f = []string{
		"dbaccess-test.sqlite",
		"dbaccess-test.sqlite-shm", "dbaccess-test.sqlite-wal",
	}

	for i := 0; i < len(f); i++ {
		os.Remove(f[i])
	}

}
func main() {

	cleanup()

	p := "dbaccess-test.sqlite"

	d := sqlitehench.NewDBAccess(sqlitehench.DBAccess{MaxIdleConns: 100, MaxOpenConns: 100})

	if d.DatabaseExists(p) {
		if err := os.Remove(p); err != nil {
			log.Fatal(err)
		}
	}

	createDatabase(p, d)

	// ExcecuteScalare
	q := "select Message from DBTest limit 1"
	if m, err := d.ExecuteScalare(q, p); err != nil {
		log.Fatal(err)
	} else {
		fmt.Println("--- ExecuteScalare()")
		fmt.Println("     ", m, "\n")
	}

	// A test of entering a few records
	totalRecords := 20000
	fmt.Println("inserting", totalRecords, "records...")

	// Keep the db open while inserting records
	var db *sql.DB
	var err error
	if db, err = d.GetDB(p); err != nil {
		log.Fatal(err)
	}
	t := time.Now()
	for i := 0; i < totalRecords; i++ {

		sqlx := fmt.Sprintf(`
			insert into DBTest(Message,DateTimeCreated)
			values('Hello World %s %d x',strftime('%%Y-%%m-%%d %%H:%%M:%%f','now'))`,
			strings.Repeat(fmt.Sprintf(" some text to fill spaces %d >>", i), 2), i+1)

		// Keeping the db open on every loop is a bit faster.
		// But it exposes the db for lock errors.
		if _, err := d.ExecuteNonQueryPointToDB(sqlx, db); err != nil {
			log.Fatal(err)
		}
	}
	db.Close()
	took := time.Since(t)
	fmt.Println("    keep db open took ................", took, "\n")

	//Closing the db on every loop is slower.
	//But exposes the db for lock errors.
	totalRecords = 1000
	fmt.Println("inserting", totalRecords, "records...")

	t = time.Now()
	for i := 0; i < totalRecords; i++ {
		sqlx := fmt.Sprintf(`
			insert into DBTest(Message,DateTimeCreated)
			values('Hello World %s %d x',strftime('%%Y-%%m-%%d %%H:%%M:%%f','now'))`,
			strings.Repeat(fmt.Sprintf(" some text to fill spaces %d >> ", i+totalRecords), 2), i+1+totalRecords)

		if _, err := d.ExecuteNonQuery(sqlx, p); err != nil {
			log.Fatal(err)
		}
	}
	took = time.Since(t)
	fmt.Println("    close db on every loop took ......", took, "\n")

	// GetDataMap
	var tp []map[string]interface{}
	q = "select * from DBTest limit 5"
	if tp, err = d.GetDataMap(q, p); err != nil {
		log.Fatal(err)
	}

	fmt.Println("--- GetDataMap()")

	for i := 1; i < len(tp); i++ {
		msg := tp[i]["Message"].(string)
		dt := tp[i]["DateTimeCreated"].(string)
		fmt.Println(fmt.Sprintf("    [%d]", i), msg, ",", dt)
	}

	time.Sleep(3 * time.Second)

	// This will leave spaces in the database.
	sqlx := "delete from DBTest"
	if _, err := d.ExecuteNonQuery(sqlx, p); err != nil {
		log.Fatal(err)
	}

	fi, _ := os.Stat(p)
	fmt.Println("\ndb size before shrink ...", fi.Size()/1024, "kB")

	// Wait a bit for the daemon to shrink the db
	time.Sleep(5 * time.Second)

	fi, _ = os.Stat(p)
	fmt.Println("db size after shrink ....", fi.Size()/1024, "  kB")

	fmt.Println("\ndone\n")
}
