# A Go environment for SQLite

## Make database calls with the all too familiar functions

### Database Driver
This package uses the SQLite driver written by <a href="https://github.com/mattn/go-sqlite3">Yasuhiro Matsumoto/G.J.R. Timmer</a>, although you can use go-sqlitehench with an SQLite Go driver of your choice. 

### Opening/Closing database files
Most functions come in two folds:  *keeping the db open*, or *close it*, after each write operation.
To avoid file corruption or lingering locks (where a db lock persists even after the db is 'closed'), the database file can be closed after each operation. This may not be suitable for every scenario (i.e. while importing large number of records, shared env.,...).

Generally spkeaing, you would not have to *close* an SQLite database after each operation.
However, in a high volume situation, a database *can* get locked (due to SQLite's single-write mechanism);
and (in some situations) may even get corrupted (i.e. if journal file(s) get out-of-sync)...

### Functions
Functions are basically wrapped into the following:

#### Close database after operation is completed
- ExecuteScalare..................... gets one value
- ExecuteNonQuery................  executes an SQL statement
- GetDataMap.........................  gets a []map of rows/cols

#### Keep database open after operation is completed

- ExecuteScalarePointToDB............ gets one value
- ExecuteNonQueryPointToDB........  executes an SQL statement
- GetDataMapPointToDB.................  gets a []map of rows/cols

#### Other
- GetDataTable........................ gets a table snapshot in form of rows/cols.
- InsertDataTable..................... inserts collections.Table into a table.
- BulkInsert.............................. inserts large sets of data into a database.
- CloneDatabase..................... creates a (local) copy of a database.
- GetDataTableLongQuery.......goes through a query page-by-page and keeps adding results to a DataTable; it also notifies the caller via an event.									 

### Performance
The default journal mode is WAL (PRAGMA journal_mode = WAL).  This is applied when a database is opened -- via

``` Go
// PRAGMA journal_mode = WAL;
db.Exec(d.PRAGMA)
```
The WAL mode is proportionately faster than the default journal mode, however, coming up with a performance benchmark depends on many factors...please, see https://sqlite.org/pragma.html#pragma_journal_mode for more details on the SQLite journal modes.

### Shrinking database files
Shrinking is done automatically, when ShrinkDatabaseFiles = true; so there is no need to *shrink* a db file explicitly - as 
every database file that interacts with the package is added to a *watchlist*. 
Each file is shrunk accordingly within a short period of time. 
If a db file is not used for more than ~one hour, it will be excluded from the watchlist.
Note that if <a href="https://sqlite.org/pragma.html#pragma_auto_vacuum">PRAGMA auto_vacuum</a> is set, the shrink-daemon will not be started.

### Usage Example

```go
// ...
import (
	"github.com/kambahr/go-collections"
	"github.com/kambahr/go-sqlitehench"
	// ... 
)
	var pragma []string = []string{
		"PRAGMA journal_mode = WALL",
		"PRAGMA temp_store = MEMORY",
		"PRAGMA synchronous = OFF",
	}
	mysqlite := sqlitehench.NewDBAccess(
	   sqlitehench.DBAccess{			
	      ShrinkDatabaseFiles: false,
	      PRAGMA:              pragma,
	   })
	dbPathSrc := "<path to the source SQLite database>"
	dbPathDest := "<path to the destination SQLite database>"

	mysqlite.CloneDatabase(dbPathSrc, dbPathDest)
    // ...

    sqlx := `select <columns> from <table name> limit 20000 offset 1000`
    mytbl, _ := mysqlite.GetDataTable(sqlx, dbPathSrc)

    mysqlite.BulkInsert(mytbl, dbPathSrc)

    //...

```
