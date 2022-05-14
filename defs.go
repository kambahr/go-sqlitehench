package sqlitehench

import "github.com/kambahr/go-collections"

type DBAccess struct {
	driverName          string
	MaxIdleConns        uint
	MaxOpenConns        uint
	PRAGMA              []string
	ShrinkDatabaseFiles bool
	Remote              IRemoteSQLite

	// ShrinkWatchList keeps a list of sqlite database
	// file paths that are to be shrinked in a set internval.
	ShrinkWatchList []string
}

// CollectionInfo holds Grid info for use in the client javascript.
type CollectionInfo struct {
	RecordCount  int
	TotalPages   int
	PageSize     int
	PageNo       int
	PositionFrom int
	PositionTo   int
}

type LonqQueryArgs struct {
	ResultTable  *collections.Table
	RowsFetched  int64
	TotalToFetch int64
	TotalPages   int
	PageSize     int
	Status       string
}

const (
	Err_DatabaseIsLocked      = "database is locked"
	Err_FileIsNotDatabase     = "file is not a database"
	Err_DatabaseFileNotExists = "database file does not exist"
	Err_NoRowsFound           = "no rows found"
)
