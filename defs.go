package sqlitehench

import (
	"fmt"
	"strings"
	"time"

	"github.com/kambahr/go-collections"
)

type DBAccess struct {
	driverName          string
	MaxIdleConns        uint
	MaxOpenConns        uint
	PRAGMA              []string
	ShrinkDatabaseFiles bool
	//Remote              IRemoteSQLite

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

// convertStringToTime --
// dateTimeString => 2020-06-22T10:20:38
// on error it returns: 0001-01-01T00:00:00.000Z
func (d *DBAccess) convertStringToTime(dateTimeString string) (time.Time, error) {

	var s string

	dateTimeString = strings.Trim(dateTimeString, " ")

	if strings.Contains(dateTimeString, " ") {
		// Missing T
		v := strings.Split(dateTimeString, " ")
		v[1] = fmt.Sprintf("T%s", v[1])
		dateTimeString = strings.Join(v, "")
	}

	tEmpty, err := time.Parse(time.RFC3339, "0001-01-01T00:00:00.000Z")
	if err != nil {
		return tEmpty, err
	}
	s = strings.Replace(dateTimeString, " ", "T", 1)
	v := strings.Split(dateTimeString, ".")

	if len(v) > 1 {
		z := ""
		if len(v[1]) >= 4 {
			z = v[1][:3]
		} else {
			z = "000"
		}
		// check again
		if len(z) < 3 {
			z = "000"
		}
		s = fmt.Sprintf("%s.%sZ", v[0], z)
	} else {
		s = fmt.Sprintf("%s.000Z", s)
	}

	t, err := time.Parse(time.RFC3339, s)

	if err != nil {
		return tEmpty, err
	}

	return t, err
}
