package sqlitehench

import (
	"fmt"
	"strings"
)

func NewDBAccess(d DBAccess) *DBAccess {

	if d.MaxIdleConns < 1 {
		d.MaxIdleConns = 1
	}

	// Setting d.MaxOpenConns to 1, reduces chances of database
	// getting locked.
	if d.MaxOpenConns < 1 {
		d.MaxOpenConns = 1
	}

	if len(d.PRAGMA) == 0 {
		// See https://sqlite.org/pragma.html#pragma_auto_vacuum
		// The defaut for auto_vacuum is NONE, because of the following statement (above URL).
		//
		// "When the auto-vacuum mode is 1 or "full", the freelist pages are moved
		//  to the end of the database file and the database file is truncated to remove
		//  the freelist pages at every transaction commit. Note, however, that auto-vacuum
		//  only truncates the freelist pages from the file. Auto-vacuum does not defragment
		//  the database nor repack individual database pages the way that the VACUUM command
		//  does. In fact, because it moves pages aroundwithin the file, auto-vacuum can
		//  actually make fragmentation worse."
		//
		d.PRAGMA = append(d.PRAGMA, "PRAGMA auto_vacuum = NONE;")

		// WAL is set as the default mode.
		// See https://sqlite.org/pragma.html#pragma_journal_mode for more details.
		d.PRAGMA = append(d.PRAGMA, "PRAGMA journal_mode = WAL;")

		// This allows multiple writers and readers.
		// https://www.sqlite.org/pragma.html#pragma_wal_autocheckpoint
		d.PRAGMA = append(d.PRAGMA, "PRAGMA wal_checkpoint(PASSIVE);")
	}

	if d.driverName == "" {
		d.driverName = "sqlite3"
	}

	d.PRAGMA = fixPragmaTextAndOrder(d.PRAGMA)

	// Shring databases?
	for i := 0; i < len(d.PRAGMA); i++ {
		if !strings.Contains(strings.ToUpper(d.PRAGMA[i]), strings.ToUpper("PRAGMA auto_vacuum")) {
			d.ShrinkDatabaseFiles = true
			break
		}
	}

	if d.ShrinkDatabaseFiles {
		go d.shrinkAllDB()
	}

	var rmt RemoteSQLite
	// RemoteSQLite exposes its entire type for
	// the caller (via Base(), so there is no need
	// to initialise here.
	d.Remote = &IRemoteSQLiteHndlr{&rmt}

	return &d
}

// fixPragmaTextAndOrder edits the pragma entries:
// smicolon at the end, wall journal_mode to accompany
// with checkpoint and also some formatting mistakes.
func fixPragmaTextAndOrder(pragArry []string) []string {

	if len(pragArry) == 0 {
		return pragArry
	}

	//Fix the PRAGMA text
	for i := 0; i < len(pragArry); i++ {
		pragArry[i] = strings.Trim(pragArry[i], " ")
		// take out extra spaces
		for {
			if !strings.Contains(pragArry[i], "  ") {
				break
			}
			pragArry[i] = strings.ReplaceAll(pragArry[i], "  ", " ")
		}
		pragArry[i] = strings.ToLower(pragArry[i])
		if !strings.HasSuffix(pragArry[i], ";") {
			pragArry[i] = fmt.Sprintf("%s;", pragArry[i])
		}
		if !strings.Contains(pragArry[i], "pragma ") {
			pragArry[i] = strings.ReplaceAll(pragArry[i], "pragma", "pragma ")
		}
	}

	// if set to wall, make sure pragma wal_checkpoint(passive) is included
	//pragma wal_checkpoint(passive);
	wallExists := false
	for i := 0; i < len(pragArry); i++ {
		if strings.Contains(pragArry[i], "journal_mode = wall") {
			wallExists = true
			break
		}
	}
	if wallExists {
		// the wall and the checkpoint pragma statements
		// have to be in consecutive order.
		var s []string
		s = append(s, "pragma journal_mode = wall;")
		s = append(s, "pragma wal_checkpoint(passive);")

		for i := 0; i < len(pragArry); i++ {
			if !strings.Contains(pragArry[i], "wal_checkpoint") && !strings.Contains(pragArry[i], "journal_mode = wall") {
				s = append(s, pragArry[i])
				break
			}
		}
		pragArry = s
	}

	return pragArry
}
