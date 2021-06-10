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

	if len(d.PRAGMA) > 0 {
		// Fix the PRAGMA text
		for i := 0; i < len(d.PRAGMA); i++ {
			right := d.PRAGMA[i][6:]
			right = strings.ReplaceAll(right, " ", "")
			d.PRAGMA[i] = fmt.Sprintf("PRAGMA %s", right)
		}
		for i := 0; i < len(d.PRAGMA); i++ {
			if !strings.Contains(d.PRAGMA[i], "PRAGMA auto_vacuum") {
				d.ShrinkDatabaseFiles = true
				break
			}
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
