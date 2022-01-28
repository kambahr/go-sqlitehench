package sqlitehench

import (
	"io/fs"
	"os"
	"time"
)

// Shrinking of SQLite databases runs in the background.
// Everytime a db-file is acceesed for writing, it is added to
// a list rather than scanning a target folder for SQLite
// db files (adds to I/O operations).  Databases in the list
// will be monitored for shrinking.
func (d *DBAccess) shrinkAllDB() {

	// Start the watchlist maint to prevent the list
	// from growing out of proportion.
	go d.maintWatchList()

lblAgain:

	time.Sleep(5 * time.Minute)

	if len(d.ShrinkWatchList) == 0 {
		// No activity.
		goto lblAgain
	}

lblForeAgain:
	for i := 0; i < len(d.ShrinkWatchList); i++ {
		// This is necessary...to avoid clashes of the
		// size of array and the counter index.
		time.Sleep(250 * time.Millisecond)
		if len(d.ShrinkWatchList) == 0 || i >= len(d.ShrinkWatchList) {
			break
		}
		// Check the last write access
		if !fileOrDirExists(d.ShrinkWatchList[i]) {
			d.removeItem(i)
			goto lblForeAgain
		}
		fi, err := os.Stat(d.ShrinkWatchList[i])
		if err != nil {
			d.removeItem(i)
			time.Sleep(500 * time.Millisecond)
			goto lblForeAgain
		}
		t := fi.ModTime()
		isBefore := t.Before(time.Now().Add(-15 * time.Minute))
		// has to not have activity for the last 15 minutes
		if !isBefore {
			continue
		}
		go d.ShrinkDB(d.ShrinkWatchList[i])
		time.Sleep(time.Second)
	}

	goto lblAgain
}

// maintWatchList examins the file paths in the watch list.
// It removes the ones that have not been acccessed for sometime.
func (d *DBAccess) maintWatchList() {

	var err error
	var fi fs.FileInfo

lblAgain:
	for i := 0; i < len(d.ShrinkWatchList); i++ {

		if len(d.ShrinkWatchList) == 0 {
			break
		}

		if !d.DatabaseExists(d.ShrinkWatchList[i]) {
			d.removeItem(i)
			goto lblAgain
		}

		if len(d.ShrinkWatchList) == 0 || i >= len(d.ShrinkWatchList) {
			break
		}

		if fi, err = os.Stat(d.ShrinkWatchList[i]); err != nil {
			// The db file could be corrupted or been removed.
			d.removeItem(i)
			goto lblAgain
		}

		t := time.Since(fi.ModTime())
		if t.Hours() < 1.25 {
			// remove from watchlist.
			d.removeItem(i)
			goto lblAgain
		}
	}
	time.Sleep(time.Minute)
	goto lblAgain
}
