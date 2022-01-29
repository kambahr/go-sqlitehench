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

	for {
		for i := 0; i < len(d.ShrinkWatchList); i++ {
			if len(d.ShrinkWatchList) == 0 || i >= len(d.ShrinkWatchList) {
				break
			}
			if !fileOrDirExists(d.ShrinkWatchList[i]) {
				d.removeItemFromShrinkWatchList(i)
				break
			}

			// Shrink the database.
			d.ShrinkDB(d.ShrinkWatchList[i])

			if i%10 == 0 {
				time.Sleep(time.Second)
			}
		}

		time.Sleep(15 * time.Second)
	}
}

// maintWatchList examins the file paths in the watch list.
// It removes the ones that have not been acccessed for sometime.
func (d *DBAccess) maintWatchList() {

	var err error
	var fi fs.FileInfo

	for {
		for i := 0; i < len(d.ShrinkWatchList); i++ {

			if len(d.ShrinkWatchList) == 0 {
				break
			}

			if !d.DatabaseExists(d.ShrinkWatchList[i]) {
				d.removeItemFromShrinkWatchList(i)
				break
			}

			if len(d.ShrinkWatchList) == 0 || i >= len(d.ShrinkWatchList) {
				break
			}

			if fi, err = os.Stat(d.ShrinkWatchList[i]); err != nil {
				// The db file could be corrupted or been removed.
				d.removeItemFromShrinkWatchList(i)
				break
			}

			t := time.Since(fi.ModTime())
			if t.Hours() < 1.25 {
				// remove from watchlist.
				d.removeItemFromShrinkWatchList(i)
				break
			}
		}
		time.Sleep(time.Minute)
	}
}
