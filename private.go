package sqlitehench

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"math"
	"os"
	"time"
)

func (d *DBAccess) itemExists(dbFilePath string) bool {

	for i := 0; i < len(d.ShrinkWatchList); i++ {
		if d.ShrinkWatchList[i] == dbFilePath {
			return true
		}
	}

	return false
}

// removeItemFromShrinkWatchList removes a db file path from the d.ShrinkWatchList list.
func (d *DBAccess) removeItemFromShrinkWatchList(i int) {

	if len(d.ShrinkWatchList) == 0 || i >= len(d.ShrinkWatchList) {
		return
	}

	if len(d.ShrinkWatchList) > 1 {
		d.ShrinkWatchList[len(d.ShrinkWatchList)-1], d.ShrinkWatchList[i] = d.ShrinkWatchList[i], d.ShrinkWatchList[len(d.ShrinkWatchList)-1]
	}

	d.ShrinkWatchList = d.ShrinkWatchList[:len(d.ShrinkWatchList)-1]
}

func fileOrDirExists(path string) bool {
	if path == "" {
		return false
	}

	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false
	}

	return true
}

func durationToString(d time.Duration) string {
	h := int64(math.Mod(d.Hours(), 24))
	m := int64(math.Mod(d.Minutes(), 60))
	s := int(math.Mod(d.Seconds(), 60))
	ms := int(math.Mod(float64(d.Milliseconds()), 1000))

	return fmt.Sprintf("%02d:%02d:%02d.%03d", h, m, s, ms)
}

func formatNumber(number int64) string {
	output := fmt.Sprintf("%v", number)
	startOffset := 3
	if number < 0 {
		startOffset++
	}
	for outputIndex := len(output); outputIndex > startOffset; {
		outputIndex -= 3
		output = output[:outputIndex] + "," + output[outputIndex:]
	}
	return output
}
func removeElmFrmArryString(v []string, e string) []string {
	var r []string
	count := len(v)
	for i := 0; i < count; i++ {
		if v[i] != e {
			r = append(r, v[i])
		}
	}
	return r
}
func removeElmFrmArry(v []interface{}, e interface{}) []interface{} {
	var r []interface{}
	count := len(v)
	for i := 0; i < count; i++ {
		if v[i] != e {
			r = append(r, v[i])
		}
	}
	return r
}

// createHash --
func createHash(key string) string {
	hasher := md5.New()
	hasher.Write([]byte(key))
	return hex.EncodeToString(hasher.Sum(nil))
}
