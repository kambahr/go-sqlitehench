package sqlitehench

import (
	"fmt"
	"net/http"
	"strconv"
)

// GetPagingInfo returns pageSize, offset, and collection info.
func (d *DBAccess) GetPagingInfo(pageSize int, pageNo int, tableName string,
	countColName string, filter string, dbFilePath string) (int, int, CollectionInfo) {

	var ci CollectionInfo

	if pageSize < 1 {
		pageSize = 10
	}

	if pageNo < 1 {
		pageNo = 1
	}

	sc := fmt.Sprintf("select count(_rowid_) from [%s]", tableName)

	if filter != "" && filter != "$get_all$" {
		sc = fmt.Sprintf("select count(%s) from [%s] WHERE (%s)", countColName, tableName, filter)
	}

	recordCount := 0

	rObj, err := d.ExecuteScalare(sc, dbFilePath)
	if err != nil {
		fmt.Println("GetPagingInfo()=>", err)
		return pageSize, -1, ci
	}
	if rObj != nil {
		recordCount = int(rObj.(int64))
	}

	totalPages, offset, pageNo := d.GetPageOffset(recordCount, pageSize, pageNo)

	if pageNo > totalPages {
		pageNo = totalPages
	}

	ci.TotalPages = totalPages
	ci.PageNo = pageNo
	ci.PageSize = pageSize
	ci.RecordCount = recordCount
	ci.PositionFrom = offset
	ci.PositionTo = (offset + pageSize)

	if ci.PositionFrom < 1 {
		ci.PositionFrom = 1
	}

	if ci.PositionTo > recordCount {
		ci.PositionTo = recordCount
	}

	return pageSize, offset, ci
}

// GetPageInfoFromQuery --
func (d *DBAccess) GetPageInfoFromQuery(r *http.Request) (int, int) {

	pSize := r.URL.Query().Get("page_size")
	pNo := r.URL.Query().Get("page_no")

	pageSize := 0
	pageNo := 0

	if pSize != "" {
		pageSize, _ = strconv.Atoi(pSize)
	}

	if pNo != "" {
		pageNo, _ = strconv.Atoi(pNo)
	}

	if pageSize == 0 {
		pageSize = 10
	}
	if pageNo == 0 {
		pageNo = 1
	}

	return pageSize, pageNo
}

// GetPageOffset returns totalPages, offset, pageNo
func (d *DBAccess) GetPageOffset(recordCount int, pageSize int, pageNo int) (int, int, int) {

	if pageSize < 1 || recordCount < 1 {
		return 0, 0, 0
	}

	totalPages := recordCount / pageSize

	remainder := recordCount % pageSize
	if remainder > 0 {
		totalPages++
	}

	if pageNo > totalPages {
		pageNo = totalPages
	}

	offset := pageSize * pageNo
	if pageNo == 1 {
		offset = 0
	}
	remainder = offset % pageSize
	if remainder > 0 {
		offset += pageSize
	}
	if offset < 0 {
		offset = 0
	}
	if offset > recordCount {
		offset = recordCount - pageSize
	}

	if pageNo < 1 {
		pageNo = 1
	}

	if totalPages < 1 {
		totalPages = 1
	}

	return totalPages, offset, pageNo
}
