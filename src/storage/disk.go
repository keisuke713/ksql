package storage

import (
	"os"
)

type (
	DiskManager interface {
		AllocatePage() PageID
		ReadPageData(pageID PageID) [PageSize]byte
		WritePageData(pageID PageID, data [PageSize]byte)
	}

	DiskManagerImpl struct {
		heapFile   *os.File
		nextPageID uint64
	}
)

func NewDiskManager(heapFile *os.File) DiskManager {
	stat, err := heapFile.Stat()
	if err != nil {
		panic(err)
	}
	fSize := stat.Size()
	return &DiskManagerImpl{
		heapFile:   heapFile,
		nextPageID: uint64(fSize / PageSize),
	}
}

func Open(path string) DiskManager {
	f, err := os.OpenFile(path, os.O_RDWR, 0666)
	if err != nil {
		panic(err)
	}
	return NewDiskManager(f)
}

func (dm *DiskManagerImpl) AllocatePage() PageID {
	pageID := dm.nextPageID
	dm.nextPageID = dm.nextPageID + 1
	return PageID(pageID)
}

func (dm *DiskManagerImpl) ReadPageData(pageID PageID) [PageSize]byte {
	offset := PageSize * pageID
	data := make([]byte, PageSize)
	_, err := dm.heapFile.ReadAt(data, int64(offset))
	if err != nil {
		panic(err)
	}

	var bytesArr [PageSize]byte
	for i, b := range data {
		bytesArr[i] = b
	}
	return bytesArr
}

func (dm *DiskManagerImpl) WritePageData(pageID PageID, data [PageSize]byte) {
	offset := PageSize * pageID
	// pageSizeより大きいデータは書き込まないようにする
	dm.heapFile.WriteAt(data[:], int64(offset))
}
