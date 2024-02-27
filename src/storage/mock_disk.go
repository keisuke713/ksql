package storage

type (
	MockDiskManagerImpl struct{}
)

func (dm MockDiskManagerImpl) AllocatePage() PageID {
	return PageID(3)
}

func (dm MockDiskManagerImpl) ReadPageData(pageID PageID) [PageSize]byte {
	var bs [PageSize]byte
	bs[0] = 1
	return bs
}

func (dm MockDiskManagerImpl) WritePageData(pageID PageID, data [PageSize]byte) {}

func (dm MockDiskManagerImpl) FSize() int64 {
	return 0
}
