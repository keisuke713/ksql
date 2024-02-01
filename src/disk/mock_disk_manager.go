package disk

type (
	MockDiskManagerImpl struct{}
)

func (dm MockDiskManagerImpl) AllocatePage() PageID {
	return PageID(3)
}

func (dm MockDiskManagerImpl) ReadPageData(pageID PageID) []byte {
	bs := make([]byte, PageSize)
	bs[0] = 1
	return bs
}

func (dm MockDiskManagerImpl) WritePageData(pageID PageID, data []byte) {}
