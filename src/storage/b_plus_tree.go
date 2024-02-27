package storage

import (
	"encoding/binary"
	"errors"
)

type (
	BPlustTree struct {
		RootNodeID PageID
		KeyLen     uint32
	}
)

// ファイルはすでに作らている前提
// Tableクラス作る？
// ということでCreate,Insertの動線を整えたい
func NewBPlustTree(dm DiskManager) *BPlustTree {
	metaBytes := dm.ReadPageData(PageID(0))
	keyLen := binary.NativeEndian.Uint32(metaBytes[:4])

	// PageID1がルートの情報なので
	// ファイルサイズが4KBを超える場合はルートのーどが存在すると判断してセットする
	fSize := dm.FSize()
	rootPageID := InvalidPageID
	if fSize > PageSize {
		rootPageID = RootPageID
	}
	return &BPlustTree{
		rootPageID,
		keyLen,
	}
}

func (b *BPlustTree) PrintAll(dm DiskManager) {
	if b.RootNodeID == InvalidPageID {
		return
	}
	bytes := dm.ReadPageData(b.RootNodeID)
	root, err := NewPage(bytes)
	if err != nil {
		panic(err)
	}
	root.PrintAll(dm, "")
}

func (b *BPlustTree) Slice(dm DiskManager) []Page {
	if b.RootNodeID == InvalidPageID {
		return nil
	}
	bytes := dm.ReadPageData(b.RootNodeID)
	root, err := NewPage(bytes)
	if err != nil {
		panic(err)
	}
	var ps []Page
	root.Walk(dm, &ps, 0)
	return ps
}

func (b *BPlustTree) InsertPair(dm DiskManager, key, value Bytes) error {
	// rootがnilの場合
	if b.RootNodeID == InvalidPageID {
		err := b.CreateRoot(dm)
		if err != nil {
			return err
		}
	}

	// 該当するleafを探す
	bytes := dm.ReadPageData(b.RootNodeID)
	root, err := NewPage(bytes)
	if err != nil {
		return err
	}

	pages, err := root.SearchByV3(dm, key, key, b.KeyLen)
	if err != nil {
		return err
	}
	if len(pages) == 0 {
		return errors.New("not found correnponding page")
	}
	p := pages[0]
	return p.InsertPair(dm, key, value)
}

func (b *BPlustTree) CreateRoot(dm DiskManager) error {
	rootPageID := dm.AllocatePage()
	page := &Page{
		rootPageID,
		NodeTypeLeaf,
		InvalidPageID,
		InvalidPageID,
		InvalidPageID,
		InvalidPageID,
		[]Pair{},
		0,
	}
	if err := page.Flush(dm); err != nil {
		return err
	}
	b.RootNodeID = rootPageID
	return nil
}
