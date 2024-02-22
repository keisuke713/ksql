package storage

import (
	"encoding/binary"
	"errors"
	"os"
)

type (
	BPlustTree struct {
		RootNodeID PageID
		KeyLen     uint32
	}
)

func NewBPlustTree() *BPlustTree {
	return &BPlustTree{
		InvalidPageID,
		0,
	}
}

// ファイルはすでに作らている前提
// 最初の4KBはメタデータでそれ以降にノードの情報が入る、キーの長さを動的に指定できるようにする
// Tableクラス作る？
// ということでCreate,Insertの動線を整えたい
func NewBPlustTreeHoge(dm DiskManager, f *os.File) *BPlustTree {
	metaBytes := dm.ReadPageData(PageID(0))
	keyLen := binary.NativeEndian.Uint32(metaBytes[:4])
	return &BPlustTree{
		RootPageID,
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
	// todo どこでkeyの長さ管理する？
	pages, err := root.SearchByV3(dm, key, key, ColumnSize)
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
