package storage

import (
	"errors"
	"fmt"
)

type (
	BPlustTree struct {
		RootNodeID PageID
	}
)

func NewBPlustTree() *BPlustTree {
	return &BPlustTree{
		InvalidPageID,
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
	root.Walk(dm, &ps)
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
	fmt.Println("")
	fmt.Printf("root: %+v, p: %+v. key: %v, value: %v\n", root, p, key, value)
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
	}
	if err := page.Flush(dm); err != nil {
		return err
	}
	b.RootNodeID = rootPageID
	return nil
}
