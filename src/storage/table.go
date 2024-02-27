package storage

import (
	"encoding/binary"
	"fmt"
	"os"
)

type ()

func NewTable(fName string, keyLen uint32) {
	f, err := os.Create(fmt.Sprintf("../../table/%s", fName))
	if err != nil {
		panic(err)
	}

	dm := NewDiskManager(f)
	// メタデータを先頭4KBに書き込む
	var b [PageSize]byte
	binary.NativeEndian.PutUint32(b[:4], keyLen)
	dm.WritePageData(dm.AllocatePage(), b)
}

func NewTable2(dm DiskManager, keyLen uint32) {
	// メタデータを先頭4KBに書き込む
	var b [PageSize]byte
	binary.NativeEndian.PutUint32(b[:4], keyLen)
	dm.WritePageData(dm.AllocatePage(), b)
}
