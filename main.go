package main

import (
	"bytes"
	"fmt"
	"ksql/src/storage"
	"os"
)

func main() {
	// 0からインサート
	// f, _ := os.Create("table/test_table_65535")
	// dm := storage.NewDiskManager(f)
	// btree := storage.NewBPlustTree()
	// var i uint32
	// for i = 0; i < 65535; i++ {
	// 	btree.InsertPair(dm, storage.NewBytes(i), storage.NewBytes(i))
	// }

	// 既存のを使う
	f, err := os.OpenFile("table/test_table_65535", os.O_RDWR, 0666)
	if err != nil {
		panic(err)
	}
	dm := storage.NewDiskManager(f)
	btree := storage.NewBPlustTree()
	btree.RootNodeID = storage.PageID(1)

	// リーフを全て表示
	slice := btree.Slice(dm)
	var sum int
	var sumLeaf int
	buf := bytes.Buffer{}
	for _, p := range slice {
		if p.NodeType == storage.NodeTypeBranch {
			continue
		}
		sum += 1
		sumLeaf += len(p.Items)
		buf.WriteString(fmt.Sprintf("start: %+v, end: %+v, len: %+v, depth: %+v\n", p.Items[0].Key, p.Items[len(p.Items)-1].Key, len(p.Items), p.Depth))
	}
	buf.WriteString(fmt.Sprintf("sum: %+v leaf, item count: %+v \n", sum, sumLeaf))
	f2, _ := os.Create("leaf_list")
	fmt.Fprint(f2, buf.String())
}
