package main

import (
	"fmt"
)

const lenn = 256 * 256 * 128

func main() {
	fmt.Println("Hello world")
	// arr := [lenn]int{}
	// for i := range arr {
	// 	arr[i] = i
	// }
	// src := rand.NewSource(lenn)
	// rand := rand.New(src)
	// rand.Shuffle(lenn, func(i, j int) {
	// 	a := arr[i]
	// 	arr[i] = arr[j]
	// 	arr[j] = a
	// })

	// // ランダムインサート
	// f, _ := os.Create("table/test_table_random_800")
	// dm := storage.NewDiskManager(f)
	// storage.NewTable2(dm, 4)
	// btree := storage.NewBPlustTree(dm)
	// for i := range arr {
	// 	btree.InsertPair(dm, storage.NewBytes(uint32(i)), storage.NewBytes(uint32(i)))
	// }

	// 既存のを使う
	// f, err := os.OpenFile("table/test_table_random_800", os.O_RDWR, 0666)
	// if err != nil {
	// 	panic(err)
	// }
	// dm := storage.NewDiskManager(f)
	// storage.NewTable2(dm, 4)
	// btree := storage.NewBPlustTree(dm)
	// btree.RootNodeID = storage.PageID(1)

	// // リーフを全て表示
	// slice := btree.Slice(dm)
	// var sum int
	// var sumLeaf int
	// buf := bytes.Buffer{}
	// for _, p := range slice {
	// 	if p.NodeType == storage.NodeTypeBranch {
	// 		continue
	// 	}
	// 	sum += 1
	// 	sumLeaf += len(p.Items)
	// 	buf.WriteString(fmt.Sprintf("start: %+v, end: %+v, len: %+v, depth: %+v\n", p.Items[0].Key, p.Items[len(p.Items)-1].Key, len(p.Items), p.Depth))
	// }
	// buf.WriteString(fmt.Sprintf("sum: %+v leaf, item count: %+v \n", sum, sumLeaf))
	// f2, _ := os.Create("leaf_list")
	// fmt.Fprint(f2, buf.String())

	// 既存のデータから絞り込む
	// f, err := os.OpenFile("table/test_table_random_800", os.O_RDWR, 0666)
	// if err != nil {
	// 	panic(err)
	// }
	// dm := storage.NewDiskManager(f)
	// btree := storage.NewBPlustTree(dm)
	// pages, err := btree.SearchBy(dm, storage.NewBytes(255*255*2), storage.NewBytes(255*255*2), 4)
	// if err != nil {
	// 	panic(err)
	// }
	// fmt.Printf("pages: %+v \n", pages[0])
}

// func test() {
// 	// 対象のインデックスを見つける
// 	index := "hoge"
// 	f, err := os.OpenFile(fmt.Sprintf("table/%s", index), os.O_RDWR, 0666)
// 	if err != nil {
// 		panic(err)
// 	}
// 	dm := storage.NewDiskManager(f)
// 	btree := storage.NewBPlustTree(dm)

// 	// typeごとにminとmaxをセットする
// 	var (
// 		hoge storage.SearchMode
// 		min  storage.Bytes
// 		max  storage.Bytes

// 		len *uint32
// 	)
// 	switch hoge {
// 	case storage.SearchModeAll:
// 		// min
// 		minVal := make([]uint32, btree.KeyLen)
// 		for i := 0; i < int(btree.KeyLen); i++ {
// 			minVal[i] = storage.MinTargetValue
// 		}
// 		min = storage.NewBytes(minVal...)
// 		// max
// 		maxVal := make([]uint32, btree.KeyLen)
// 		for i := 0; i < int(btree.KeyLen); i++ {
// 			maxVal[i] = storage.MaxTargetValue
// 		}
// 		min = storage.NewBytes(maxVal...)
// 	case storage.SearchModeConst:
// 		// 同じ値が入る、ただしNewBytes()に渡す引数の長さは応相談
// 		min = storage.NewBytes(0)
// 		max = storage.NewBytes(0)
// 	case storage.SearchModeRange:
// 		// NewByets()に渡す引数の長さは応相談
// 		min = storage.NewBytes(0)
// 		max = storage.NewBytes(100)
// 	}
// 	pages, err := btree.SearchBy(dm, min, max, *len)
// 	if err != nil {
// 		panic(err)
// 	}
// 	var rows []any
// 	for _, p := range pages {
// 		// rowsに詰め替える
// 	}
// 	// 他の処理 e.g.where句での絞り込みとか、ジョイントか、
// }

// type hoge struct {
// 	index      string
// 	mode       storage.SearchMode
// 	minKeyVal  []uint32 // allの時は空
// 	maxKeyVal  []uint32 // allの時は空
// 	keyLen     *uint32  // allの時は渡されない
// 	whereStmts []WhereStmt
// }

// // allの時はkeylen分のBytesを作成したいけど、どこでインデックスのkeylenを持たせるか
// // bplustreeが持っているからそこから渡す？
// // constとrangeの時も実際のkeylenより長かったらカットする必要あり？そこまでやらなくても良い？
// func (h *hoge) minTargetVal() storage.Bytes {
// 	vals := make([]uint32, len(h.minKeyVal))
// 	switch h.mode {
// 	case storage.SearchModeAll:
// 	case storage.SearchModeConst:
// 	case storage.SearchModeRange:
// 	}
// }

// type WhereStmt struct {
// 	colName string // 何番目かのバリューかで判断するかも？
// 	mode    string // < == > どれ
// 	val     uint32 // 比較対象の値
// }
