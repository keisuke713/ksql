package storage

import (
	"fmt"
	"os"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("BPustTreeのテスト", func() {
	Describe("InsertPair", func() {
		var (
			btree BPlustTree

			dm DiskManager
		)
		BeforeEach(func() {
			f, _ := os.Create("insert_test_table")
			// f, _ := os.OpenFile("insert_test_table", os.O_RDWR, 0666)
			dm = NewDiskManager(f)
		})
		JustBeforeEach(func() {
			btree = *NewBPlustTree()
			// btree.RootNodeID = PageID(0)
			// btree.InsertPair(dm, 60, 60)
			var i uint32
			// key40が消える。2段目に中間ノードとリーフができる。PageID2と4が消える
			// rootを分割するときデフォでleafになっているのがいけない
			// 新しく中間リーフになる5と6がそれぞれleafと繋がっていない？rightPointerも多分使えていない
			for i = 0; i < 6; i++ {
				btree.InsertPair(dm, i*10, i*10)
			}
			fmt.Println("===============")
			btree.PrintAll(dm)
		})
		Context("", func() {
			FIt("", func() {
				Expect(1).To(Equal(2))
			})
		})
	})
})
