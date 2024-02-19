package storage

import (
	"os"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("BPustTreeのテスト", func() {
	Describe("InsertPair", func() {
		var (
			btree BPlustTree

			dm DiskManager

			res []Page
		)
		BeforeEach(func() {
			f, _ := os.OpenFile("insert_test_table", os.O_RDWR, 0666)
			dm = NewDiskManager(f)
		})
		JustBeforeEach(func() {
			btree = *NewBPlustTree()
			btree.RootNodeID = PageID(1) // 始点を1にする
			var i uint32
			for i = 3; i < 7; i++ {
				btree.InsertPair(dm, NewBytes(i), NewBytes(i))
			}
			res = btree.Slice(dm)
		})
		JustAfterEach(func() {
			fcp, _ := os.OpenFile("insert_test_table_cp", os.O_RDONLY, 0644)
			b := make([]byte, PageSize*4)
			fcp.Read(b)
			f, _ := os.Create("insert_test_table")
			f.Write(b)
		})
		// TODO テストの時はページごとの上限を2にする(通常は4KBいっぱい)
		Context("0から順番に6まで挿入した場合", func() {
			BeforeEach(func() {
			})
			It("深さが2のB+Treeになる", func() {
				Expect(len(res)).To(Equal(7))
				Expect(res[0]).To(Equal(Page{
					PageID(1),
					NodeTypeBranch,
					PageID(0),
					PageID(0),
					PageID(0),
					PageID(8),
					[]Pair{
						{
							NewBytes(3),
							NewBytes(7),
						},
					},
				}))
				Expect(res[1]).To(Equal(Page{
					PageID(7),
					NodeTypeBranch,
					PageID(1),
					PageID(0),
					PageID(8),
					PageID(0),
					[]Pair{
						{
							NewBytes(1),
							NewBytes(2),
						},
						{
							NewBytes(3),
							NewBytes(5),
						},
					},
				}))
				Expect(res[2]).To(Equal(Page{
					PageID(2),
					NodeTypeLeaf,
					PageID(7),
					PageID(0),
					PageID(5),
					PageID(0),
					[]Pair{
						{
							NewBytes(0),
							NewBytes(0),
						},
						{
							NewBytes(1),
							NewBytes(1),
						},
					},
				}))
				Expect(res[3]).To(Equal(Page{
					PageID(5),
					NodeTypeLeaf,
					PageID(7),
					PageID(2),
					PageID(6),
					PageID(0),
					[]Pair{
						{
							NewBytes(2),
							NewBytes(2),
						},
						{
							NewBytes(3),
							NewBytes(3),
						},
					},
				}))
				Expect(res[4]).To(Equal(Page{
					PageID(8),
					NodeTypeBranch,
					PageID(1),
					PageID(7),
					PageID(0),
					PageID(3),
					[]Pair{
						{
							NewBytes(5),
							NewBytes(6),
						},
					},
				}))
				Expect(res[5]).To(Equal(Page{
					PageID(6),
					NodeTypeLeaf,
					PageID(8),
					PageID(5),
					PageID(3),
					PageID(0),
					[]Pair{
						{
							NewBytes(4),
							NewBytes(4),
						},
						{
							NewBytes(5),
							NewBytes(5),
						},
					},
				}))
				Expect(res[6]).To(Equal(Page{
					PageID(3),
					NodeTypeLeaf,
					PageID(8),
					PageID(6),
					PageID(0),
					PageID(0),
					[]Pair{
						{
							NewBytes(6),
							NewBytes(6),
						},
					},
				}))
			})
		})
	})
})
