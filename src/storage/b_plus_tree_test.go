package storage

import (
	"os"
	"strconv"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("BPustTreeのテスト", func() {
	Describe("InsertPair", func() {
		var (
			btree BPlustTree

			dm       DiskManager
			max      uint32
			pageSize string

			res []Page
		)
		BeforeEach(func() {
			f, _ := os.Create("insert_test_table")
			dm = NewDiskManager(f)
		})
		JustBeforeEach(func() {
			btree = *NewBPlustTree()
			os.Setenv(KSQLPageSizeKey, pageSize)
			var i uint32
			for i = 0; i < max; i++ {
				btree.InsertPair(dm, NewBytes(i), NewBytes(i))
			}
			res = btree.Slice(dm)
			btree.PrintAll(dm)
		})
		Context("0から順番に6まで挿入した場合", func() {
			BeforeEach(func() {
				max = 7
				pageSize = strconv.Itoa(64)
			})
			It("深さが2のB+Treeになる", func() {
				Expect(len(res)).To(Equal(7))
				Expect(res[0]).To(Equal(Page{
					PageID(1),
					NodeTypeBranch,
					PageID(0),
					PageID(0),
					PageID(0),
					PageID(7),
					[]Pair{
						{
							NewBytes(3),
							NewBytes(6),
						},
					},
				}))
				Expect(res[1]).To(Equal(Page{
					PageID(6),
					NodeTypeBranch,
					PageID(1),
					PageID(0),
					PageID(7),
					PageID(0),
					[]Pair{
						{
							NewBytes(1),
							NewBytes(2),
						},
						{
							NewBytes(3),
							NewBytes(4),
						},
					},
				}))
				Expect(res[2]).To(Equal(Page{
					PageID(2),
					NodeTypeLeaf,
					PageID(6),
					PageID(0),
					PageID(4),
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
					PageID(4),
					NodeTypeLeaf,
					PageID(6),
					PageID(2),
					PageID(5),
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
					PageID(7),
					NodeTypeBranch,
					PageID(1),
					PageID(6),
					PageID(0),
					PageID(3),
					[]Pair{
						{
							NewBytes(5),
							NewBytes(5),
						},
					},
				}))
				Expect(res[5]).To(Equal(Page{
					PageID(5),
					NodeTypeLeaf,
					PageID(7),
					PageID(4),
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
					PageID(7),
					PageID(5),
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
