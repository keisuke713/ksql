package storage

import (
	"os"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Pageのテスト", func() {
	Describe("NewPage", func() {
		var (
			bytes [PageSize]byte

			err error

			expected, actual *Page
		)
		JustBeforeEach(func() {
			expected, err = NewPage(bytes)
		})
		Context("中間ノードの場合", func() {
			BeforeEach(func() {
				actual = &Page{
					PageID:       PageID(2),
					NodeType:     NodeTypeBranch,
					PrevPageID:   PageID(1),
					NextPageID:   PageID(3),
					RightPointer: PageID(4),
					Items: []Pair{
						{
							NewBytes(3),
							NewBytes(6),
						},
						{
							NewBytes(6),
							NewBytes(12),
						},
						{
							NewBytes(9),
							NewBytes(18),
						},
						{
							NewBytes(12),
							NewBytes(24),
						},
						{
							NewBytes(15),
							NewBytes(30),
						},
						{
							NewBytes(65535),
							NewBytes(65536),
						},
					},
				}
				bytes = actual.Bytes()
			})
			It("デコード後も同じ値になる", func() {
				Expect(*expected).To(Equal((*actual)))
			})
			It("errはnil", func() {
				Expect(err).To(BeNil())
			})
		})
		Context("リーフの場合", func() {
			BeforeEach(func() {
				actual = &Page{
					PageID:       PageID(2),
					NodeType:     NodeTypeLeaf,
					PrevPageID:   PageID(1),
					NextPageID:   PageID(3),
					RightPointer: InvalidPageID,
					Items: []Pair{
						{
							NewBytes(1),
							NewBytes(5),
						},
						{
							NewBytes(2),
							NewBytes(6),
						},
						{
							NewBytes(3),
							NewBytes(7),
						},
						{
							NewBytes(4),
							NewBytes(8),
						},
						{
							NewBytes(5),
							NewBytes(9),
						},
					},
				}
				bytes = actual.Bytes()
			})
			It("デコード後も同じ値になる", func() {
				Expect(*expected).To(Equal((*actual)))
			})
			It("errはnil", func() {
				Expect(err).To(BeNil())
			})
		})
	})
	Describe("SearchByV3", func() {
		var (
			p   *Page
			err error

			dm DiskManager

			minTargetVal uint32
			maxTargetVal uint32

			len uint32 = ColumnSize

			res []*Page
		)
		BeforeEach(func() {
			f, _ := os.Create("test_table")
			dm = NewDiskManager(f)
			CreateTestPage(dm)
		})
		JustBeforeEach(func() {
			p, err = NewPage([PageSize]byte(dm.ReadPageData(PageID(1))))
			if err != nil {
				panic(err)
			}
			minBytes := NewBytes(minTargetVal)
			maxBytes := NewBytes(maxTargetVal)
			res, err = p.SearchByV3(dm, minBytes, maxBytes, len)
		})
		Context("定数での絞り込みの場合", func() {
			Context("対象が見つかった場合", func() {
				BeforeEach(func() {
					minTargetVal = 31
					maxTargetVal = 31
				})
				It("PageID11のPageが含まれる", func() {
					Expect(res[0].PageID).To(Equal(PageID(11)))
				})
				It("errはnil", func() {
					Expect(err).To(BeNil())
				})
			})
			Context("見つからない場合", func() {
				BeforeEach(func() {
					minTargetVal = 5
					maxTargetVal = 5
				})
				It("PageID6のPageが含まれる", func() {
					Expect(res[0].PageID).To(Equal(PageID(6)))
				})
				It("errはnil", func() {
					Expect(err).To(BeNil())
				})
			})
		})
		// 複数ページにまたがるように
		Context("範囲検索の場合", func() {
			Context("greaterのみの場合", func() {
				// 20より大きい
				BeforeEach(func() {
					minTargetVal = 20
					maxTargetVal = MaxTargetValue
				})
				Context("対象が見つかった場合", func() {
					It("PageID9~13のpageが含まれる", func() {
						Expect(res[0].PageID).To(Equal(PageID(9)))
						Expect(res[1].PageID).To(Equal(PageID(10)))
						Expect(res[2].PageID).To(Equal(PageID(11)))
						Expect(res[3].PageID).To(Equal(PageID(12)))
						Expect(res[4].PageID).To(Equal(PageID(13)))
					})
					It("errはnil", func() {
						Expect(err).To(BeNil())
					})
				})
			})
			Context("lessのみの場合", func() {
				// 4未満
				BeforeEach(func() {
					minTargetVal = MinTargetValue
					maxTargetVal = 4
				})
				It("PageID5~6のpageが含まれる", func() {
					Expect(res[0].PageID).To(Equal(PageID(5)))
					Expect(res[1].PageID).To(Equal(PageID(6)))
				})
				It("errはnil", func() {
					Expect(err).To(BeNil())
				})
			})
			Context("下限上限両方指定されている場合", func() {
				// 5より大きい15未満
				BeforeEach(func() {
					minTargetVal = 5
					maxTargetVal = 15
				})
				It("PageID6~9のpageが含まれる", func() {
					Expect(res[0].PageID).To(Equal(PageID(6)))
					Expect(res[1].PageID).To(Equal(PageID(7)))
					Expect(res[2].PageID).To(Equal(PageID(8)))
					Expect(res[3].PageID).To(Equal(PageID(9)))
				})
				It("errはnil", func() {
					Expect(err).To(BeNil())
				})
			})
		})
	})
	PDescribe("InsertPair", func() {
		var (
			p   *Page
			err error

			dm DiskManager
		)
		BeforeEach(func() {
			f, _ := os.Create("insert_test_table")
			dm = NewDiskManager(f)
			CreateRootPage(dm)
		})
		JustBeforeEach(func() {
			p, err = NewPage([PageSize]byte(dm.ReadPageData(PageID(1))))
			if err != nil {
				panic(err)
			}
			var i uint32
			for i = 0; i < 3; i++ {
				p.InsertPair(dm, NewBytes(1<<i), NewBytes(1<<i))
			}
			p, err = NewPage([PageSize]byte(dm.ReadPageData(PageID(1))))
			if err != nil {
				panic(err)
			}
		})
		Context("", func() {
			It("", func() {
				Expect(1).To(Equal(2))
			})
		})
	})
	Describe("NBytes", func() {
		var (
			p     *Page
			nByte uint32
		)
		JustBeforeEach(func() {
			nByte = p.NBytes()
		})
		Context("キーバリューペアが空の場合", func() {
			BeforeEach(func() {
				p = &Page{
					PageID:       PageID(1),
					NodeType:     NodeTypeBranch,
					ParentID:     PageID(0),
					PrevPageID:   PageID(0),
					NextPageID:   PageID(0),
					RightPointer: PageID(2),
				}
			})
			It("24バイトが返る", func() {
				Expect(nByte).To(Equal(uint32(24)))
			})
		})
		Context("キーバリューペアが存在する場合", func() {
			BeforeEach(func() {
				p = &Page{
					PageID:       PageID(1),
					NodeType:     NodeTypeBranch,
					ParentID:     PageID(0),
					PrevPageID:   PageID(0),
					NextPageID:   PageID(0),
					RightPointer: PageID(2),
					Items: []Pair{
						{
							NewBytes(1, 2),
							NewBytes(1, 2, 3),
						},
						{
							NewBytes(255),
							NewBytes(),
						},
					},
				}
			})
			It("72バイトが返る", func() {
				Expect(nByte).To(Equal(uint32(72)))
			})
		})
	})
})

func CreateRootPage(dm DiskManager) {
	// id 1
	createTestPage(dm, dm.AllocatePage(), NodeTypeLeaf, InvalidPageID, InvalidPageID, InvalidPageID, InvalidPageID, []Pair{
		{NewBytes(10), NewBytes(100)},
	})
}

// 1がroot
// 2~4がinternal node
// 5~13がleaf
func CreateTestPage(dm DiskManager) {
	// rootPage
	// id 1
	createTestPage(dm, dm.AllocatePage(), NodeTypeBranch, InvalidPageID, InvalidPageID, InvalidPageID, PageID(4), []Pair{
		{NewBytes(10), NewBytes(2)},
		{NewBytes(30), NewBytes(3)},
	})
	// internal node
	// id 2
	createTestPage(dm, dm.AllocatePage(), NodeTypeBranch, PageID(1), InvalidPageID, PageID(3), PageID(7), []Pair{
		{NewBytes(2), NewBytes(5)},
		{NewBytes(5), NewBytes(6)},
	})
	// id 3
	createTestPage(dm, dm.AllocatePage(), NodeTypeBranch, PageID(1), PageID(2), PageID(4), PageID(10), []Pair{
		{NewBytes(11), NewBytes(8)},
		{NewBytes(20), NewBytes(9)},
	})
	// id 4
	createTestPage(dm, dm.AllocatePage(), NodeTypeBranch, PageID(1), PageID(3), InvalidPageID, PageID(13), []Pair{
		{NewBytes(32), NewBytes(11)},
		{NewBytes(40), NewBytes(12)},
	})
	// leaf node
	// id 5
	createTestPage(dm, dm.AllocatePage(), NodeTypeLeaf, PageID(2), InvalidPageID, PageID(6), InvalidPageID, []Pair{
		{NewBytes(1), NewBytes(1)},
	})
	// id 6
	createTestPage(dm, dm.AllocatePage(), NodeTypeLeaf, PageID(2), PageID(5), PageID(7), InvalidPageID, []Pair{
		{NewBytes(2), NewBytes(2)},
		{NewBytes(4), NewBytes(4)},
	})
	// id 7
	createTestPage(dm, dm.AllocatePage(), NodeTypeLeaf, PageID(2), PageID(6), PageID(8), InvalidPageID, []Pair{
		{NewBytes(6), NewBytes(6)},
	})
	// id 8
	createTestPage(dm, dm.AllocatePage(), NodeTypeLeaf, PageID(3), PageID(7), PageID(9), InvalidPageID, []Pair{
		{NewBytes(7), NewBytes(10)},
	})
	// id 9
	createTestPage(dm, dm.AllocatePage(), NodeTypeLeaf, PageID(3), PageID(8), PageID(10), InvalidPageID, []Pair{
		{NewBytes(12), NewBytes(12)},
		{NewBytes(13), NewBytes(13)},
	})
	// id 10
	createTestPage(dm, dm.AllocatePage(), NodeTypeLeaf, PageID(3), PageID(9), PageID(11), InvalidPageID, []Pair{
		{NewBytes(21), NewBytes(21)},
	})
	// id 11
	createTestPage(dm, dm.AllocatePage(), NodeTypeLeaf, PageID(4), PageID(10), PageID(12), InvalidPageID, []Pair{
		{NewBytes(31), NewBytes(31)},
	})
	// id 12
	createTestPage(dm, dm.AllocatePage(), NodeTypeLeaf, PageID(4), PageID(11), PageID(13), InvalidPageID, []Pair{
		{NewBytes(36), NewBytes(36)},
		{NewBytes(38), NewBytes(38)},
	})
	// id 13
	createTestPage(dm, dm.AllocatePage(), NodeTypeLeaf, PageID(4), PageID(12), InvalidPageID, InvalidPageID, []Pair{
		{NewBytes(43), NewBytes(43)},
		{NewBytes(46), NewBytes(46)},
	})
}

func createTestPage(dm DiskManager, pageID PageID, nodeType NodeType, parentID, prevID, nextID PageID, right PageID, kvs []Pair) {
	page := Page{
		pageID,
		nodeType,
		parentID,
		prevID,
		nextID,
		right,
		kvs,
	}
	b := page.Bytes()
	dm.WritePageData(pageID, b)
}
