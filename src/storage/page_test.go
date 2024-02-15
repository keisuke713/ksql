package storage

import (
	"encoding/json"
	"fmt"
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
							uint32(3),
							int32(5),
						},
						{
							uint32(6),
							int32(6),
						},
						{
							uint32(9),
							int32(7),
						},
						{
							uint32(12),
							int32(8),
						},
						{
							uint32(15),
							int32(9),
						},
					},
				}
				pBytes, _ := json.Marshal((*actual))
				bytes = [PageSize]byte{}
				// 4KBのバイト列に詰め直す
				for i, b := range pBytes {
					bytes[i] = b
				}
			})
			It("marchal後も同じ値になる", func() {
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
							uint32(1),
							int32(5),
						},
						{
							uint32(2),
							int32(6),
						},
						{
							uint32(3),
							int32(7),
						},
						{
							uint32(4),
							int32(8),
						},
						{
							uint32(5),
							int32(9),
						},
					},
				}
				pBytes, _ := json.Marshal((*actual))
				bytes = [PageSize]byte{}
				// 4KBのバイト列に詰め直す
				for i, b := range pBytes {
					bytes[i] = b
				}
			})
			It("marchal後も同じ値になる", func() {
				fmt.Println(*expected)
				fmt.Println(*actual)
				Expect(*expected).To(Equal((*actual)))
			})
			It("errはnil", func() {
				Expect(err).To(BeNil())
			})
		})
	})
	// Describe("SearchBy", func() {
	// 	var (
	// 		p   *Page
	// 		err error

	// 		dm DiskManager

	// 		minTargetVal uint32
	// 		maxTargetVal uint32
	// 		mode         SearchMode

	// 		res []Pair
	// 	)
	// 	// TODO
	// 	// 一回だけ呼ばれれば良いのだが
	// 	BeforeEach(func() {
	// 		f, _ := os.Create("test_table")
	// 		dm = NewDiskManager(f)
	// 		CreateTestPage(dm)
	// 	})
	// 	JustBeforeEach(func() {
	// 		p, err = NewPage([4096]byte(dm.ReadPageData(PageID(0))))
	// 		if err != nil {
	// 			panic(err)
	// 		}

	// 		res, err = p.SearchBy(dm, minTargetVal, maxTargetVal, mode)
	// 	})
	// 	Context("定数での絞り込みの場合", func() {
	// 		BeforeEach(func() {
	// 			mode = SearchModeConst
	// 		})
	// 		Context("対象が見つかった場合", func() {
	// 			BeforeEach(func() {
	// 				minTargetVal = 31
	// 			})
	// 			It("resは27のペア", func() {
	// 				Expect(res[0]).To(Equal(Pair{31, 31}))
	// 			})
	// 			It("errはnil", func() {
	// 				Expect(err).To(BeNil())
	// 			})
	// 		})
	// 		Context("見つからない場合", func() {
	// 			BeforeEach(func() {
	// 				minTargetVal = 5
	// 			})
	// 			It("resは空", func() {
	// 				Expect(len(res)).To(Equal(0))
	// 			})
	// 			It("errはnil", func() {
	// 				Expect(err).To(BeNil())
	// 			})
	// 		})
	// 	})
	// 	// 複数ページにまたがるように
	// 	Context("範囲検索の場合", func() {
	// 		BeforeEach(func() {
	// 			// mode = SearchModeGreater
	// 		})
	// 		Context("greaterのみの場合", func() {
	// 			// 20より大きい
	// 			BeforeEach(func() {
	// 				mode = SearchModeRange
	// 				minTargetVal = 20
	// 				maxTargetVal = MaxTargetValue
	// 			})
	// 			It("20より大きい値が5つ取れる", func() {
	// 				Expect(res[0]).To(Equal(Pair{21, 21}))
	// 				Expect(res[1]).To(Equal(Pair{31, 31}))
	// 				Expect(res[2]).To(Equal(Pair{36, 36}))
	// 				Expect(res[3]).To(Equal(Pair{38, 38}))
	// 				Expect(res[4]).To(Equal(Pair{43, 43}))
	// 				Expect(res[5]).To(Equal(Pair{46, 46}))
	// 			})
	// 			It("errはnil", func() {
	// 				Expect(err).To(BeNil())
	// 			})
	// 		})
	// 		Context("lessのみの場合", func() {
	// 			// 4未満
	// 			BeforeEach(func() {
	// 				mode = SearchModeRange
	// 				minTargetVal = MinTargetValue
	// 				maxTargetVal = 4
	// 			})
	// 			It("4未満の値が3つ取れる", func() {
	// 				Expect(res[0]).To(Equal(Pair{1, 1}))
	// 				Expect(res[1]).To(Equal(Pair{2, 2}))
	// 			})
	// 			It("errはnil", func() {
	// 				Expect(err).To(BeNil())
	// 			})
	// 		})
	// 		Context("下限上限両方指定されている場合", func() {
	// 			// 5より大きい15未満
	// 			BeforeEach(func() {
	// 				mode = SearchModeRange
	// 				minTargetVal = 5
	// 				maxTargetVal = 15
	// 			})
	// 			It("5以上15以下の値が4つ取れる", func() {
	// 				Expect(res[0]).To(Equal(Pair{6, 6}))
	// 				Expect(res[1]).To(Equal(Pair{7, 10}))
	// 				Expect(res[2]).To(Equal(Pair{12, 12}))
	// 				Expect(res[3]).To(Equal(Pair{13, 13}))
	// 			})
	// 			It("errはnil", func() {
	// 				Expect(err).To(BeNil())
	// 			})
	// 		})
	// 	})
	// })
	// Describe("SearchByV2", func() {
	// 	var (
	// 		p   *Page
	// 		err error

	// 		dm DiskManager

	// 		minTargetVal uint32
	// 		maxTargetVal uint32

	// 		minPageID *PageID
	// 		maxPageID *PageID
	// 	)
	// 	// TODO
	// 	// 一回だけ呼ばれれば良いのだが
	// 	BeforeEach(func() {
	// 		f, _ := os.Create("test_table")
	// 		dm = NewDiskManager(f)
	// 		CreateTestPage(dm)
	// 	})
	// 	JustBeforeEach(func() {
	// 		p, err = NewPage([PageSize]byte(dm.ReadPageData(PageID(0))))
	// 		if err != nil {
	// 			panic(err)
	// 		}
	// 		minPageID, maxPageID, err = p.SearchByV2(dm, minTargetVal, maxTargetVal)
	// 	})
	// 	JustAfterEach(func() {
	// 		minPageID = nil
	// 		maxPageID = nil
	// 	})
	// 	Context("定数での絞り込みの場合", func() {
	// 		Context("対象が見つかった場合", func() {
	// 			BeforeEach(func() {
	// 				minTargetVal = 31
	// 				maxTargetVal = 31
	// 			})
	// 			It("minPageIDは10", func() {
	// 				Expect(*minPageID).To(Equal(PageID(10)))
	// 			})
	// 			It("maxPageIDは10", func() {
	// 				Expect(*maxPageID).To(Equal(PageID(10)))
	// 			})
	// 			It("errはnil", func() {
	// 				Expect(err).To(BeNil())
	// 			})
	// 		})
	// 		Context("見つからない場合", func() {
	// 			BeforeEach(func() {
	// 				minTargetVal = 5
	// 				maxTargetVal = 5
	// 			})
	// 			It("minPageIDは6", func() {
	// 				Expect(*minPageID).To(Equal(PageID(6)))
	// 			})
	// 			It("maxPageIDはnil", func() {
	// 				Expect(*maxPageID).To(Equal(PageID(6)))
	// 			})
	// 			It("errはnil", func() {
	// 				Expect(err).To(BeNil())
	// 			})
	// 		})
	// 	})
	// 	// 複数ページにまたがるように
	// 	Context("範囲検索の場合", func() {
	// 		Context("greaterのみの場合", func() {
	// 			// 20より大きい
	// 			BeforeEach(func() {
	// 				minTargetVal = 20
	// 				maxTargetVal = MaxTargetValue
	// 			})
	// 			Context("対象が見つかった場合", func() {
	// 				It("minPageIDは9", func() {
	// 					Expect(*minPageID).To(Equal(PageID(9)))
	// 				})
	// 				It("maxPageIDは12", func() {
	// 					Expect(*maxPageID).To(Equal(PageID(12)))
	// 				})
	// 				It("errはnil", func() {
	// 					Expect(err).To(BeNil())
	// 				})
	// 			})
	// 		})
	// 		Context("lessのみの場合", func() {
	// 			// 4未満
	// 			BeforeEach(func() {
	// 				minTargetVal = MinTargetValue
	// 				maxTargetVal = 4
	// 			})
	// 			It("minPageIDは4", func() {
	// 				Expect(*minPageID).To(Equal(PageID(4)))
	// 			})
	// 			It("maxPageIDは5", func() {
	// 				Expect(*maxPageID).To(Equal(PageID(5)))
	// 			})
	// 			It("errはnil", func() {
	// 				Expect(err).To(BeNil())
	// 			})
	// 		})
	// 		Context("下限上限両方指定されている場合", func() {
	// 			// 5より大きい15未満
	// 			BeforeEach(func() {
	// 				minTargetVal = 5
	// 				maxTargetVal = 15
	// 			})
	// 			It("minPageIDは6", func() {
	// 				Expect(*minPageID).To(Equal(PageID(6)))
	// 			})
	// 			It("maxPageIDは9", func() {
	// 				Expect(*maxPageID).To(Equal(PageID(9)))
	// 			})
	// 			It("errはnil", func() {
	// 				Expect(err).To(BeNil())
	// 			})
	// 		})
	// 	})
	// 	// TODO
	// 	// minとmaxにそれぞれ指定したら良いだけ
	// 	Context("フルスキャン", func() {
	// 		BeforeEach(func() {
	// 			minTargetVal = MinTargetValue
	// 			maxTargetVal = MaxTargetValue
	// 		})
	// 	})
	// })
	Describe("SearchByV3", func() {
		var (
			p   *Page
			err error

			dm DiskManager

			minTargetVal uint32
			maxTargetVal uint32

			res []*Page
		)
		BeforeEach(func() {
			f, _ := os.Create("test_table")
			dm = NewDiskManager(f)
			CreateTestPage(dm)
		})
		JustBeforeEach(func() {
			p, err = NewPage([PageSize]byte(dm.ReadPageData(PageID(0))))
			if err != nil {
				panic(err)
			}
			res, err = p.SearchByV3(dm, minTargetVal, maxTargetVal)
		})
		Context("定数での絞り込みの場合", func() {
			Context("対象が見つかった場合", func() {
				BeforeEach(func() {
					minTargetVal = 31
					maxTargetVal = 31
				})
				It("PageID10のPageが含まれる", func() {
					Expect(res[0].PageID).To(Equal(PageID(10)))
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
				It("PageID5のPageが含まれる", func() {
					Expect(res[0].PageID).To(Equal(PageID(5)))
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
					It("PageID8~12のpageが含まれる", func() {
						Expect(res[0].PageID).To(Equal(PageID(8)))
						Expect(res[1].PageID).To(Equal(PageID(9)))
						Expect(res[2].PageID).To(Equal(PageID(10)))
						Expect(res[3].PageID).To(Equal(PageID(11)))
						Expect(res[4].PageID).To(Equal(PageID(12)))
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
				It("PageID4~5のpageが含まれる", func() {
					Expect(res[0].PageID).To(Equal(PageID(4)))
					Expect(res[1].PageID).To(Equal(PageID(5)))
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
				It("PageID6~8のpageが含まれる", func() {
					Expect(res[0].PageID).To(Equal(PageID(5)))
					Expect(res[1].PageID).To(Equal(PageID(6)))
					Expect(res[2].PageID).To(Equal(PageID(7)))
					Expect(res[3].PageID).To(Equal(PageID(8)))
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
			p, err = NewPage([PageSize]byte(dm.ReadPageData(PageID(0))))
			if err != nil {
				panic(err)
			}
			var i uint32
			for i = 0; i < 3; i++ {
				p.InsertPair(dm, i*10, i*10)
			}

			// fmt.Println("inserting 2 ...")
			// leftLeaf, err := NewPage([PageSize]byte(dm.ReadPageData(PageID(1))))
			// if err != nil {
			// 	panic(err)
			// }
			// leftLeaf.InsertPair(dm, 2, 20)

			// fmt.Println("")
			// p.PrintAll(dm, "")
			// leftLeaf2, err := NewPage([PageSize]byte(dm.ReadPageData(PageID(3))))
			// if err != nil {
			// 	panic(err)
			// }
			// fmt.Printf("pageID3: %+v", leftLeaf2)
			p, err = NewPage([PageSize]byte(dm.ReadPageData(PageID(0))))
			if err != nil {
				panic(err)
			}
			fmt.Println("")
			p.PrintAll(dm, "")
		})
		Context("", func() {
			It("", func() {
				Expect(1).To(Equal(2))
			})
		})
	})
})

func CreateRootPage(dm DiskManager) {
	// id 0
	createTestPage(dm, dm.AllocatePage(), NodeTypeLeaf, InvalidPageID, InvalidPageID, InvalidPageID, InvalidPageID, []Pair{
		{10, 100},
	})
}

// 0がroot
// 1~3がinternal node
// 4~12がleaf
func CreateTestPage(dm DiskManager) {
	// rootPage
	// id 0
	createTestPage(dm, dm.AllocatePage(), NodeTypeBranch, InvalidPageID, InvalidPageID, InvalidPageID, PageID(3), []Pair{
		{10, 1},
		{30, 2},
	})
	// internal node
	// id 1
	createTestPage(dm, dm.AllocatePage(), NodeTypeBranch, PageID(0), InvalidPageID, PageID(2), PageID(6), []Pair{
		{2, 4},
		{5, 5},
	})
	// id 2
	createTestPage(dm, dm.AllocatePage(), NodeTypeBranch, PageID(0), PageID(1), PageID(3), PageID(9), []Pair{
		{11, 7},
		{20, 8},
	})
	// id 3
	createTestPage(dm, dm.AllocatePage(), NodeTypeBranch, PageID(0), PageID(2), InvalidPageID, PageID(12), []Pair{
		{32, 10},
		{40, 11},
	})
	// leaf node
	// id 4
	createTestPage(dm, dm.AllocatePage(), NodeTypeLeaf, PageID(1), InvalidPageID, PageID(5), InvalidPageID, []Pair{
		{1, 1},
	})
	// id 5
	createTestPage(dm, dm.AllocatePage(), NodeTypeLeaf, PageID(1), PageID(4), PageID(6), InvalidPageID, []Pair{
		{2, 2},
		{4, 4},
	})
	// id 6
	createTestPage(dm, dm.AllocatePage(), NodeTypeLeaf, PageID(1), PageID(5), PageID(7), InvalidPageID, []Pair{
		{6, 6},
	})
	// id 7
	createTestPage(dm, dm.AllocatePage(), NodeTypeLeaf, PageID(2), PageID(6), PageID(8), InvalidPageID, []Pair{
		{7, 10},
	})
	// id 8
	createTestPage(dm, dm.AllocatePage(), NodeTypeLeaf, PageID(2), PageID(7), PageID(9), InvalidPageID, []Pair{
		{12, 12},
		{13, 13},
	})
	// id 9
	createTestPage(dm, dm.AllocatePage(), NodeTypeLeaf, PageID(2), PageID(8), PageID(10), InvalidPageID, []Pair{
		{21, 21},
	})
	// id 10
	createTestPage(dm, dm.AllocatePage(), NodeTypeLeaf, PageID(3), PageID(9), PageID(11), InvalidPageID, []Pair{
		{31, 31},
	})
	// id 11
	createTestPage(dm, dm.AllocatePage(), NodeTypeLeaf, PageID(3), PageID(10), PageID(12), InvalidPageID, []Pair{
		{36, 36},
		{38, 38},
	})
	// id 12
	createTestPage(dm, dm.AllocatePage(), NodeTypeLeaf, PageID(3), PageID(11), InvalidPageID, InvalidPageID, []Pair{
		{43, 43},
		{46, 46},
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
	bytes, err := json.Marshal(page)
	if err != nil {
		panic(err)
	}
	pp := [PageSize]byte{}
	for i, b := range bytes {
		pp[i] = b
	}
	dm.WritePageData(pageID, pp)
}
