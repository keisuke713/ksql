package storage

import (
	"encoding/json"
	"fmt"
	"os"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Pageのテスト", func() {
	var (
		expected *Page
		actual   *Page
	)
	Describe("NewPage", func() {
		var (
			bytes [PageSize]byte

			err error
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
	Describe("SearchBy", func() {
		var (
			p   *Page
			err error

			dm DiskManager

			key  uint32
			mode SearchMode

			res []Pair
		)
		// TODO
		// 一回だけ呼ばれれば良いのだが
		BeforeEach(func() {
			f, _ := os.Create("test_table")
			dm = NewDiskManager(f)
			CreateTestPage(dm)

			stat, _ := f.Stat()
			fmt.Println("size: ", stat.Size())
		})
		JustBeforeEach(func() {
			p, err = NewPage([4096]byte(dm.ReadPageData(PageID(0))))
			if err != nil {
				panic(err)
			}
			fmt.Println("page: ", p)

			res, err = p.SearchBy(dm, key, mode)
		})
		Context("定数での絞り込みの場合", func() {
			BeforeEach(func() {
				mode = SearchModeConst
			})
			Context("対象が見つかった場合", func() {
				BeforeEach(func() {
					key = 31
				})
				It("resは27のペア", func() {
					Expect(res[0]).To(Equal(Pair{31, 31}))
				})
				It("errはnil", func() {
					Expect(err).To(BeNil())
				})
			})
			Context("見つからない場合", func() {
				BeforeEach(func() {
					key = 5
				})
				It("resは空", func() {
					Expect(len(res)).To(Equal(0))
				})
				It("errはnil", func() {
					Expect(err).To(BeNil())
				})
			})
		})
		// 複数ページにまたがるように
		Context("範囲検索の場合", func() {
			BeforeEach(func() {
				mode = SearchModeRange
			})
			Context("greaterのみの場合", func() {
				// 20以上
			})
			Context("lessのみの場合", func() {
				// 4以下
			})
			Context("下限上限両方指定されている場合", func() {
				// 5以上15以下
			})
		})
		// TODO IN検索
	})
})

// 0がroot
// 1~3がinternal node
// 4~12がleaf
func CreateTestPage(dm DiskManager) {
	// rootPage
	// id 0
	createTestPage(dm, dm.AllocatePage(), NodeTypeBranch, InvalidPageID, InvalidPageID, PageID(3), []Pair{
		{10, 1},
		{30, 2},
	})
	// internal node
	// id 1
	createTestPage(dm, dm.AllocatePage(), NodeTypeBranch, InvalidPageID, PageID(2), PageID(6), []Pair{
		{2, 4},
		{5, 5},
	})
	// id 2
	createTestPage(dm, dm.AllocatePage(), NodeTypeBranch, PageID(1), PageID(3), PageID(9), []Pair{
		{11, 7},
		{20, 8},
	})
	// id 3
	createTestPage(dm, dm.AllocatePage(), NodeTypeBranch, PageID(2), InvalidPageID, PageID(12), []Pair{
		{32, 10},
		{40, 11},
	})
	// leaf node
	// id 4
	createTestPage(dm, dm.AllocatePage(), NodeTypeLeaf, InvalidPageID, PageID(5), InvalidPageID, []Pair{
		{1, 1},
	})
	// id 5
	createTestPage(dm, dm.AllocatePage(), NodeTypeLeaf, PageID(4), PageID(6), InvalidPageID, []Pair{
		{2, 2},
		{4, 4},
	})
	// id 6
	createTestPage(dm, dm.AllocatePage(), NodeTypeLeaf, PageID(5), PageID(7), InvalidPageID, []Pair{
		{6, 6},
	})
	// id 7
	createTestPage(dm, dm.AllocatePage(), NodeTypeLeaf, PageID(6), PageID(8), InvalidPageID, []Pair{
		{7, 10},
	})
	// id 8
	createTestPage(dm, dm.AllocatePage(), NodeTypeLeaf, PageID(7), PageID(9), InvalidPageID, []Pair{
		{12, 12},
		{13, 13},
	})
	// id 9
	createTestPage(dm, dm.AllocatePage(), NodeTypeLeaf, PageID(8), PageID(10), InvalidPageID, []Pair{
		{21, 21},
	})
	// id 10
	createTestPage(dm, dm.AllocatePage(), NodeTypeLeaf, PageID(9), PageID(11), InvalidPageID, []Pair{
		{31, 31},
	})
	// id 11
	createTestPage(dm, dm.AllocatePage(), NodeTypeLeaf, PageID(10), PageID(12), InvalidPageID, []Pair{
		{36, 36},
		{38, 38},
	})
	// id 12
	createTestPage(dm, dm.AllocatePage(), NodeTypeLeaf, PageID(11), InvalidPageID, InvalidPageID, []Pair{
		{43, 43},
		{46, 46},
	})
}

func createTestPage(dm DiskManager, pageID PageID, nodeType NodeType, prevID PageID, nextID PageID, right PageID, kvs []Pair) {
	page := Page{
		pageID,
		nodeType,
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
