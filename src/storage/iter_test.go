package storage

import (
	"os"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Iterのテスト", func() {
	var (
		dm DiskManager
	)
	BeforeEach(func() {
		f, _ := os.Create("test_table")
		dm = NewDiskManager(f)
		CreateTestPage(dm)
	})
	PDescribe("Nextのテスト", func() {
		var (
			i   *Iter
			err error

			minPageID *PageID
			maxPageID *PageID

			pages []*Page
		)
		JustBeforeEach(func() {
			i, err = NewIter(dm, minPageID, maxPageID)
			if err != nil {
				panic(err)
			}

			var (
				p   *Page
				err error
			)
			for {
				p, err = i.Next()
				if err != nil {
					panic(err)
				}
				if p == nil {
					break
				}
				pages = append(pages, p)
			}
		})
		JustAfterEach(func() {
			minPageID = nil
			maxPageID = nil

			pages = make([]*Page, 0)
		})
		Context("minのみ指定されている場合", func() {
			BeforeEach(func() {
				minPID := PageID(11)
				minPageID = &minPID

				maxPID := PageID(12)
				maxPageID = &maxPID
			})
			It("ID11・12のPageが取得される", func() {
				Expect(len(pages)).To(Equal(2))
				Expect(pages[0].PageID).To(Equal(PageID(11)))
				Expect(pages[1].PageID).To(Equal(PageID(12)))
			})
			It("errはnil", func() {
				Expect(err).To(BeNil())
			})
		})
		Context("maxのみ指定されている場合", func() {
			BeforeEach(func() {
				minPID := PageID(4)
				minPageID = &minPID

				maxPID := PageID(4)
				maxPageID = &maxPID
			})
			It("ID4のPageが取得される", func() {
				Expect(len(pages)).To(Equal(1))
				Expect(pages[0].PageID).To(Equal(PageID(4)))
			})
			It("errはnil", func() {
				Expect(err).To(BeNil())
			})
		})
		Context("minとmaxともに指定されている場合", func() {
			BeforeEach(func() {
				minPID := PageID(4)
				minPageID = &minPID

				maxPID := PageID(11)
				maxPageID = &maxPID
			})
			It("ID4~11のPageが取得される", func() {
				Expect(len(pages)).To(Equal(8))
				Expect(pages[0].PageID).To(Equal(PageID(4)))
				Expect(pages[1].PageID).To(Equal(PageID(5)))
				Expect(pages[2].PageID).To(Equal(PageID(6)))
				Expect(pages[3].PageID).To(Equal(PageID(7)))
				Expect(pages[4].PageID).To(Equal(PageID(8)))
				Expect(pages[5].PageID).To(Equal(PageID(9)))
				Expect(pages[6].PageID).To(Equal(PageID(10)))
				Expect(pages[7].PageID).To(Equal(PageID(11)))
			})
			It("errはnil", func() {
				Expect(err).To(BeNil())
			})
		})
	})
})
