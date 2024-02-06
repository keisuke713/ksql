package storage

import (
	"os"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("DiskManagerのテスト", func() {
	var (
		dm DiskManager
	)
	Describe("ReadPageData", func() {
		var (
			res [PageSize]byte
		)
		const (
			fPath = "test_table"
		)
		Context("書き込まれている場合", func() {
			BeforeEach(func() {
				f, _ := os.Create(fPath)
				dm = NewDiskManager(f)

				var data [PageSize]byte
				nextPageID := dm.AllocatePage()
				dm.WritePageData(nextPageID, data)

				nextPageID = dm.AllocatePage()
				data[0] = 1
				data[1] = 2

				dm.WritePageData(nextPageID, data)
				res = dm.ReadPageData(PageID(1))
			})
			It("正しいバイトが取得される", func() {
				Expect(res[0]).To(Equal(byte(1)))
				Expect(res[1]).To(Equal(byte(2)))
			})
		})
	})
})
