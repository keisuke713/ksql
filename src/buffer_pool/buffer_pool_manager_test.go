package bufferpool

import (
	"fmt"
	"ksql/src/disk"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("BufferPoolのテスト", func() {
	Describe("Evictのテスト", func() {
		var (
			bp BufferPool

			buffers []Frame
			cap     uint8

			bufferIDp *BufferID
		)
		JustBeforeEach(func() {
			bp = BufferPool{
				buffers,
				cap,
			}
			bufferIDp, _ = bp.Evict()
		})
		Context("buffersが空の場合", func() {
			BeforeEach(func() {
				buffers = []Frame{}
				cap = 1
			})
			It("bufferIDはnil", func() {
				Expect(bufferIDp).To(BeNil())
			})
		})
		Context("bufferが一杯の場合場合", func() {
			BeforeEach(func() {
				buffers = []Frame{
					{
						referencedTime: time.Date(2024, time.January, 31, 23, 10, 10, 1, time.Local),
					},
					{
						referencedTime: time.Date(2024, time.January, 30, 23, 10, 10, 1, time.Local),
					},
					{
						referencedTime: time.Date(2024, time.January, 31, 13, 10, 10, 1, time.Local),
					},
				}
				cap = 3
			})
			It("2つ目のbufferが返される", func() {
				Expect(*bufferIDp).To(Equal(BufferID(1)))
			})
		})
	})
})

var _ = Describe("BufferPoolManagerのテスト", func() {
	Describe("FetchPageのテスト", func() {
		var (
			bpm *BufferPoolManagerImpl

			pool      BufferPool
			pageTable map[disk.PageID]BufferID

			pageID disk.PageID

			buffer *Buffer
			err    error
		)
		JustBeforeEach(func() {
			bpm = &BufferPoolManagerImpl{
				disk.MockDiskManagerImpl{},
				pool,
				pageTable,
			}
			buffer, err = bpm.FetchPage(pageID)
		})
		Context("取得対象がすでにbufferにある場合", func() {
			BeforeEach(func() {
				pool = BufferPool{
					[]Frame{
						{
							buffer: Buffer{
								disk.PageID(4),
								[disk.PageSize]byte{2},
								false,
							},
						},
						{
							buffer: Buffer{
								disk.PageID(1),
								[disk.PageSize]byte{1},
								false,
							},
						},
					},
					2,
				}
				pageTable = map[disk.PageID]BufferID{
					disk.PageID(4): BufferID(0),
					disk.PageID(1): BufferID(1),
				}
				pageID = disk.PageID(1)
			})
			It("pageID1のbufferが取得される", func() {
				Expect(buffer.pageID).To(Equal(pageID))
				Expect(buffer.page[0]).To(Equal(byte(1)))
			})
			It("errはnil", func() {
				Expect(err).To(BeNil())
			})
		})
		Context("取得対象がbufferに存在しない場合", func() {
			Context("キャパに余裕がある場合", func() {
				BeforeEach(func() {
					pool = BufferPool{
						[]Frame{
							{
								buffer: Buffer{
									disk.PageID(4),
									[disk.PageSize]byte{2},
									false,
								},
							},
							{
								buffer: Buffer{
									disk.PageID(1),
									[disk.PageSize]byte{3},
									false,
								},
							},
						},
						3,
					}
					pageTable = map[disk.PageID]BufferID{
						disk.PageID(4): BufferID(0),
						disk.PageID(1): BufferID(1),
					}
					pageID = disk.PageID(3)
				})
				It("pageID3のbufferが取得される", func() {
					Expect(buffer.pageID).To(Equal(pageID))
					Expect(buffer.page[0]).To(Equal(byte(1)))
				})
				It("pageIDのbufferが作成される", func() {
					Expect(bpm.pageTable[disk.PageID(0)]).To(Equal(BufferID(0)))
				})
				It("errはnil", func() {
					Expect(err).To(BeNil())
				})
			})
			Context("キャパに余裕がない場合", func() {
				BeforeEach(func() {
					pool = BufferPool{
						[]Frame{
							{
								buffer: Buffer{
									disk.PageID(4),
									[disk.PageSize]byte{2},
									false,
								},
								referencedTime: time.Date(2024, time.January, 31, 0, 0, 0, 0, time.Local),
							},
							{
								buffer: Buffer{
									disk.PageID(1),
									[disk.PageSize]byte{2},
									false,
								},
								referencedTime: time.Date(2024, time.January, 30, 0, 0, 0, 0, time.Local),
							},
						},
						2,
					}
					pageTable = map[disk.PageID]BufferID{
						disk.PageID(4): BufferID(0),
						disk.PageID(1): BufferID(1),
					}
					pageID = disk.PageID(0)
				})
				It("pageID0のbufferが取得される", func() {
					Expect(buffer.pageID).To(Equal(pageID))
					Expect(buffer.page[0]).To(Equal(byte(1)))
				})
				It("page1のバッファが削除されて、代わりにpageID0のバッファが作成される", func() {
					_, ok := bpm.pageTable[disk.PageID(1)]
					Expect(ok).To(BeFalse())
					Expect(bpm.pageTable[pageID]).To(Equal(BufferID(1)))
				})
				It("errはnil", func() {
					Expect(err).To(BeNil())
				})
			})
		})
	})
	Describe("CreatePageのテスト", func() {
		var (
			bpm *BufferPoolManagerImpl

			pool      BufferPool
			pageTable map[disk.PageID]BufferID

			buffer *Buffer
			err    error
		)
		JustBeforeEach(func() {
			bpm = &BufferPoolManagerImpl{
				disk.MockDiskManagerImpl{},
				pool,
				pageTable,
			}
			buffer, err = bpm.CreatePage()
		})
		Context("キャパが一杯の場合", func() {
			BeforeEach(func() {
				pool = BufferPool{
					[]Frame{
						{
							buffer: Buffer{
								pageID: disk.PageID(2),
							},
							referencedTime: time.Date(2024, time.February, 1, 0, 0, 0, 0, time.Local),
						},
						{
							buffer: Buffer{
								pageID: disk.PageID(1),
							},
							referencedTime: time.Date(2024, time.February, 1, 1, 0, 0, 0, time.Local),
						},
					},
					2,
				}
				pageTable = map[disk.PageID]BufferID{
					disk.PageID(2): BufferID(0),
					disk.PageID(1): BufferID(1),
				}
			})
			It("pageTableからpageID:0に対するバッファがなくなっている", func() {
				fmt.Println("bpm.pageTable: ", bpm.pageTable)
				_, ok := bpm.pageTable[disk.PageID(0)]
				Expect(ok).To(BeFalse())
			})
			It("pageID: 3に対するバッファが新しく作られている", func() {
				Expect(bpm.pageTable[disk.PageID(3)]).To(Equal(BufferID(0)))
			})
			It("errはnil", func() {
				Expect(err).To(BeNil())
			})
		})
		Context("キャパに余裕がある場合", func() {
			BeforeEach(func() {
				pool = BufferPool{
					[]Frame{
						{
							buffer: Buffer{
								pageID: disk.PageID(2),
							},
							referencedTime: time.Date(2024, time.February, 1, 0, 0, 0, 0, time.Local),
						},
						{
							buffer: Buffer{
								pageID: disk.PageID(1),
							},
							referencedTime: time.Date(2024, time.February, 1, 1, 0, 0, 0, time.Local),
						},
					},
					3,
				}
				pageTable = map[disk.PageID]BufferID{
					disk.PageID(2): BufferID(0),
					disk.PageID(1): BufferID(1),
				}
			})
			It("pageID: 1に対するバッファが残っている", func() {
				Expect(bpm.pageTable[disk.PageID(1)]).To(Equal(BufferID(1)))
			})
			It("pageID: 3に対するバッファが新しく作成される", func() {
				Expect(bpm.pageTable[disk.PageID(3)]).To(Equal(BufferID(2)))
				Expect(buffer.pageID).To(Equal(disk.PageID(3)))
			})
			It("errはnil", func() {
				Expect(err).To(BeNil())
			})
		})
	})
})
