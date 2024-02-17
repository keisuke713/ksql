package storage

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Bytesのテスト", func() {
	Describe("Compare", func() {
		var (
			self  Bytes
			other Bytes

			len uint32

			res ComparisonResult
		)
		JustBeforeEach(func() {
			res = self.Compare(other, len)
		})
		Context("lenが4byteの倍数の時", func() {
			Context("1カラム分で比較", func() {
				BeforeEach(func() {
					self = []byte{255, 255, 0, 0} // 65535
					len = ColumnSize
				})
				Context("等しい", func() {
					BeforeEach(func() {
						other = []byte{255, 255, 0, 0}
					})
					It("ComparisonResultEqualが返る", func() {
						Expect(res).To(Equal(ComparisonResultEqual))
					})
				})
				Context("selfが小さい", func() {
					BeforeEach(func() {
						other = []byte{0, 0, 1, 0} // 65536
					})
					It("ComparisonResultSmallが返る", func() {
						Expect(res).To(Equal(ComparisonResultSmall))
					})
				})
				Context("selfが大きい", func() {
					BeforeEach(func() {
						other = []byte{255, 254, 0, 0}
					})
					It("ComparisonResultBigが返る", func() {
						Expect(res).To(Equal(ComparisonResultBig))
					})
				})
			})
			Context("2カラム分で比較", func() {
				BeforeEach(func() {
					self = []byte{255, 255, 0, 0, 255, 255, 0, 0} // 65535, 65535
					len = ColumnSize * 2
				})
				Context("1カラムは等しい", func() {
					BeforeEach(func() {
						other = []byte{255, 255, 0, 0}
					})
					Context("2カラム目も等しい", func() {
						BeforeEach(func() {
							other = append(other, 255, 255, 0, 0)
						})
						It("ComparisonResultEqualが返る", func() {
							Expect(res).To(Equal(ComparisonResultEqual))
						})
					})
					Context("2カラム目はselfが大きい", func() {
						BeforeEach(func() {
							other = append(other, 255, 254, 0, 0)
						})
						It("ComparisonResultBigが返る", func() {
							Expect(res).To(Equal(ComparisonResultBig))
						})
					})
				})
				Context("1カラム目でselfが小さい", func() {
					BeforeEach(func() {
						self = []byte{255, 255, 0, 0, 0, 0, 1, 0}  // 65535, 65536
						other = []byte{0, 0, 1, 0, 255, 255, 0, 0} // 65536, 65535
						len = ColumnSize * 2
					})
					It("ComparisonResultSmallが返る", func() {
						Expect(res).To(Equal(ComparisonResultSmall))
					})
				})
			})
		})
		Context("lenが4byteの倍数でない時", func() {
			BeforeEach(func() {
				len = ColumnSize + 1
			})
			It("ComparisonResultUnKnownが返る", func() {
				Expect(res).To(Equal(ComparisonResultUnKnown))
			})
		})
		Context("lenがキーの長さをオーバーしている時", func() {
			BeforeEach(func() {
				self = []byte{255, 255, 0, 0} // 65535
				other = []byte{0, 0, 1, 0}    // 65536
				len = ColumnSize * 2
			})
			It("ComparisonResultUnKnownが返る", func() {
				Expect(res).To(Equal(ComparisonResultUnKnown))
			})
		})
	})
})
