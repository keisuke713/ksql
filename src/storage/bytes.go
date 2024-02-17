package storage

import (
	"encoding/binary"
)

type (
	//  keyまたはvalueで使われる、カラムをエンコードしたbyte列
	Bytes            []byte
	ComparisonResult int8
)

const (
	ColumnSize uint32 = 4 // 全てのカラムを4Byte(uint32)に統一
)

const (
	ComparisonResultEqual   = ComparisonResult(0)
	ComparisonResultSmall   = ComparisonResult(-1)
	ComparisonResultBig     = ComparisonResult(1)
	ComparisonResultUnKnown = ComparisonResult(-2)
)

// 先頭lenBytesを4byteずつ比較して等しいなら0,selfが小さいなら-1,othersが大きいなら1を返す
// keyLengthはColumnSizeの倍数でなければならない
func (b Bytes) Compare(others Bytes, keyLength uint32) ComparisonResult {
	if keyLength%ColumnSize != 0 {
		return ComparisonResultUnKnown
	}
	if len(b) < int(keyLength) || len(others) < int(keyLength) {
		return ComparisonResultUnKnown
	}
	return compare(b[:keyLength], others[:keyLength])
}

func compare(self []byte, other []byte) ComparisonResult {
	for i := 0; i < len(self); i += int(ColumnSize) {
		upper := i + int(ColumnSize)
		b1 := binary.NativeEndian.Uint32(self[i:upper])
		b2 := binary.NativeEndian.Uint32(other[i:upper])
		if b1 == b2 {
			continue
		} else if b1 > b2 {
			return ComparisonResultBig
		} else {
			return ComparisonResultSmall
		}
	}
	return ComparisonResultEqual
}

// 4バイトずつbytes.Compareで比較する？
// 多分NativeENdianでエンコードすれば正しい結果が得られるはず
// https://pkg.go.dev/bytes#Compare
// arr1 := make([]uint32, 0, 4)
// arr1 = append(arr1, 1, 65536, 2, 3)
// buf1 := &bytes.Buffer{}
// binary.Write(buf1, binary.NativeEndian, arr1)
// fmt.Println("nativeEndian: ", buf1.Bytes())

// arr2 := make([]uint32, 0, 4)
// arr2 = append(arr2, 1, 65535, 2, 3)
// buf2 := &bytes.Buffer{}
// binary.Write(buf2, binary.NativeEndian, arr2)
// fmt.Println("nativeEndian: ", buf2.Bytes())

// binary.NativeEndian.Uint32(bytes[:4])でuint32に戻せるそこから比較だな
// Compareは配列の要素1つずつ見ていくから使えない
