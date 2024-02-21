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

func NewBytes(val ...uint32) Bytes {
	b := make([]byte, 4*len(val))
	for i, n := range val {
		binary.NativeEndian.PutUint32(b[i*4:(i+1)*4], n)
	}
	return b
}

// TODO []uint32を返すようにする
func (b Bytes) Uint32(start uint32) uint32 {
	return binary.NativeEndian.Uint32(b[start : start+4])
}

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

func (b Bytes) Len() uint32 {
	return uint32(len(b))
}
