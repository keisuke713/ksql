package storage

import (
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
)

type (
	SearchMode uint8
	NodeType   uint8

	PageID uint32

	Page struct {
		PageID       PageID
		NodeType     NodeType
		ParentID     PageID
		PrevPageID   PageID
		NextPageID   PageID
		RightPointer PageID // leafの時は使わない
		Items        []Pair // `items` 内ではpairはkeyの昇順で並んでいることが保証される
	}

	// leafの時valueは実際のデータ、中間ノードの時は子のページID
	Pair struct {
		Key   Bytes
		Value Bytes
	}
)

const (
	SearchModeConst SearchMode = iota
	SearchModeRange
	SearchModeAll
)

const (
	NodeTypeBranch NodeType = iota
	NodeTypeLeaf
)

const (
	PageSize = 4 * 1_024 // 4KB

	InvalidPageID PageID = 0 // metadataに使うので中間・リーフとしては使われないID
	RootPageID    PageID = 1

	MinTargetValue uint32 = 0
	MaxTargetValue uint32 = 4_294_967_295
)

const (
	NodeTypeOffset     = 4
	ParentIDOffset     = 8
	PrevPageIDOffset   = 12
	NextPageIDOffset   = 16
	RightPointerOffset = 20

	HeaderNByte = RightPointerOffset + 4

	// キーのオフセット、長さとバリューの長さをそれぞれ何バイトで保存しているか
	KeyOffsetNByte = 4
	KeyLenNByte    = 4
	ValueLenNByte  = 4
)

const (
	MaxNByte = 4 * 1024 // 4KB
)

const (
	BytesSizeLimitKey = "BYTES_SIZE_LIMIT"
)

func LimitBytesSize() uint32 {
	if size, ok := os.LookupEnv(BytesSizeLimitKey); ok {
		if sizei, err := strconv.Atoi(size); err == nil {
			return uint32(sizei)
		}
	}
	return PageSize
}

func NewPage(b [PageSize]byte) (*Page, error) {
	p := &Page{}
	p.PageID = PageID(binary.NativeEndian.Uint32(b[:4]))
	p.NodeType = NodeType(binary.NativeEndian.Uint32(b[NodeTypeOffset : NodeTypeOffset+4]))
	p.ParentID = PageID(binary.NativeEndian.Uint32(b[ParentIDOffset : ParentIDOffset+4]))
	p.PrevPageID = PageID(binary.NativeEndian.Uint32(b[PrevPageIDOffset : PrevPageIDOffset+4]))
	p.NextPageID = PageID(binary.NativeEndian.Uint32(b[NextPageIDOffset : NextPageIDOffset+4]))
	p.RightPointer = PageID(binary.NativeEndian.Uint32(b[RightPointerOffset : RightPointerOffset+4]))

	var (
		start uint32 = HeaderNByte
	)
	for {
		// キーが始まるバイト数
		offset := binary.NativeEndian.Uint32(b[start : start+4])
		start += 4

		// キーの長さ
		keyLen := binary.NativeEndian.Uint32(b[start : start+4])
		start += 4
		// バリューの長さ
		valueLen := binary.NativeEndian.Uint32(b[start : start+4])
		start += 4

		if start >= offset {
			break
		}

		// キーの値
		key := b[offset : offset+keyLen]
		// バリューの値
		value := b[offset+keyLen : offset+keyLen+valueLen]
		p.Items = append(p.Items, Pair{key, value})
	}
	return p, nil
}

// Pageの配列を返す

// DONE
// page.SearchByでは下限上限を元にpageの範囲を求める(あれ、searchModeどこで使う？->ALLだったら全ページ取る必要があるのか)

// TODO
// constの場合はmin,maxに同じ値。rangeはそのまま、allはminとmaxに型で上限の値を入れる
// 呼び出し元でtype(const, range, all)とtargetValを元にPairを取得する？
// -> 呼び出し元でtargetValとpairの値を比較していく

// このメソッドを呼ぶ時点ですでに対象のインデックス(ファイル)は決まっているのでindexは渡す必要はない
func (p *Page) SearchByV3(dm DiskManager, minTargetVal Bytes, maxTargetVal Bytes, len uint32) ([]*Page, error) {
	res := make([]*Page, 0)
	return p.searchByV3(dm, minTargetVal, maxTargetVal, &res, len)
}

func (p *Page) searchByV3(dm DiskManager, minTargetVal Bytes, maxTargetVal Bytes, res *[]*Page, len uint32) ([]*Page, error) {
	// leafの場合key >= maxTargetValになるまで続ける
	if p.NodeType == NodeTypeLeaf {
		*res = append(*res, p)
		for _, pair := range p.Items {
			if pair.Key.Compare(maxTargetVal, len) != ComparisonResultSmall {
				return *res, nil
			}
		}
		// 一番大きい右側のleafならここで終了
		if p.NextPageID == InvalidPageID {
			return *res, nil
		}
		// Page内の全てのKeyがmaxTargetValより小さいなら次のページも見る
		bytes := dm.ReadPageData(p.NextPageID)
		nextPage, err := NewPage(bytes)
		if err != nil {
			return nil, err
		}
		return nextPage.searchByV3(dm, minTargetVal, maxTargetVal, res, len)
	}
	// internal nodeの場合、対象のchildIDを探す
	nextPageID := InvalidPageID
	for _, pair := range p.Items {
		if pair.Key.Compare(minTargetVal, len) != ComparisonResultSmall {
			nextPageID = PageID(pair.Value.Uint32(0))
			break
		}
	}
	if nextPageID == InvalidPageID {
		nextPageID = PageID(p.RightPointer)
	}
	bytes := dm.ReadPageData(nextPageID)
	nextPage, err := NewPage(bytes)
	if err != nil {
		return nil, err
	}
	return nextPage.searchByV3(dm, minTargetVal, maxTargetVal, res, len)
}

// 対象のページに新しくkey-valueを追加する
// 前提として正しいページに挿入されるものとする
func (p *Page) InsertPair(dm DiskManager, key, value Bytes) error {
	var hasInserted bool
	for i, item := range p.Items {
		if item.Key.Compare(key, ColumnSize*2) == ComparisonResultBig {
			p.Items = append(p.Items[:i+1], p.Items[i:]...)
			p.Items[i] = Pair{key, value}
			hasInserted = true
			break
		}
	}
	if !hasInserted {
		p.Items = append(p.Items, Pair{key, value})
	}
	if p.NBytes() > LimitBytesSize() {
		// 新しいページを割り当てる
		newPageID := dm.AllocatePage()
		// 新しいページに左半分を割り当てる
		l := Page{
			newPageID,
			p.NodeType,
			p.ParentID,
			p.PrevPageID,
			p.PageID,
			InvalidPageID,
			[]Pair{},
		}
		// 元のページのprevを修正
		p.PrevPageID = newPageID
		itemLen := len(p.Items)
		l.Items = p.Items[:itemLen/2+1]
		p.Items = p.Items[itemLen/2+1:]
		// left-siblingがいた場合nextPageIDを更新する
		if l.PrevPageID != InvalidPageID {
			bytes := dm.ReadPageData(l.PrevPageID)
			prevPage, err := NewPage(bytes)
			if err != nil {
				return err
			}
			prevPage.NextPageID = l.PageID
			prevPage.Flush(dm)
		}
		l.LinkToChild(dm)
		// 子が親のPageIDを参照できるようにする
		// rootの場合は中間ノードにして左右に振り分ける
		if p.ParentID == InvalidPageID {
			r := Page{
				dm.AllocatePage(),
				p.NodeType,
				p.PageID,
				l.PageID,
				InvalidPageID,
				p.RightPointer,
				p.Items,
			}
			l.ParentID = p.PageID
			l.NextPageID = r.PageID
			p.NodeType = NodeTypeBranch
			p.PrevPageID = InvalidPageID
			p.NextPageID = InvalidPageID
			p.RightPointer = r.PageID
			p.Items = []Pair{
				{
					l.Items[len(l.Items)-1].Key,
					NewBytes(uint32(l.PageID)),
				},
			}
			if err := l.Flush(dm); err != nil {
				return err
			}
			if err := r.Flush(dm); err != nil {
				return err
			}
			r.LinkToChild(dm)
		}
		if p.ParentID != InvalidPageID {
			bytes := dm.ReadPageData(p.ParentID)
			parentPage, err := NewPage(bytes)
			if err != nil {
				return err
			}
			if err := p.Flush(dm); err != nil {
				return err
			}
			if err := l.Flush(dm); err != nil {
				return err
			}
			return parentPage.InsertPair(dm, l.Items[len(l.Items)-1].Key, NewBytes(uint32(l.PageID)))
		}
	}

	return p.Flush(dm)
}

// ノードの分割などで親と子の結びつきに変更があった際に呼び出す
func (p *Page) LinkToChild(dm DiskManager) error {
	// leafは子ノードを持たないのでreturn
	if p.NodeType == NodeTypeLeaf {
		return nil
	}

	for _, item := range p.Items {
		bytes := dm.ReadPageData(PageID(item.Value.Uint32(0)))
		child, err := NewPage(bytes)
		if err != nil {
			return err
		}
		child.ParentID = p.PageID
		if err := child.Flush(dm); err != nil {
			return err
		}
	}
	if p.RightPointer != InvalidPageID {
		bytes := dm.ReadPageData(p.RightPointer)
		child, err := NewPage(bytes)
		if err != nil {
			return err
		}
		child.ParentID = p.PageID
		if err := child.Flush(dm); err != nil {
			return err
		}
	}
	return nil
}

func (p *Page) Flush(dm DiskManager) error {
	dm.WritePageData(p.PageID, p.Bytes())
	return nil
}

func (p *Page) Bytes() [PageSize]byte {
	var b [PageSize]byte
	binary.NativeEndian.PutUint32(b[:4], uint32(p.PageID))
	binary.NativeEndian.PutUint32(b[NodeTypeOffset:NodeTypeOffset+4], uint32(p.NodeType))
	binary.NativeEndian.PutUint32(b[ParentIDOffset:ParentIDOffset+4], uint32(p.ParentID))
	binary.NativeEndian.PutUint32(b[PrevPageIDOffset:PrevPageIDOffset+4], uint32(p.PrevPageID))
	binary.NativeEndian.PutUint32(b[NextPageIDOffset:NextPageIDOffset+4], uint32(p.NextPageID))
	binary.NativeEndian.PutUint32(b[RightPointerOffset:RightPointerOffset+4], uint32(p.RightPointer))

	var start uint32 = HeaderNByte // 24バイト目までは固定のヘッダー
	var tail uint32 = PageSize
	for _, item := range p.Items {
		// キーが何バイト目から始まるか
		itemLen := item.Key.Len() + item.Value.Len()
		binary.NativeEndian.PutUint32(b[start:start+4], tail-itemLen)
		start += 4
		// キーの長さ
		binary.NativeEndian.PutUint32(b[start:start+4], item.Key.Len())
		start += 4
		// バリューの長さ
		binary.NativeEndian.PutUint32(b[start:start+4], item.Value.Len())
		start += 4

		// バリューの値を入れる
		index := 0
		for i := tail - item.Value.Len(); i < tail; i++ {
			b[i] = item.Value[index]
			index += 1
		}
		tail -= item.Value.Len()
		// キーの値を入れる
		index = 0
		for i := tail - item.Key.Len(); i < tail; i++ {
			b[i] = item.Key[index]
			index += 1
		}
		tail -= item.Key.Len()
	}
	return b
}

// 現在ページ内で使われているバイト数を返す
func (p *Page) NBytes() uint32 {
	var totalBytes uint32
	totalBytes = HeaderNByte
	for _, i := range p.Items {
		totalBytes += KeyOffsetNByte
		totalBytes += KeyLenNByte
		totalBytes += ValueLenNByte
		totalBytes += i.Key.Len()
		totalBytes += i.Value.Len()
	}
	return uint32(totalBytes)
}

// とりあえずデバッグ用で実装する
// 自身と子ノードを全て表示。in-order
func (p *Page) PrintAll(dm DiskManager, prefix string) {
	fmt.Printf("%s page: %+v \n", prefix, p)
	if p.NodeType == NodeTypeLeaf {
		return
	}
	// internal nodeの時のみchild nodeの確認をする
	for _, i := range p.Items {
		prefix := prefix + "-"
		bytes := dm.ReadPageData(PageID(i.Value.Uint32(0)))
		nextPage, err := NewPage(bytes)
		if err != nil {
			panic(err)
		}
		nextPage.PrintAll(dm, prefix)
	}
	if p.RightPointer != InvalidPageID {
		newprefix := prefix + "-"
		bytes := dm.ReadPageData(PageID(p.RightPointer))
		nextPage, err := NewPage(bytes)
		if err != nil {
			panic(err)
		}
		nextPage.PrintAll(dm, newprefix)
	}
}

func (p *Page) Walk(dm DiskManager, ps *[]Page) {
	*ps = append(*ps, *p)
	if p.NodeType == NodeTypeLeaf {
		return
	}
	// internal nodeの時のみchild nodeの確認をする
	for _, i := range p.Items {
		bytes := dm.ReadPageData(PageID(i.Value.Uint32(0)))
		nextPage, err := NewPage(bytes)
		if err != nil {
			panic(err)
		}
		nextPage.Walk(dm, ps)
	}
	if p.RightPointer != InvalidPageID {
		bytes := dm.ReadPageData(PageID(p.RightPointer))
		nextPage, err := NewPage(bytes)
		if err != nil {
			panic(err)
		}
		nextPage.Walk(dm, ps)
	}
}
