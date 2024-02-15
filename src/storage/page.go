package storage

import (
	"encoding/json"
	"errors"
	"fmt"
)

type (
	SearchMode uint8
	NodeType   uint8

	PageID int32

	Page struct {
		PageID       PageID   `json:"page_id"`
		NodeType     NodeType `json:"node_type"`
		ParentID     PageID   `json:"parent_id"`
		PrevPageID   PageID   `json:"prev_page_id"`
		NextPageID   PageID   `json:"next_page_id"`
		RightPointer PageID   `json:"right_pointer"` // leafの時は使わない
		Items        []Pair   `json:"items"`         // `items` 内ではpairはkeyの昇順で並んでいることが保証される
	}

	// leafの時valueは実際のデータ、中間ノードの時は子のページID
	Pair struct {
		Key   uint32 `json:"key"`
		Value int32  `json:"value"`
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

	InvalidPageID PageID = -1

	MinTargetValue uint32 = 0
	MaxTargetValue uint32 = 4_294_967_295
)

func NewPage(bytes [PageSize]byte) (*Page, error) {
	var pageBytes []byte
	// Pageが4KBぴったりということはないので使われていないbyteは切り捨てる
	for i, b := range bytes {
		if b == byte(0) {
			pageBytes = bytes[:i]
			break
		}
	}
	var p Page
	err := json.Unmarshal(pageBytes, &p)
	if err != nil {
		return nil, err
	}
	return &p, nil
}

// 欲を言えばsaechModeで検索対象のカラム、その値、等号なのか範囲なのかを判定させたい
// func (p *Page) SearchBy(dm DiskManager, minTargetVal uint32, maxTargetVal uint32, searchMode SearchMode) ([]Pair, error) {
// 	res := make([]Pair, 0)
// 	return p.searchBy(dm, minTargetVal, maxTargetVal, searchMode, &res)
// }

// // TODO
// // IN検索
// // インサート
// // キーだけじゃなく、バリューでも絞り込めるようにする(これはALLですな)
// // Pageのこのメソッド呼ぶ時点ですでに対象のインデックス(ファイル)は決まっているのでindexを指定する必要はない、constかallの判別だけで大丈夫
// func (p *Page) searchBy(dm DiskManager, minTargetVal uint32, maxTargetVal uint32, searchMode SearchMode, res *[]Pair) ([]Pair, error) {
// 	// leafの場合keyに合致する値を見つける
// 	if p.NodeType == NodeTypeLeaf {
// 		for _, pair := range p.Items {
// 			// 等号比較の場合は一つ合致したら返す
// 			if searchMode == SearchModeConst {
// 				if pair.Key == minTargetVal {
// 					*res = append(*res, pair)
// 					return *res, nil
// 				}
// 			} else if searchMode == SearchModeRange {
// 				if pair.Key < maxTargetVal {
// 					*res = append(*res, pair)
// 				} else {
// 					return *res, nil
// 				}
// 			}
// 		}
// 		// Page内の値が全てTargetValより小さいなら次のページも見る
// 		if p.NextPageID == InvalidPageID {
// 			return *res, nil
// 		}
// 		bytes := dm.ReadPageData(p.NextPageID)
// 		nextPage, err := NewPage(bytes)
// 		if err != nil {
// 			return nil, err
// 		}
// 		return nextPage.searchBy(dm, minTargetVal, maxTargetVal, searchMode, res)
// 	}
// 	// internal nodeの場合、対象のchildIDを探す
// 	nextPageID := InvalidPageID
// 	for _, pair := range p.Items {
// 		if pair.Key > minTargetVal {
// 			nextPageID = PageID(pair.Value)
// 			break
// 		}
// 	}
// 	if nextPageID == InvalidPageID {
// 		nextPageID = PageID(p.RightPointer)
// 	}
// 	bytes := dm.ReadPageData(nextPageID)
// 	nextPage, err := NewPage(bytes)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return nextPage.searchBy(dm, minTargetVal, maxTargetVal, searchMode, res)
// }

// DONE
// page.SearchByでは下限上限を元にpageの範囲を求める(あれ、searchModeどこで使う？->ALLだったら全ページ取る必要があるのか)

// TODO
// 呼び出し元でminとmaxそれぞれ呼び出した方がシンプルかも？それかPageの配列
// -> で呼び出し元でtype(const, range, all)とtargetValを元にPairを取得する？
// -> ここから

// seachModeはpageで使わないようにしよう(iterで使う)。pageではただminとmaxに則ってページIDを取得するだけ(それでALLも対応できる)
// iteratorにpageの下限上限とmodeとtargetValsetを渡して実際の値を求める(ここではindexを使うのかvalueなのかの判別がしたい)
// インサート
// キーだけじゃなく、バリューでも絞り込めるようにする(これはALLですな、これはpageを呼び出すレイヤー)
// Pageのこのメソッド呼ぶ時点ですでに対象のインデックス(ファイル)は決まっているのでindexを指定する必要はない、constかallの判別だけで大丈夫
// => constかallの判別も上のレイヤーでやるわ
// func (p *Page) SearchByV2(dm DiskManager, minTargetVal uint32, maxTargetVal uint32) (*PageID, *PageID, error) {
// 	return p.searchByV2(dm, minTargetVal, maxTargetVal, nil, nil)
// }

// func (p *Page) searchByV2(dm DiskManager, minTargetVal uint32, maxTargetVal uint32, minPageID *PageID, maxPageID *PageID) (*PageID, *PageID, error) {
// 	// leafの場合key >= maxTargetValになるまで続ける
// 	if p.NodeType == NodeTypeLeaf {
// 		if minPageID == nil {
// 			minPageID = &p.PageID
// 		}
// 		for _, pair := range p.Items {
// 			if pair.Key >= maxTargetVal {
// 				maxPageID = &p.PageID
// 				return minPageID, maxPageID, nil
// 			}
// 		}
// 		// 一番大きい右側のleafならここで終了
// 		if p.NextPageID == InvalidPageID {
// 			maxPageID = &p.PageID
// 			return minPageID, maxPageID, nil
// 		}
// 		// Page内の全てのKeyがmaxTargetValより小さいなら次のページも見る
// 		bytes := dm.ReadPageData(p.NextPageID)
// 		nextPage, err := NewPage(bytes)
// 		if err != nil {
// 			return nil, nil, err
// 		}
// 		return nextPage.searchByV2(dm, minTargetVal, maxTargetVal, minPageID, maxPageID)
// 	}
// 	// internal nodeの場合、対象のchildIDを探す
// 	nextPageID := InvalidPageID
// 	for _, pair := range p.Items {
// 		if pair.Key > minTargetVal {
// 			nextPageID = PageID(pair.Value)
// 			break
// 		}
// 	}
// 	if nextPageID == InvalidPageID {
// 		nextPageID = PageID(p.RightPointer)
// 	}
// 	bytes := dm.ReadPageData(nextPageID)
// 	nextPage, err := NewPage(bytes)
// 	if err != nil {
// 		return nil, nil, err
// 	}
// 	return nextPage.searchByV2(dm, minTargetVal, maxTargetVal, minPageID, maxPageID)
// }

// Pageの配列を返す

// DONE
// page.SearchByでは下限上限を元にpageの範囲を求める(あれ、searchModeどこで使う？->ALLだったら全ページ取る必要があるのか)

// TODO
// constの場合はmin,maxに同じ値。rangeはそのまま、allはminとmaxに型で上限の値を入れる
// 呼び出し元でtype(const, range, all)とtargetValを元にPairを取得する？
// -> 呼び出し元でtargetValとpairの値を比較していく

// このメソッドを呼ぶ時点ですでに対象のインデックス(ファイル)は決まっているのでindexは渡す必要はない
func (p *Page) SearchByV3(dm DiskManager, minTargetVal uint32, maxTargetVal uint32) ([]*Page, error) {
	res := make([]*Page, 0)
	return p.searchByV3(dm, minTargetVal, maxTargetVal, &res)
}

func (p *Page) searchByV3(dm DiskManager, minTargetVal uint32, maxTargetVal uint32, res *[]*Page) ([]*Page, error) {
	// leafの場合key >= maxTargetValになるまで続ける
	if p.NodeType == NodeTypeLeaf {
		*res = append(*res, p)
		for _, pair := range p.Items {
			if pair.Key >= maxTargetVal {
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
		return nextPage.searchByV3(dm, minTargetVal, maxTargetVal, res)
	}
	// internal nodeの場合、対象のchildIDを探す
	nextPageID := InvalidPageID
	for _, pair := range p.Items {
		if pair.Key >= minTargetVal {
			nextPageID = PageID(pair.Value)
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
	return nextPage.searchByV3(dm, minTargetVal, maxTargetVal, res)
}

// 1pageにつきpairは二つまで(暫定)
const maxNumberPair = 2

// 対象のページに新しくkeyvalueを追加する
// 前提として正しいページに挿入されるものとする

// TODO
// B+Treeのインサートで呼び出される
// 追々key-valueは[]byteにしたい
func (p *Page) InsertPair(dm DiskManager, key, value uint32) error {
	var hasInserted bool
	fmt.Println("")
	fmt.Printf("p: %+v before insert. key: %v, value: %v\n", p, key, value)
	for i, item := range p.Items {
		if item.Key > key {
			p.Items = append(p.Items[:i+1], p.Items[i:]...)
			p.Items[i] = Pair{key, int32(value)}
			hasInserted = true
			break
		}
	}
	if !hasInserted {
		p.Items = append(p.Items, Pair{key, int32(value)})
	}
	fmt.Printf("p: %+v after insert\n", p)
	// 元を左に置くver
	// if len(p.Items) > maxNumberPair {
	// 	// 新しいページを割り当てる
	// 	newPageID := dm.AllocatePage()
	// 	// 新しいページに右半分のペアを割り当てる
	// 	r := Page{
	// 		newPageID,
	// 		p.NodeType,
	// 		p.ParentID,
	// 		p.PageID,
	// 		p.NextPageID,
	// 		InvalidPageID,
	// 		[]Pair{},
	// 	}
	// 	// 元ページのnextPageIDを修正
	// 	// p.NextPageID = r.PageID
	// 	// 新リーフのprevPageIDを割り当ててnextPageIDに旧リーフのnextPageIDを割り当てる
	// 	r.Items = p.Items[maxNumberPair:]
	// 	p.Items = p.Items[:maxNumberPair]
	// 	// とりあえずpageが親のPageIDを参照できるようにする
	// 	// rootの場合は中間ノードにして左右に振り分ける
	// 	if p.ParentID == InvalidPageID {
	// 		l := Page{
	// 			dm.AllocatePage(),
	// 			NodeTypeLeaf,
	// 			p.PageID,
	// 			InvalidPageID,
	// 			newPageID,
	// 			InvalidPageID,
	// 			p.Items,
	// 		}
	// 		r.ParentID = p.PageID
	// 		r.PrevPageID = l.PageID
	// 		p.NodeType = NodeTypeBranch
	// 		p.PrevPageID = InvalidPageID
	// 		p.NextPageID = InvalidPageID
	// 		p.RightPointer = r.PageID
	// 		p.Items = []Pair{
	// 			{
	// 				r.Items[0].Key,
	// 				int32(l.PageID),
	// 			},
	// 		}
	// 		if err := l.Flush(dm); err != nil {
	// 			return err
	// 		}
	// 		if err := r.Flush(dm); err != nil {
	// 			return err
	// 		}
	// 	}
	// 	// 新リーフのページIDを親ノードが参照するようにする(親がいなければルートなので新しく子を作る)
	// 	// 再帰的に動く？
	// 	// https: //cstack.github.io/db_tutorial/parts/part10.html
	// 	// https: //cstack.github.io/db_tutorial/parts/part14.html
	// 	// 中間ノードの場合
	// 	// 親に新しくkeyとvalueを挿入する
	// 	// if leaf.ParentID != InvalidPageID {
	// 	// 	leaf.NextPageID = r.PageID
	// 	// 	fmt.Printf("leaf: %+v, r: %+v\n", leaf, r)
	// 	// 	bytes := dm.ReadPageData(leaf.ParentID)
	// 	// 	parentPage, err := NewPage(bytes)
	// 	// 	if err != nil {
	// 	// 		return err
	// 	// 	}
	// 	// 	// keyはr.Items[0].Keyじゃなくてleaf.NextのItems[0].Keyにする？
	// 	// 	// return parentPage.InsertPair(dm, r.Items[0].Key, uint32(r.PageID))
	// 	// 	// TODO このロジックだと対象のページをleafまで遡ってしまうから中間ノードに挿入できない
	// 	// 	// 挿入対象のペアを探す処理と実際に挿入する処理は分けないといけない
	// 	// 	return parentPage.InsertPair(dm, uint32(10), uint32(3))
	// 	// }
	// }
	// 元を右に置くver
	if len(p.Items) > maxNumberPair {
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
		l.Items = p.Items[:maxNumberPair]
		p.Items = p.Items[maxNumberPair:] // これlとpで同じの参照しているから追加したら壊れる？
		// left-siblingがいた場合nextPageIDを更新する
		if l.PrevPageID != InvalidPageID {
			// fmt.Printf("p: %+v, p.PrevPageID != InvalidPageID: %v, \n", p, p.PrevPageID != InvalidPageID)
			bytes := dm.ReadPageData(l.PrevPageID)
			prevPage, err := NewPage(bytes)
			if err != nil {
				return err
			}
			prevPage.NextPageID = l.PageID
			prevPage.Flush(dm)
		}
		// とりあえずpageが親のPageIDを参照できるようにする
		// rootの場合は中間ノードにして左右に振り分ける
		if p.ParentID == InvalidPageID {
			r := Page{
				dm.AllocatePage(),
				NodeTypeLeaf,
				p.PageID,
				l.PageID,
				InvalidPageID,
				InvalidPageID,
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
					// r.Items[0].Key, // lの最大値の方がいいかや？
					l.Items[len(l.Items)-1].Key,
					int32(l.PageID),
				},
			}
			if err := l.Flush(dm); err != nil {
				return err
			}
			if err := r.Flush(dm); err != nil {
				return err
			}
		}
		if p.ParentID != InvalidPageID {
			fmt.Printf("newLeft: %+v \n", l)
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
			// return parentPage.InsertPair(dm, p.Items[0].Key, uint32(l.PageID))
			return parentPage.InsertPair(dm, l.Items[len(l.Items)-1].Key, uint32(l.PageID))
		}
	}

	fmt.Printf("p: %+v right before flush\n", p)
	return p.Flush(dm)
}

func (p *Page) Flush(dm DiskManager) error {
	bytes, err := json.Marshal(p)
	if err != nil {
		return err
	}
	if len(bytes) > PageSize {
		return errors.New("full")
	}
	pp := [PageSize]byte{}
	for i, b := range bytes {
		pp[i] = b
	}
	dm.WritePageData(p.PageID, pp)
	return nil
}

// とりあえずデバッグ用で実装する
// 自身と子ノードを全て表示。順番は考え中
func (p *Page) PrintAll(dm DiskManager, prefix string) {
	fmt.Printf("%scurrentPage: %+v\n", prefix, p)
	if p.NodeType == NodeTypeLeaf {
		return
	}
	// internal nodeの時のみchild nodeの確認をする
	for _, i := range p.Items {
		prefix := prefix + "-"
		bytes := dm.ReadPageData(PageID(i.Value))
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
