package storage

import (
	"encoding/json"
)

type (
	SearchMode uint8
	NodeType   uint8

	PageID int32

	Page struct {
		PageID       PageID   `json:"page_id"`
		NodeType     NodeType `json:"node_type"`
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
// 呼び出し元でminとmaxそれぞれ呼び出した方がシンプルかも？それかPageの配列
// -> で呼び出し元でtype(const, range, all)とtargetValを元にPairを取得する？
// -> ここから

// seachModeはpageで使わないようにしよう(iterで使う)。pageではただminとmaxに則ってページIDを取得するだけ(それでALLも対応できる)
// iteratorにpageの下限上限とmodeとtargetValsetを渡して実際の値を求める(ここではindexを使うのかvalueなのかの判別がしたい)
// インサート
// キーだけじゃなく、バリューでも絞り込めるようにする(これはALLですな、これはpageを呼び出すレイヤー)
// Pageのこのメソッド呼ぶ時点ですでに対象のインデックス(ファイル)は決まっているのでindexを指定する必要はない、constかallの判別だけで大丈夫
// => constかallの判別も上のレイヤーでやるわ
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
		if pair.Key > minTargetVal {
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
