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
		Items        []Pair   `json:"items"`
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
)

const (
	NodeTypeBranch NodeType = iota
	NodeTypeLeaf
)

const (
	PageSize = 4 * 1_024 // 4KB

	InvalidPageID PageID = -1
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
func (p *Page) SearchBy(dm DiskManager, key uint32, searchMode SearchMode) ([]Pair, error) {
	return p.searchBy(dm, key, searchMode)
}

// TODO
// 範囲検索
// IN検索
// インサート
// キーだけじゃなく、バリューでも絞り込めるようにする
func (p *Page) searchBy(dm DiskManager, key uint32, searchMode SearchMode) ([]Pair, error) {
	// leafの場合keyに合致する値を見つける
	if p.NodeType == NodeTypeLeaf {
		var res []Pair
		for _, pair := range p.Items {
			if pair.Key == key {
				res = append(res, pair)
			}
		}
		return res, nil
	}
	// internal nodeの場合、対象のchildIDを探す
	nextPageID := InvalidPageID
	for _, pair := range p.Items {
		if pair.Key > key {
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
	return nextPage.searchBy(dm, key, searchMode)
}
