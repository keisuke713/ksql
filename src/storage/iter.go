package storage

import (
	"errors"
)

type (
	Iter struct {
		dm        DiskManager
		minPageID *PageID
		maxPageID *PageID
		// nextPage  *Page
		nextPageID *PageID
	}
)

func NewIter(dm DiskManager, minPageID *PageID, maxPageID *PageID) (*Iter, error) {
	if minPageID == nil || maxPageID == nil {
		return nil, errors.New("must specify both minPageID and maxPageID")
	}

	return &Iter{
		dm,
		minPageID,
		maxPageID,
		minPageID,
	}, nil
}

func (i *Iter) Next() (*Page, error) {
	currPageID := i.nextPageID
	if currPageID == nil {
		return nil, nil
	}
	bytes := i.dm.ReadPageData(*currPageID)
	currPage, err := NewPage(bytes)
	if err != nil {
		return nil, err
	}

	if i.maxPageID != nil && currPage.PageID != *i.maxPageID {
		i.nextPageID = &currPage.NextPageID
	} else {
		i.nextPageID = nil
	}

	return currPage, nil
}
