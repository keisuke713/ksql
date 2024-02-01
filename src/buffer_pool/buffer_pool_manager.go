package bufferpool

import (
	"errors"
	"fmt"
	"ksql/src/disk"
	"time"
)

type (
	BufferID uint64 // BufferPool.buffersのインデックスに使われる
	Page     [disk.PageSize]byte

	Buffer struct {
		pageID  disk.PageID
		page    Page
		isDirty bool
	}

	Frame struct {
		buffer         Buffer
		referencedTime time.Time // 最後に参照された時間
	}

	BufferPool struct {
		buffers []Frame
		cap     uint8 // buffersは際限なく増えるのでcapを決める
	}

	BufferPoolManager interface {
		FetchPage(pageID disk.PageID) (*Buffer, error)
		CreatePage() (*Buffer, error)
		Flush() error
	}

	BufferPoolManagerImpl struct {
		disk      disk.DiskManager
		pool      BufferPool
		pageTable map[disk.PageID]BufferID
	}
)

// BufferPool
func NewBufferPool(cap uint8) *BufferPool {
	return &BufferPool{
		make([]Frame, 0),
		cap,
	}
}

// 参照日時が一番古いbufferを削除対象とする
// LRUキャッシュは本来ダブル連結リストとハッシュを使った複雑なアルゴリズムだが本実装はO(n)を許容するため下記のようにした
func (bp *BufferPool) Evict() (*BufferID, error) {
	// キャパに余裕がある場合はバッファは削除しない
	if bp.HasRoom() {
		return nil, nil
	}

	var (
		refTime  time.Time = time.Now()
		victimID BufferID
		hasFound bool = false
	)
	for i, f := range bp.buffers {
		if f.referencedTime.Before(refTime) {
			refTime = f.referencedTime
			victimID = BufferID(i)
			hasFound = hasFound || true
		}
	}
	if !hasFound {
		return nil, errors.New("faied to find victim id")
	}
	return &victimID, nil
}

func (bp *BufferPool) HasRoom() bool {
	return len(bp.buffers) < int(bp.cap)
}

func (bp *BufferPool) AddFrame(pageID disk.PageID, data Page) (BufferID, error) {
	if bp.HasRoom() {
		frame := Frame{
			buffer: Buffer{
				pageID:  pageID,
				page:    data,
				isDirty: false,
			},
			referencedTime: time.Now(),
		}
		bp.buffers = append(bp.buffers, frame)
		return BufferID(len(bp.buffers) - 1), nil
	}
	return BufferID(0), errors.New("no room for new frame")
}

// BufferPoolManager
// 内部でpageTable作ろうとすると is not a type でる
// pageTable渡したらどうなる
// func NewBufferPoolManager(disk disk.DiskManager, pool BufferPool) BufferPoolManager {
// 	var pageTable map[disk.PageID]BufferID // disk.PageID is not a type
// 	return &BufferPoolManagerImpl{
// 		disk,
// 		pool,
// 		pageTable,
// 	}
// }

// FetchPageとCreatePageが似たような処理になっている
// バッファに新しくスペースを確保する処理を書く？
// というかCreatePageいる？？
func (bpm *BufferPoolManagerImpl) FetchPage(pageID disk.PageID) (*Buffer, error) {
	if bufferID, ok := bpm.pageTable[pageID]; ok {
		frame := bpm.pool.buffers[bufferID]
		frame.referencedTime = time.Now()
		return &frame.buffer, nil
	}

	var bufferID *BufferID
	bufferID, err := bpm.pool.Evict()
	if err != nil {
		return nil, err
	}

	// 削除対象のバッファがなかったら新しく発番して、対象がある場合はディスクに書き込んでバッファから削除する
	page := Page(bpm.disk.ReadPageData(pageID))
	if bufferID == nil {
		newBufferID, err := bpm.pool.AddFrame(pageID, page)
		if err != nil {
			return nil, err
		}
		bufferID = &newBufferID
	} else {
		bpm.EvictPage(*bufferID)
		// 既存のframeを上書きするためポインタで取得する
		frame := &bpm.pool.buffers[*bufferID]
		frame.buffer.pageID = pageID
		frame.buffer.isDirty = false
		frame.buffer.page = page
		frame.referencedTime = time.Now()
	}

	bpm.pageTable[pageID] = *bufferID
	return &bpm.pool.buffers[*bufferID].buffer, nil
}

func (bpm *BufferPoolManagerImpl) CreatePage() (*Buffer, error) {
	var bufferID *BufferID
	bufferID, err := bpm.pool.Evict()
	if err != nil {
		return nil, err
	}

	// 削除対象があったらそれを置き換える
	if bufferID != nil {
		bpm.EvictPage(*bufferID)
		frame := &bpm.pool.buffers[*bufferID]
		frame.buffer = Buffer{
			pageID: bpm.disk.AllocatePage(),
		}
		frame.referencedTime = time.Now()
	} else {
		newBufferID := BufferID(len(bpm.pool.buffers))
		bufferID = &newBufferID
	}
	frame := Frame{
		buffer: Buffer{
			pageID: bpm.disk.AllocatePage(),
		},
		referencedTime: time.Now(),
	}
	bpm.pageTable[frame.buffer.pageID] = *bufferID
	return &frame.buffer, nil
}

// ディスクに書き込んでバッファから削除する？
// 一旦後回し
func (bpm *BufferPoolManagerImpl) Flush() error {
	return fmt.Errorf("")
}

func (bpm *BufferPoolManagerImpl) EvictPage(bufferID BufferID) {
	frame := bpm.pool.buffers[bufferID]
	evictPageID := frame.buffer.pageID

	if frame.buffer.isDirty {
		bpm.disk.WritePageData(evictPageID, frame.buffer.page[:])
	}

	delete(bpm.pageTable, evictPageID)
}
