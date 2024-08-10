package bitcask

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/google/btree"
)

type RecordPos struct {
	Key   []byte
	Value *Pos
}

func (r *RecordPos) Less(than btree.Item) bool {
	return bytes.Compare(r.Key, than.(*RecordPos).Key) < 0
}

// 结构体
type MemTable struct {
	data *btree.BTree //树
	mu   sync.RWMutex //锁
	size int          //容量
}

// 产生新的memtable
func NewMemTable() *MemTable {
	return &MemTable{
		data: btree.New(9),
		size: 0,
	}
}

// set
func (t *MemTable) Set(r *RecordPos) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.data.ReplaceOrInsert(r)

}

// 查询
func (t *MemTable) Query(key []byte) *RecordPos {
	t.mu.RLock()
	defer t.mu.RUnlock()

	item := &RecordPos{Key: key}
	found := t.data.Get(item)
	if found == nil {
		return nil
	}
	return found.(*RecordPos)
}
func (t *MemTable) Delete(key []byte) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.data.Delete(&RecordPos{
		Key: key,
	})
}

// 获取容量
func (t *MemTable) Len() int {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.size
}

// 得到所有的RecordPoss
func (t *MemTable) GetRecordPoss() []*RecordPos {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var RecordPoss []*RecordPos
	t.data.Ascend(func(item btree.Item) bool {
		v := item.(*RecordPos)
		RecordPoss = append(RecordPoss, v)
		return true
	})
	return RecordPoss
}

// 测试使用
func (t *MemTable) Show() {
	t.mu.RLock()
	defer t.mu.RUnlock()

	fmt.Println("memory table info!")
	t.data.Ascend(func(item btree.Item) bool {
		fmt.Printf("(%s:%v)\n", item.(*RecordPos).Key, item.(*RecordPos).Value)
		return true
	})
}

// 按理来说需要进行加锁访问的
func (t *MemTable) Fold(f func(key []byte, value *Pos) error) error {

	t.data.Ascend(func(item btree.Item) bool {
		record := item.(*RecordPos)
		f(record.Key, record.Value)
		return true
	})
	return nil
}
