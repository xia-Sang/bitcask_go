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
	// return r.Key < than.(*RecordPos).Key
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

// 将数据装为bytes
// func (t *MemTable) Bytes() []byte {
// 	t.mu.RLock()
// 	defer t.mu.RUnlock()

// 	buf := bytes.NewBuffer(nil)
// 	t.data.Ascend(func(item btree.Item) bool {
// 		v := item.(*RecordPos)
// 		n, body := v.Bytes()
// 		binary.Write(buf, binary.LittleEndian, uint32(n))
// 		buf.Write(body)
// 		return true
// 	})
// 	return buf.Bytes()
// }

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

// 不需要添加锁的 set部分已经添加了
// func (t *MemTable) Restore(data []byte) error {
// 	buf := bytes.NewBuffer(data)
// 	var n uint32
// 	for {
// 		if err := binary.Read(buf, binary.LittleEndian, &n); err != nil {
// 			if err == io.EOF {
// 				break
// 			}
// 			return err
// 		}
// 		cmd := new(RecordPos)
// 		cmd.Restore(buf.Next(int(n)))
// 		t.Set(cmd)
// 	}
// 	return nil
// }

// 得到第一个key
func (t *MemTable) First() []byte {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var firstKey []byte
	t.data.Ascend(func(item btree.Item) bool {
		firstKey = item.(*RecordPos).Key
		return false
	})
	return firstKey
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

// 数据合并
func (t *MemTable) Merge(other *MemTable) {
	if other == nil {
		return
	}
	other.mu.RLock()
	defer other.mu.RUnlock()

	other.data.Ascend(func(item btree.Item) bool {
		RecordPos := item.(*RecordPos)
		t.Set(RecordPos)
		return true
	})
}
