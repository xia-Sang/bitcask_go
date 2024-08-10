package bitcask

import (
	"fmt"
	"math"
	"os"
	"path"
	"sort"
	"sync"
	"sync/atomic"
)

// BitCask 结构体表示一个 BitCask 数据库实例
type BitCask struct {
	opts          *Options              // 配置选项
	memtable      *MemTable             // 内存表
	lock          sync.RWMutex          // 读写锁
	walReader     map[string]*WalReader // WAL 读取器
	walWriter     *WalWriter            // WAL 写入器
	memTableIndex uint32                // 内存表索引
	batchSeq      atomic.Uint32         // 批处理序列号
}

// getMaxWalSize 获取最大 WAL 文件大小
func (bc *BitCask) getMaxWalSize() uint32 {
	pow := int(bc.memTableIndex) / bc.opts.tableNum
	if pow > bc.opts.maxLevelNum {
		pow = bc.opts.maxLevelNum
	}
	return bc.opts.maxWalSize * uint32(math.Pow10(pow))
}

// checkWalOverFlow 检查 WAL 文件是否溢出
func (bc *BitCask) checkWalOverFlow() bool {
	return bc.walWriter.Size() >= bc.getMaxWalSize()
}

// tryToFreshMemTable 尝试刷新内存表
func (bc *BitCask) tryToFreshMemTable() {
	if bc.checkWalOverFlow() {
		bc.newWalFile()
	}
}

// NewBitCask 创建并返回一个新的 BitCask 实例
func NewBitCask(opts *Options) (*BitCask, error) {
	bc := &BitCask{
		opts:          opts,
		memtable:      NewMemTable(),
		walReader:     make(map[string]*WalReader),
		memTableIndex: 0,
	}

	if err := bc.LoadWal(); err != nil {
		return nil, err
	}
	return bc, nil
}

// Set 在数据库中添加或更新一个键值对
func (bc *BitCask) Set(key, value []byte) error {
	record := &Record{
		Key:   key,
		Value: value,
		RType: RecordUpdate,
	}
	if err := bc.set(record); err != nil {
		return err
	}
	return nil
}

// set 写入记录到 WAL 文件并更新内存表
func (bc *BitCask) set(record *Record) error {
	bc.lock.Lock()
	pos, err := bc.walWriter.Write(record)
	if err != nil {
		bc.lock.Unlock()
		return err
	}
	if record.RType == RecordUpdate || record.RType == RecordBatchUpdated {
		bc.memtable.Set(&RecordPos{Key: record.Key, Value: pos})
	}
	if record.RType == RecordDelete || record.RType == RecordBatchDeleted {
		bc.memtable.Delete(record.Key)
	}
	bc.lock.Unlock()

	bc.tryToFreshMemTable()
	return nil
}

// Delete 从数据库中删除一个键值对
func (bc *BitCask) Delete(key []byte) error {
	record := &Record{
		Key:   key,
		RType: RecordDelete,
	}
	if err := bc.set(record); err != nil {
		return err
	}
	return nil
}

// Query 检索与给定键关联的值
func (bc *BitCask) Query(key []byte) ([]byte, error) {
	bc.lock.RLock()
	defer bc.lock.RUnlock()

	pos := bc.memtable.Query(key)
	if pos == nil {
		return nil, ErrorNotExist
	}

	if pos.Value.fileName == bc.walWriter.fileName {
		record, err := bc.walWriter.Restore(pos.Value.offset, pos.Value.length)
		if err != nil {
			return nil, err
		}
		return record.Value, nil
	} else {
		if v, ok := bc.walReader[pos.Value.fileName]; ok {
			record, err := v.Restore(pos.Value.offset, pos.Value.length)
			if err != nil {
				return nil, err
			}
			return record.Value, nil
		}
		return nil, ErrorNotExist
	}
}

// Flush 刷新内存表到新的 WAL 文件
func (bc *BitCask) Flush() error {
	bc.newWalFile()

	var ls []string
	for _, w := range bc.walReader {
		ls = append(ls, w.fileName)
	}

	if err := bc.memtable.Fold(func(key []byte, pos *Pos) error {
		if v, ok := bc.walReader[pos.fileName]; ok {
			record, err := v.Restore(pos.offset, pos.length)
			if err != nil {
				return err
			}
			if err := bc.Set(key, record.Value); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}

	for _, v := range ls {
		bc.walReader[v].Clear()
		delete(bc.walReader, v)
	}
	return nil
}

// newWalFile 创建一个新的 WAL 文件
func (bc *BitCask) newWalFile() {
	bc.lock.Lock()
	defer bc.lock.Unlock()

	oldWalName := bc.walWriter.fileName
	bc.walWriter.Close()
	bc.walReader[oldWalName], _ = NewWalReader(oldWalName)
	bc.memTableIndex++
	bc.walWriter, _ = NewWalWriter(walFile(bc.opts.dirPath, bc.memTableIndex))
}

// LoadWal 加载 WAL 文件
func (bc *BitCask) LoadWal() error {
	bc.lock.RLock()
	defer bc.lock.RUnlock()

	fs, err := os.ReadDir(bc.opts.dirPath)
	if err != nil {
		return err
	}
	if len(fs) == 0 {
		bc.walWriter, _ = NewWalWriter(walFile(bc.opts.dirPath, bc.memTableIndex))
		return nil
	}
	var ls []string
	for _, f := range fs {
		if f.IsDir() {
			continue
		}
		if path.Ext(f.Name()) == WalSuffix {
			ls = append(ls, f.Name())
		}
	}

	sort.Slice(ls, func(i, j int) bool {
		return ls[i] < ls[j]
	})

	txMap := make(map[uint32][]*txInfo)
	var txSeq uint32
	for i, f := range ls {
		fileName := path.Join(bc.opts.dirPath, f)
		if i == len(ls)-1 {
			index := getWalFileIndex(f)
			bc.memTableIndex = uint32(index)
			walWriter, err := NewWalWriter(fileName)
			if err != nil {
				return err
			}
			bc.walWriter = walWriter
			seq, offset, err := walWriter.RestoreAll(bc.memtable, txMap)
			if err != nil {
				return err
			}
			bc.walWriter.offset = offset
			txSeq = max(txSeq, seq)
		} else {
			walReader, err := NewWalReader(fileName)
			if err != nil {
				return err
			}
			seq, _, err := walReader.RestoreAll(bc.memtable, txMap)
			if err != nil {
				return err
			}
			bc.walReader[fileName] = walReader
			txSeq = max(txSeq, seq)
		}
	}
	bc.batchSeq.Store(txSeq)
	return nil
}
func (bc *BitCask) Fold(fn func(key, value []byte) bool) error {
	bc.lock.Lock()
	defer bc.lock.Unlock()

	if err := bc.memtable.Fold(func(key []byte, pos *Pos) error {
		if v, ok := bc.walReader[pos.fileName]; ok {
			record, err := v.Restore(pos.offset, pos.length)
			if err != nil {
				return err
			}
			if !fn(record.Key, record.Value) {
				return nil
			}
			return nil
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}
func (bc *BitCask) Show() error {
	bc.lock.RLock()
	defer bc.lock.RUnlock()

	if err := bc.memtable.Fold(func(key []byte, pos *Pos) error {
		if v, ok := bc.walReader[pos.fileName]; ok {
			record, err := v.Restore(pos.offset, pos.length)
			if err != nil {
				return err
			}
			fmt.Printf("(%s:%s)\n", key, record.Value)
			return nil
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}
func (bc *BitCask) ShowMemTable() error {
	bc.lock.RLock()
	defer bc.lock.RUnlock()
	err := bc.Show()
	if err != nil {
		return err
	}
	return nil
}
