package bitcask

import (
	"math"
	"os"
	"path"
	"sort"
	"sync"
	"sync/atomic"
)

type BitCask struct {
	opts          *Options
	memtable      *MemTable
	lock          sync.RWMutex
	walReader     map[string]*WalReader
	walWriter     *WalWriter
	memTableIndex uint32
	batchSeq      atomic.Uint32
}

func (bc *BitCask) getMaxWalSize() uint32 {
	pow := int(bc.memTableIndex) / bc.opts.tableNum
	if pow > bc.opts.maxLevelNum {
		pow = bc.opts.maxLevelNum
	}
	return bc.opts.maxWalSize * uint32(math.Pow10(pow))
}
func (bc *BitCask) checkWalOverFlow() bool {
	return bc.walWriter.Size() >= bc.getMaxWalSize()
}
func (bc *BitCask) tryToFreshMemTable() {
	if bc.checkWalOverFlow() {
		bc.newWalFile()
	}
}
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
func (bc *BitCask) Flush() error {
	bc.newWalFile()

	var ls []string
	for k, _ := range bc.walReader {
		ls = append(ls, k)
	}

	if err := bc.memtable.Fold(func(key []byte, pos *Pos) error {
		if v, ok := bc.walReader[pos.fileName]; ok {

			record, err := v.Restore(pos.offset, pos.length)
			if err != nil {
				return err
			}
			//fmt.Printf("(%s:%s)\n", key, record.Value)
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
func (bc *BitCask) newWalFile() {
	bc.lock.Lock()
	defer bc.lock.Unlock()

	oldWalName := bc.walWriter.fileName
	bc.walWriter.Close()
	bc.walReader[oldWalName], _ = NewWalReader(oldWalName)
	bc.memTableIndex++
	bc.walWriter, _ = NewWalWriter(walFile(bc.opts.dirPath, bc.memTableIndex))
}
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
			//fmt.Println("offset", offset, bc.walWriter.offset)
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
