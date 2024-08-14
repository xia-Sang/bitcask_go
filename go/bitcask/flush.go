package bitcask

import (
	util "github.com/xia-Sang/bitcask_go/util"
)

// NewBitCask 创建并返回一个新的 BitCask 实例
func NewBitCask(opts *Options) (*BitCask, error) {
	bc := &BitCask{
		opts:          opts,
		memtable:      NewMemTable(),
		walReader:     make(map[string]*WalReader),
		memTableIndex: 0,
		stat:          NewStat(),
	}

	if err := bc.LoadWal(); err != nil {
		return nil, err
	}

	// 启动后台 flush goroutine
	return bc, nil
}

// checkFlushFlag 检查是否需要进行 flush
func (bc *BitCask) checkFlushFlag() bool {
	// 检查 walWriter 是否为 nil
	if bc.walWriter == nil {
		return false
	}

	// 检查当前 WAL 文件的大小和无用空间的大小
	curWalSize := bc.getMaxWalSize()
	uselessSize := bc.stat.UselessSize

	// 如果 WAL 文件大小小于设置的无用空间的倍数，进行存储检查
	if curWalSize < uselessSize*bc.opts.walSyncSize {
		// 获取磁盘使用情况
		storage, err := util.GetDiskUsage(bc.opts.dirPath)
		if err != nil {
			return false
		}

		// 检查可用空间是否足够
		if storage.Free > uint64(curWalSize+uselessSize*bc.opts.walSyncSize) {
			return true
		}
	}
	return false
}

func (bc *BitCask) Flush() error {
	// 创建新的 WAL 文件
	bc.newWalFile()

	var ls []string
	for _, w := range bc.walReader {
		ls = append(ls, w.fileName)
	}
	bc.stat = NewStat()

	// 将 MemTable 的数据写入新的 WAL 文件
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

	// 清理旧的 WAL 文件
	for _, v := range ls {
		bc.walReader[v].Clear()
		delete(bc.walReader, v)
	}
	bc.stat.UselessSize = 0

	return nil
}
