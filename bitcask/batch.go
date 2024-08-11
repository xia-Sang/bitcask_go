package bitcask

// Batch 表示一个批处理操作
type Batch struct {
	db       *BitCask           // 数据库实例
	opts     *Options           // 选项配置
	batchSeq uint32             // 批处理序列号
	pending  map[string]*Record // 待处理的记录
}

// NewBatch 创建并返回一个新的 Batch 实例
func NewBatch(db *BitCask, opts *Options) *Batch {
	return &Batch{db: db, opts: opts, pending: make(map[string]*Record), batchSeq: db.batchSeq.Load()}
}

// Set 在批处理中添加或更新一个键值对
func (b *Batch) Set(key []byte, value []byte) error {
	record := &Record{Key: key, Value: value, TxSeq: b.batchSeq, RType: RecordBatchUpdated}
	b.pending[string(key)] = record
	return nil
}

// Delete 从批处理中删除一个键值对
func (b *Batch) Delete(key []byte) error {
	if _, ok := b.pending[string(key)]; ok {
		delete(b.pending, string(key))
	} else {
		record := &Record{Key: key, TxSeq: b.batchSeq, RType: RecordBatchDeleted}
		b.pending[string(key)] = record
	}
	return nil
}

// Commit 提交批处理中的所有操作
func (b *Batch) Commit() error {
	if len(b.pending) == 0 {
		return nil
	}
	if uint32(len(b.pending)) > b.opts.batchMaxNum {
		return ErrorBatchExceed
	}
	for _, record := range b.pending {
		if err := b.db.set(record); err != nil {
			return err
		}
	}
	finishRecord := &Record{
		TxSeq: b.batchSeq,
		RType: RecordBatchFinished,
	}
	if err := b.db.set(finishRecord); err != nil {
		return err
	}
	if b.opts.batchSync {
		if err := b.db.walWriter.Sync(); err != nil {
			return ErrorBatchSync
		}
	}
	b.pending = make(map[string]*Record)
	b.db.batchSeq.Add(1)
	return nil
}
