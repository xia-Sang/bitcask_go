package bitcask

type Batch struct {
	db       *BitCask
	opts     *Options
	batchSeq uint32
	pending  map[string]*Record
}

func NewBatch(db *BitCask, opts *Options) *Batch {
	return &Batch{db: db, opts: opts, pending: make(map[string]*Record), batchSeq: db.batchSeq.Load()}
}
func (b *Batch) Set(key []byte, value []byte) error {
	record := &Record{Key: key, Value: value, TxSeq: b.batchSeq, RType: RecordBatchUpdated}
	b.pending[string(key)] = record
	return nil
}
func (b *Batch) Delete(key []byte) error {
	if _, ok := b.pending[string(key)]; ok {
		delete(b.pending, string(key))
	} else {
		record := &Record{Key: key, TxSeq: b.batchSeq, RType: RecordBatchDeleted}
		b.pending[string(key)] = record
	}
	return nil
}
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
	b.pending = make(map[string]*Record)
	b.db.batchSeq.Add(1)
	return nil
}
