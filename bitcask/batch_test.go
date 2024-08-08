package bitcask

import (
	"bitcask/util"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMemTable_Batch1(t *testing.T) {
	opts, err := NewOptions("./data")
	assert.Nil(t, err)
	bc, err := NewBitCask(opts)
	assert.Nil(t, err)
	db := NewBatch(bc, bc.opts)
	m := map[string][]byte{}
	for i := range 200 {
		key, value := util.GenerateKey(i), util.GenerateRandomBytes(12)
		err := db.Set(key, value)
		assert.Nil(t, err)
		m[string(key)] = value
	}
	for i := 10; i < 190; i++ {
		key, _ := util.GenerateKey(i), util.GenerateRandomBytes(12)
		err := db.Delete(key)
		assert.Nil(t, err)
	}
	for i := range 200 {
		key, _ := util.GenerateKey(i), util.GenerateRandomBytes(12)
		val, err := bc.Query(key)

		assert.NotNil(t, err)
		assert.Equal(t, err, ErrorNotExist)
		t.Logf("(%s:%s)", key, val)
	}
}
func TestMemTable_Batch2(t *testing.T) {
	opts, err := NewOptions("./data")
	assert.Nil(t, err)
	bc, err := NewBitCask(opts)
	assert.Nil(t, err)
	db := NewBatch(bc, bc.opts)
	m := map[string][]byte{}
	for i := range 200 {
		key, value := util.GenerateKey(i), util.GenerateRandomBytes(12)
		err := db.Set(key, value)
		assert.Nil(t, err)
		m[string(key)] = value
	}
	for i := 10; i < 190; i++ {
		key, _ := util.GenerateKey(i), util.GenerateRandomBytes(12)
		err := db.Delete(key)
		assert.Nil(t, err)
	}
	err = db.Commit()
	assert.Nil(t, err)
	for i := range 200 {
		key, _ := util.GenerateKey(i), util.GenerateRandomBytes(12)
		val, err := bc.Query(key)

		//assert.NotNil(t, err)
		//assert.Equal(t, err, ErrorNotExist)
		t.Logf("(%s:%s:%v)", key, val, err)
	}
}
func TestMemTable_Batch3(t *testing.T) {
	opts, err := NewOptions("./data")
	assert.Nil(t, err)
	bc, err := NewBitCask(opts)
	assert.Nil(t, err)
	//bc.memtable.Show()

	for i := range 200 {
		key, _ := util.GenerateKey(i), util.GenerateRandomBytes(12)
		val, err := bc.Query(key)
		if i < 10 || i >= 190 {
			assert.Nil(t, err)
			assert.Equal(t, len(val), 12)
		} else {
			assert.NotNil(t, err)
			assert.Equal(t, err, ErrorNotExist)
		}
		//assert.NotNil(t, err)
		//assert.Equal(t, err, ErrorNotExist)
		//t.Logf("(%s:%s:%v)", key, val, err)
	}
}
func TestMemTable_Batch4(t *testing.T) {
	opts, err := NewOptions("./data")
	assert.Nil(t, err)
	bc, err := NewBitCask(opts)
	assert.Nil(t, err)
	t.Log("bc.batchSeq.Load()", bc.batchSeq.Load())
	db := NewBatch(bc, bc.opts)
	m := map[string][]byte{}
	for i := range 200 {
		key, value := util.GenerateKey(i), util.GenerateRandomBytes(12)
		err := db.Set(key, value)
		assert.Nil(t, err)
		m[string(key)] = value
	}
	err = db.Commit()
	assert.Nil(t, err)

	db = NewBatch(bc, bc.opts)
	for i := 10; i < 190; i++ {
		key, _ := util.GenerateKey(i), util.GenerateRandomBytes(12)
		err := db.Delete(key)
		assert.Nil(t, err)
	}
	err = db.Commit()
	assert.Nil(t, err)

	for i := range 200 {
		key, _ := util.GenerateKey(i), util.GenerateRandomBytes(12)
		val, err := bc.Query(key)
		if i < 10 || i >= 190 {
			assert.Nil(t, err)
			assert.Equal(t, len(val), 12)
			assert.Equal(t, val, m[string(key)])
		} else {
			assert.NotNil(t, err)
			assert.Equal(t, err, ErrorNotExist)
		}
		t.Logf("(%s:%s:%v)", key, val, err)
	}

}
func TestMemTable_Batch5(t *testing.T) {
	opts, err := NewOptions("./data")
	assert.Nil(t, err)
	bc, err := NewBitCask(opts)
	assert.Nil(t, err)
	bc.memtable.Show()
}
func TestMemTable_Batch6(t *testing.T) {
	opts, err := NewOptions("./data")
	assert.Nil(t, err)
	bc, err := NewBitCask(opts)
	assert.Nil(t, err)
	t.Log("bc.batchSeq.Load()", bc.batchSeq.Load())
	db := NewBatch(bc, bc.opts)
	m := map[string][]byte{}
	for i := range 200 {
		key, value := util.GenerateKey(i), util.GenerateRandomBytes(12)
		err := db.Set(key, value)
		assert.Nil(t, err)
		m[string(key)] = value
	}
	err = db.Commit()
	assert.Nil(t, err)

	db = NewBatch(bc, bc.opts)
	for i := 10; i < 190; i++ {
		key, _ := util.GenerateKey(i), util.GenerateRandomBytes(12)
		err := db.Delete(key)
		assert.Nil(t, err)
	}
	err = db.Commit()
	assert.Nil(t, err)

	bc.memtable.Show()
	bc.memtable.Fold(func(key []byte, value *Pos) error {
		record, err := bc.walWriter.Restore(value.offset, value.length)
		t.Log(record.show())
		return err
	})
}
