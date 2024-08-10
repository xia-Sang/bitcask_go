package bitcask

import (
	"fmt"
	"testing"

	"github.com/xia-Sang/bitcask_go/util"

	"github.com/stretchr/testify/assert"
)

func TestMemTable_Set(t *testing.T) {
	opts, err := NewOptions("./data")
	assert.Nil(t, err)
	db, err := NewBitCask(opts)
	assert.Nil(t, err)
	t.Log(db)

	m := map[string][]byte{}
	for i := range 100 {
		key, value := util.GenerateKey(i), util.GenerateRandomBytes(12)
		err := db.Set(key, value)
		assert.Nil(t, err)
		m[string(key)] = value
	}
	for i := range 109 {
		key, _ := util.GenerateKey(i), util.GenerateRandomBytes(12)
		val, err := db.Query(key)
		if i < 100 {
			assert.Nil(t, err)
			assert.Equal(t, len(val), 12)
			assert.Equal(t, m[string(key)], val)
		} else {
			assert.NotNil(t, err)
			assert.Equal(t, err, ErrorNotExist)
		}
		t.Logf("(%s:%s)", key, val)
	}
}
func TestMemTable_Set1(t *testing.T) {
	opts, err := NewOptions("./data")
	assert.Nil(t, err)
	db, err := NewBitCask(opts)
	assert.Nil(t, err)
	t.Log(db)
	t.Log("db.getMaxWalSize()", db.getMaxWalSize())
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
		val, err := db.Query(key)
		if i < 10 || i >= 190 {
			assert.Nil(t, err)
			assert.Equal(t, len(val), 12)
			assert.Equal(t, m[string(key)], val)
		} else {
			assert.NotNil(t, err)
			assert.Equal(t, err, ErrorNotExist)
		}
		t.Logf("(%s:%s)", key, val)
	}
}
func TestMemTable_Set6(t *testing.T) {
	opts, err := NewOptions("./data")
	assert.Nil(t, err)
	db, err := NewBitCask(opts)
	assert.Nil(t, err)
	t.Log(db)
	t.Log("db.getMaxWalSize()", db.getMaxWalSize())
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
		val, err := db.Query(key)
		if i < 10 || i >= 190 {
			assert.Nil(t, err)
			assert.Equal(t, len(val), 12)
			assert.Equal(t, m[string(key)], val)
		} else {
			assert.NotNil(t, err)
			assert.Equal(t, err, ErrorNotExist)
		}
	}

	db, err = NewBitCask(opts)
	assert.Nil(t, err)
	for i := range 200 {
		key, _ := util.GenerateKey(i), util.GenerateRandomBytes(12)
		val, err := db.Query(key)
		if i < 10 || i >= 190 {
			assert.Nil(t, err)
			assert.Equal(t, len(val), 12)
			assert.Equal(t, m[string(key)], val)
		} else {
			assert.NotNil(t, err)
			assert.Equal(t, err, ErrorNotExist)
		}
		//t.Logf("(%s:%s)", key, val)
	}
	db.Fold(func(key, value []byte) bool {
		fmt.Printf("(%s:%s)\n", key, value)
		return true
	})
}
func TestMemTable_Get0(t *testing.T) {
	opts, err := NewOptions("./data")
	assert.Nil(t, err)
	db, err := NewBitCask(opts)
	assert.Nil(t, err)
	t.Log(db)
	t.Log("db.getMaxWalSize()", db.getMaxWalSize())

	for i := range 200 {
		key, _ := util.GenerateKey(i), util.GenerateRandomBytes(12)
		val, err := db.Query(key)
		if i < 10 || i >= 190 {
			assert.Nil(t, err)
			assert.Equal(t, len(val), 12)
		} else {
			assert.NotNil(t, err)
			assert.Equal(t, err, ErrorNotExist)
		}
	}
}
func TestMemTable_Get(t *testing.T) {
	opts, err := NewOptions("./data")
	assert.Nil(t, err)
	db, err := NewBitCask(opts)
	assert.Nil(t, err)
	t.Log(db)

	for i := range 200 {
		key, _ := util.GenerateKey(i), util.GenerateRandomBytes(12)
		val, err := db.Query(key)
		if i < 10 || i >= 190 {
			assert.Nil(t, err)
			assert.Equal(t, len(val), 12)
		} else {
			assert.NotNil(t, err)
			assert.Equal(t, err, ErrorNotExist)
		}
		t.Logf("(%s:%s)", key, val)
	}
	err = db.memtable.Fold(func(key []byte, value *Pos) error {
		t.Logf("(%s:%v)\n", key, value)
		return nil
	})
	assert.Nil(t, err)
}
func TestMemTable_Get1(t *testing.T) {
	opts, err := NewOptions("./data")
	assert.Nil(t, err)
	db, err := NewBitCask(opts)
	assert.Nil(t, err)
	t.Log(db)
	t.Log(db.walWriter.offset)

	err = db.Flush()
	assert.Nil(t, err)
}
