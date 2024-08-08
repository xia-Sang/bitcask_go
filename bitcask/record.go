package bitcask

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// Record 实现record记录信息
type Record struct {
	TxSeq uint32
	Key   []byte     // key
	Value []byte     // value
	RType RecordType // 类型信息
}

func (r *Record) show() string {
	return fmt.Sprintf("(%v:%s:%s:%v)\n", r.TxSeq, r.Key, r.Value, r.RType)
}

//	update | key size | key | value size | value
//	deleled| key size | key
//
// tx update | tx seq   |key size | key | value size | value
// tx deleled| tx seq   |key size | key
// tx findish| tx seq   |
func (r *Record) Show() string {
	return fmt.Sprintf("%s:%v:%v", r.Key, r.Value, r.RType)
}

type RecordType uint8 //record类型信息

const (
	RecordUpdate        RecordType = iota //数据更新
	RecordDelete                          //数据删除
	RecordBatchUpdated                    //事务数据更新
	RecordBatchDeleted                    //事务数据删除
	RecordBatchFinished                   //事务数据完成
)

// Bytes 将数据转为 bytes进行存储
func (r *Record) Bytes() (int, []byte) {
	buf := bytes.NewBuffer(nil)
	binary.Write(buf, binary.LittleEndian, r.RType)
	if r.RType == RecordBatchFinished || r.RType == RecordBatchDeleted || r.RType == RecordBatchUpdated {
		binary.Write(buf, binary.LittleEndian, r.TxSeq)
	}
	if r.RType == RecordBatchFinished {
		return buf.Len(), buf.Bytes()
	}
	binary.Write(buf, binary.LittleEndian, uint32(len(r.Key)))
	buf.Write([]byte(r.Key))
	if r.RType == RecordUpdate || r.RType == RecordBatchUpdated {
		binary.Write(buf, binary.LittleEndian, uint32(len(r.Value)))
		buf.Write([]byte(r.Value))
	}
	return buf.Len(), buf.Bytes()
}

// Restore 将数据进行恢复处理
func (r *Record) Restore(data []byte) {
	var n uint32
	buf := bytes.NewBuffer(data)
	binary.Read(buf, binary.LittleEndian, &r.RType)
	if r.RType == RecordBatchFinished || r.RType == RecordBatchDeleted || r.RType == RecordBatchUpdated {
		binary.Read(buf, binary.LittleEndian, &r.TxSeq)
	}
	if r.RType == RecordBatchFinished {

	} else {
		binary.Read(buf, binary.LittleEndian, &n)
		r.Key = []byte(string(buf.Next(int(n))))
		if r.RType == RecordUpdate || r.RType == RecordBatchUpdated {
			binary.Read(buf, binary.LittleEndian, &n)
			r.Value = []byte(string(buf.Next(int(n))))
		}
		buf = nil
	}

}
func (w *WalWriter) RestoreAll(mem *MemTable, txMap map[uint32][]*txInfo) (uint32, uint32, error) {
	return restoreAll(w.dest, mem, w.fileName, txMap)
}
func (w *WalReader) RestoreAll(mem *MemTable, txMap map[uint32][]*txInfo) (uint32, uint32, error) {
	return restoreAll(w.src, mem, w.fileName, txMap)
}

type txInfo struct {
	rType RecordType //类型
	key   []byte     //删除操作只需要传入key
	pos   *Pos       //更新 只需要传入pos
}

func restoreAll(buf io.Reader, mem *MemTable, fileName string, txMap map[uint32][]*txInfo) (uint32, uint32, error) {
	var (
		rType  RecordType
		n      uint32
		rTxSeq uint32
		txSeq  int32 = -1
	)
	data := make([]byte, 20)
	var offset uint32
	for {
		var length uint32
		if err := binary.Read(buf, binary.LittleEndian, &rType); err != nil {
			if err == io.EOF {
				break
			}
			return 0, offset, err
		}
		length += 1
		if rType == RecordBatchFinished || rType == RecordBatchDeleted || rType == RecordBatchUpdated {
			if err := binary.Read(buf, binary.LittleEndian, &rTxSeq); err != nil {
				if err == io.EOF {
					break
				}
				return 0, offset, err
			}
			length += 4
		}
		if rType == RecordBatchFinished {
			for _, v := range txMap[rTxSeq] {
				if v.rType == RecordBatchUpdated {
					mem.Set(&RecordPos{
						Key:   v.key,
						Value: v.pos,
					})
				} else {
					mem.Delete(v.key)
				}
			}
			txSeq = max(int32(rTxSeq), txSeq)
			delete(txMap, rTxSeq)
		} else {
			if err := binary.Read(buf, binary.LittleEndian, &n); err != nil {
				return 0, offset, err
			}
			length += 4
			if cap(data) < int(n) {
				data = make([]byte, n)
			} else {
				data = data[:n]
			}
			count, err := io.ReadFull(buf, data)
			if err != nil {
				return 0, offset, err
			}
			if count != int(n) {
				return 0, offset, errors.New("invalid data")
			}
			length += n
			record := &Record{Key: []byte(string(data)), RType: rType}

			if rType == RecordUpdate || rType == RecordBatchUpdated {
				if err := binary.Read(buf, binary.LittleEndian, &n); err != nil {
					return 0, offset, err
				}
				length += 4
				if cap(data) < int(n) {
					data = make([]byte, n)
				} else {
					data = data[:n]
				}
				count, err := io.ReadFull(buf, data)
				if err != nil {
					return 0, offset, err
				}
				if count != int(n) {
					return 0, offset, errors.New("valid data")
				}
				length += n
				record.Value = []byte(string(data))
				pos := &Pos{
					fileName: fileName,
					offset:   offset,
					length:   length,
				}
				if rType == RecordUpdate {
					mem.Set(&RecordPos{
						Key:   record.Key,
						Value: pos,
					})
				} else {
					txMap[rTxSeq] = append(txMap[rTxSeq], &txInfo{
						rType: RecordBatchUpdated,
						key:   record.Key,
						pos:   pos,
					})
				}
			} else {
				if rType == RecordUpdate {
					mem.Delete(record.Key)
				} else {
					txMap[rTxSeq] = append(txMap[rTxSeq], &txInfo{
						rType: RecordBatchDeleted,
						key:   record.Key,
					})
				}
			}
		}
		offset += length
	}
	if txSeq == -1 {
		return 0, offset, nil
	}
	return uint32(txSeq) + 1, offset, nil
}
