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
	Key   []byte     // key
	Value []byte     // value
	RType RecordType // 类型信息
}

func (r *Record) Show() string {
	return fmt.Sprintf("%s:%v:%v", r.Key, r.Value, r.RType)
}

type RecordType uint8 //record类型信息

const (
	RecordUpdate RecordType = iota //数据更新
	RecordDelete                   //数据删除
)

// Bytes 将数据转为 bytes进行存储
func (r *Record) Bytes() (int, []byte) {
	buf := bytes.NewBuffer(nil)
	binary.Write(buf, binary.LittleEndian, r.RType)
	binary.Write(buf, binary.LittleEndian, uint32(len(r.Key)))
	buf.Write([]byte(r.Key))
	if r.RType == RecordUpdate {
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
	binary.Read(buf, binary.LittleEndian, &n)
	r.Key = []byte(string(buf.Next(int(n))))
	if r.RType == RecordUpdate {
		binary.Read(buf, binary.LittleEndian, &n)
		r.Value = []byte(string(buf.Next(int(n))))
	}
	buf = nil
}
func (w *WalWriter) RestoreAll(mem *MemTable) (uint32, error) {
	return restoreAll(w.dest, mem, w.fileName)
}
func (w *WalReader) RestoreAll(mem *MemTable) (uint32, error) {
	return restoreAll(w.src, mem, w.fileName)
}
func restoreAll(buf io.Reader, mem *MemTable, fileName string) (uint32, error) {
	var (
		rType RecordType
		n     uint32
	)
	data := make([]byte, 20)
	var offset uint32
	for {
		if err := binary.Read(buf, binary.LittleEndian, &rType); err != nil {
			if err == io.EOF {
				break
			}
			return offset, err
		}
		if err := binary.Read(buf, binary.LittleEndian, &n); err != nil {
			return offset + 4, err
		}
		if cap(data) < int(n) {
			data = make([]byte, n)
		} else {
			data = data[:n]
		}
		count, err := io.ReadFull(buf, data)
		if err != nil {
			return offset, err
		}
		if count != int(n) {
			return offset, errors.New("invalid data")
		}
		record := &Record{Key: []byte(string(data)), RType: rType}
		length := 1 + 4 + n
		if rType == RecordUpdate {
			if err := binary.Read(buf, binary.LittleEndian, &n); err != nil {
				return offset + 4, err
			}
			if cap(data) < int(n) {
				data = make([]byte, n)
			} else {
				data = data[:n]
			}
			count, err := io.ReadFull(buf, data)
			if err != nil {
				return offset, err
			}
			if count != int(n) {
				return offset, errors.New("valid data")
			}
			record.Value = []byte(string(data))
			length += 4 + n
			mem.Set(&RecordPos{
				Key: record.Key,
				Value: &Pos{
					fileName: fileName,
					offset:   offset,
					length:   length,
				},
			})

		} else {
			mem.Delete(record.Key)
		}
		offset += length
	}
	return offset, nil
}
