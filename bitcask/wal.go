package bitcask

import (
	"fmt"
	"io"
	"os"
)

type WalWriter struct {
	fileName string
	dest     *os.File
	offset   uint32
}

func (w *WalWriter) Sync() error {
	return w.dest.Sync()
}
func (w *WalWriter) Size() uint32 {
	return w.offset
}
func NewWalWriter(fileName string) (*WalWriter, error) {
	fp, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, os.ModePerm)
	if err != nil {
		return nil, err
	}

	return &WalWriter{
		fileName: fileName,
		dest:     fp,
		offset:   0,
	}, nil
}
func (w *WalWriter) Write(record *Record) (*Pos, error) {
	n, body, err := record.Bytes()
	if err != nil {
		return nil, err
	}
	length, err := w.dest.Write(body)
	if err != nil {
		return nil, err
	}
	if length != n {
		return nil, fmt.Errorf("write length %d != %d", length, n)
	}
	pos := &Pos{
		offset:   w.offset,
		fileName: w.fileName,
		length:   uint32(length),
	}
	w.offset += uint32(length)
	return pos, nil
}
func (w *WalWriter) Close() {
	_ = w.dest.Sync() //添加sync操作 避免数据遗漏
	_ = w.dest.Close()
}

// WalReader WalReader结构体
type WalReader struct {
	fileName string
	src      *os.File
}

func (w *WalReader) Close() {
	_ = w.src.Close()
}

func NewWalReader(fileName string) (*WalReader, error) {
	fp, err := os.OpenFile(fileName, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return nil, err
	}
	return &WalReader{
		fileName: fileName,
		src:      fp,
	}, nil
}
func restore(r io.ReaderAt, offset, length uint32) (*Record, error) {
	buf := make([]byte, length)

	if _, err := r.ReadAt(buf, int64(offset)); err != nil {
		return nil, err
	}
	record := &Record{}

	if err := record.Restore(buf); err != nil {
		return nil, err
	}
	return record, nil
}
func (w *WalWriter) Restore(offset, length uint32) (*Record, error) {
	return restore(w.dest, offset, length)
}
func (w *WalReader) Restore(offset, length uint32) (*Record, error) {
	return restore(w.src, offset, length)
}

func (w *WalReader) Clear() {
	_ = w.src.Close()
	_ = os.Remove(w.fileName)
}
