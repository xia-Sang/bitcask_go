package lsm3

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path"
)

const (
	metaFile = "db.meta"
)

func (c *Contains) SaveToDB() error {
	length, data, err := c.Bytes()
	if err != nil {
		return err
	}

	fp, err := os.OpenFile(path.Join(c.dirPath, metaFile), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer fp.Close()

	if err := binary.Write(fp, binary.BigEndian, uint32(length)); err != nil {
		return err
	}

	if _, err := fp.Write(data); err != nil {
		return err
	}
	// 写入TableInfo
	for _, table := range c.table {
		length, data, err := table.Bytes()

		if err != nil {
			return err
		}
		if err := binary.Write(fp, binary.BigEndian, uint32(length)); err != nil {
			return err
		}

		if _, err := fp.Write(data); err != nil {
			return err
		}
	}
	if err := fp.Sync(); err != nil {
		return err
	}
	return nil
}
func LoadFromDB(dirPath string) (*Contains, error) {
	metaFile := path.Join(dirPath, metaFile)
	if _, err := os.Stat(metaFile); os.IsNotExist(err) {

		return &Contains{
			table: make(map[string]*TableInfo),
			ts:    make(map[string]uint32),
		}, nil
	}
	fp, err := os.OpenFile(metaFile, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer fp.Close()
	// 进行数据读取
	var length uint32
	err = binary.Read(fp, binary.BigEndian, &length)
	if err != nil {
		return nil, err
	}
	data := make([]byte, length)
	count, err := io.ReadFull(fp, data)
	if err != nil {
		return nil, err
	}
	if count != int(length) {
		return nil, fmt.Errorf("read length error")
	}
	contain, err := RestoreContains(data)
	if err != nil {
		return nil, err
	}
	contain.table = make(map[string]*TableInfo)
	for {
		err := binary.Read(fp, binary.BigEndian, &length)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		if cap(data) < int(length) {
			data = make([]byte, length)
		} else {
			data = data[:length]
		}
		count, err := io.ReadFull(fp, data)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		if count != int(length) {
			return nil, fmt.Errorf("read length error")
		}
		tableInfo := &TableInfo{}
		if err := tableInfo.FromBytes(data); err != nil {
			return nil, err
		}
		contain.table[tableInfo.TableName] = tableInfo
	}
	return contain, nil
}
