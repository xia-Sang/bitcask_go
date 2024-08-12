package lsm3

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/xia-Sang/bitcask_go/bitcask"
	"github.com/xia-Sang/bitcask_go/parse"
)

type TableInfo struct {
	RowIndex        uint32
	TableName       string
	TableColumns    []parse.ColumnDefinition
	TableStructType reflect.Type
	TableMapIndex   map[uint32]struct{}
}

func (ti *TableInfo) Bytes() (int, []byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)

	// 创建临时结构体，不包括 TableStructType
	temp := struct {
		RowIndex      uint32
		TableName     string
		TableColumns  []parse.ColumnDefinition
		TableMapIndex map[uint32]struct{}
	}{
		RowIndex:      ti.RowIndex,
		TableName:     ti.TableName,
		TableColumns:  ti.TableColumns,
		TableMapIndex: ti.TableMapIndex,
	}

	// 序列化临时结构体
	err := enc.Encode(temp)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to encode TableInfo: %w", err)
	}

	return buf.Len(), buf.Bytes(), nil
}

func (ti *TableInfo) FromBytes(data []byte) error {
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)

	// 临时结构体用来反序列化，不包括 TableStructType
	temp := struct {
		RowIndex      uint32
		TableName     string
		TableColumns  []parse.ColumnDefinition
		TableMapIndex map[uint32]struct{}
	}{}

	// 反序列化临时结构体
	err := dec.Decode(&temp)
	if err != nil {
		return fmt.Errorf("failed to decode TableInfo: %w", err)
	}

	// 还原字段
	ti.RowIndex = temp.RowIndex
	ti.TableName = temp.TableName
	ti.TableColumns = temp.TableColumns
	ti.TableMapIndex = temp.TableMapIndex

	// TableStructType 不需要处理
	ti.TableStructType = createStructType(ti.TableColumns)
	return nil
}

func NewTableInfo(ast *parse.CreateTree) *TableInfo {
	ti := &TableInfo{
		RowIndex:        0,
		TableName:       ast.Table,
		TableColumns:    ast.Columns,
		TableStructType: createStructType(ast.Columns),
		TableMapIndex:   make(map[uint32]struct{}),
	}
	return ti
}

type Contains struct {
	table      map[string]*TableInfo
	ts         map[string]uint32
	tableIndex uint32
	row        uint32
	db         *bitcask.BitCask
	dirPath    string
}

// Bytes compresses the Contains into a byte slice
func (c *Contains) Bytes() (int, []byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(c.ts)
	if err != nil {
		return 0, nil, err
	}
	err = enc.Encode(c.tableIndex)
	if err != nil {
		return 0, nil, err
	}
	err = enc.Encode(c.row)
	if err != nil {
		return 0, nil, err
	}
	return buf.Len(), buf.Bytes(), nil
}

// RestoreContains Restore decompresses the byte slice into a Contains
func RestoreContains(data []byte) (*Contains, error) {
	buf := bytes.NewReader(data)
	dec := gob.NewDecoder(buf)
	var ts map[string]uint32
	var tableIndex uint32
	var row uint32
	err := dec.Decode(&ts)
	if err != nil {
		return nil, err
	}
	err = dec.Decode(&tableIndex)
	if err != nil {
		return nil, err
	}
	err = dec.Decode(&row)
	if err != nil {
		return nil, err
	}
	return &Contains{
		ts:         ts,
		tableIndex: tableIndex,
		row:        row,
	}, nil
}

// NewContainsWithDirPath 创建表之后 我们需要对于table 进行存储
// 并且记录重要信息
// table index，table ast
func NewContainsWithDirPath(dirPath string) (*Contains, error) {
	c, err := LoadFromDB(dirPath)
	c.dirPath = dirPath
	if err != nil {
		return nil, err
	}
	opts, err := bitcask.NewOptions(dirPath)
	if err != nil {
		return nil, err
	}
	db, err := bitcask.NewBitCask(opts)
	if err != nil {
		return nil, err
	}
	c.db = db
	return c, nil
}

// NewContains 测试使用
func NewContains() (*Contains, error) {
	dirPath := "./data"
	c, err := LoadFromDB(dirPath)
	c.dirPath = dirPath
	if err != nil {
		return nil, err
	}
	opts, err := bitcask.NewOptions(dirPath)
	if err != nil {
		return nil, err
	}
	db, err := bitcask.NewBitCask(opts)
	if err != nil {
		return nil, err
	}
	c.db = db
	return c, nil
}
func (c *Contains) Close() error {
	if err := c.SaveToDB(); err != nil {
		return err
	}
	if err := c.db.Close(); err != nil {
		return err
	}
	c.ts = nil
	c.table = nil
	c.tableIndex = 0
	c.row = 0
	return nil
}
func (c *Contains) CreatTable(sql string) error {
	scan := parse.NewScannerFromString(sql)
	createAst, err := scan.ParseCreate()
	if err != nil {
		return err
	}
	if _, ok := c.ts[createAst.Table]; ok {
		return fmt.Errorf("table exist")
	}
	tableInfo := NewTableInfo(createAst)
	c.table[createAst.Table] = tableInfo
	c.ts[createAst.Table] = c.tableIndex
	c.tableIndex++
	return nil
}
func (c *Contains) InsertTable(sql string) error {
	scan := parse.NewScannerFromString(sql)
	insertAst, err := scan.ParseInsert()
	if err != nil {
		return err
	}

	table, ok := c.table[insertAst.Table]
	if !ok {
		return errors.New("table not exist")
	}
	for _, tableInfo := range insertAst.Values {
		dict := make(map[string]interface{})
		for i, value := range tableInfo {
			dict[insertAst.Columns[i]] = value
		}
		val := table.NewStructValues(dict)
		serializedValue, err := serialize(val)
		if err != nil {
			return err
		}
		key, value := tableName(c.ts[table.TableName], table.RowIndex), serializedValue
		if err = c.db.Set(key, value); err != nil {
			return err
		}
		table.TableMapIndex[table.RowIndex] = struct{}{}
		table.RowIndex++
	}
	return nil
}
func (c *Contains) SelectTable(sql string) error {
	scan := parse.NewScannerFromString(sql)
	selectAst, err := scan.ParseSelect()
	if err != nil {
		return err
	}
	values, _, err := c.getTableInfosWithWhere(selectAst.Table, selectAst.Where)
	if err != nil {
		return err
	}

	if len(values) > 0 {
		if selectAst.Projects[0] == "*" {
			printTable(values)
		} else {
			printTableWithColumns(values, selectAst.Projects)
		}
	} else {
		fmt.Println("No data found.")
	}

	return nil
}
func (c *Contains) DeleteTable(sql string) error {
	scan := parse.NewScannerFromString(sql)
	deleteAst, err := scan.ParseDelete()
	if err != nil {
		return err
	}

	values, ids, err := c.getTableInfosWithWhere(deleteAst.Table, deleteAst.Where)
	if err != nil {
		return err
	}
	//fmt.Println(ids)

	for _, id := range ids {
		key := tableName(c.ts[deleteAst.Table], id)
		if err := c.db.Delete(key); err != nil {
			return err
		}
		delete(c.table[deleteAst.Table].TableMapIndex, id)
	}
	fmt.Println("Delete Table")
	printTable(values)

	return nil
}

// tabled_row 生成一个唯一的行标识符
func tableName(tableIndex, rowIndex uint32) []byte {
	return []byte(fmt.Sprintf("%03d_%04d", tableIndex, rowIndex))
}

// 根据where信息来实现对于数据的检索
func (c *Contains) getTableInfosWithWhere(table string, where []string) ([]interface{}, []uint32, error) {
	tableInfo, ok := c.table[table]
	if !ok {
		return nil, nil, errors.New("table not exist")
	}
	var keys []uint32
	for id := range tableInfo.TableMapIndex {
		keys = append(keys, id)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})

	var values []interface{}
	var ids []uint32

	if len(where) == 0 {
		for _, id := range keys {
			key := tableName(c.ts[tableInfo.TableName], id)
			encValue, err := c.db.Query(key)
			if err != nil {
				return nil, nil, err
			}
			value := tableInfo.newTableStruct()
			if err := deserialize(encValue, value); err != nil {
				return nil, nil, err
			}
			values = append(values, value)
			ids = append(ids, id)
		}
		return values, ids, nil
	}

	if len(where) != 3 {
		return nil, nil, errors.New("invalid where condition")
	}

	column := where[0]
	op := where[1]
	rightValueStr := where[2]

	field, ok := tableInfo.TableStructType.FieldByName(strings.Title(column))
	if !ok {
		return nil, nil, errors.New("field not exist")
	}
	fieldType := field.Type

	rightValue, err := convertToType(rightValueStr, fieldType)
	if err != nil {
		return nil, nil, err
	}

	for _, id := range keys {
		key := tableName(c.ts[tableInfo.TableName], id)
		encValue, err := c.db.Query(key)
		if err != nil {
			return nil, nil, err
		}
		value := tableInfo.newTableStruct()
		if err := deserialize(encValue, value); err != nil {
			return nil, nil, err
		}
		structValue := reflect.ValueOf(value).Elem()
		leftValue := structValue.FieldByName(strings.Title(column))

		if ok := evaluateCondition(op, leftValue, rightValue); ok {
			values = append(values, value)
			ids = append(ids, id)
		}
	}
	return values, ids, nil
}
