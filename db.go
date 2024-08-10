package lsm3

import (
	"errors"
	"fmt"
	"github.com/xia-Sang/bitcask_go/bitcask"
	"github.com/xia-Sang/bitcask_go/parse"
	"reflect"
	"sort"
	"strings"
)

type TableInfo struct {
	rowIndex        uint32
	tableName       string
	tableColumns    []parse.ColumnDefinition
	tableStructType reflect.Type
	tableMapIndex   map[uint32]struct{}
}

func NewTableInfo(ast *parse.CreateTree) *TableInfo {
	ti := &TableInfo{
		rowIndex:        0,
		tableName:       ast.Table,
		tableColumns:    ast.Columns,
		tableStructType: createStructType(ast.Columns),
		tableMapIndex:   make(map[uint32]struct{}),
	}
	return ti
}

type Contains struct {
	table      map[string]*TableInfo
	ts         map[string]uint32
	tableIndex uint32
	row        uint32
	db         *bitcask.BitCask
}

// NewContains 创建表之后 我们需要对于table 进行存储
// 并且记录重要信息
// table index，table ast
func NewContains() (*Contains, error) {
	c := &Contains{
		table:      make(map[string]*TableInfo),
		ts:         make(map[string]uint32),
		tableIndex: 0,
		row:        0,
	}
	opts, err := bitcask.NewOptions("./data")
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
		key, value := tableName(c.ts[table.tableName], table.rowIndex), serializedValue
		if err = c.db.Set(key, value); err != nil {
			return err
		}
		table.tableMapIndex[table.rowIndex] = struct{}{}
		table.rowIndex++
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
		delete(c.table[deleteAst.Table].tableMapIndex, id)
	}
	fmt.Println("Delete Table")
	fmt.Println(values)

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
	for id := range tableInfo.tableMapIndex {
		keys = append(keys, id)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})

	var values []interface{}
	var ids []uint32

	if len(where) == 0 {
		for _, id := range keys {
			key := tableName(c.ts[tableInfo.tableName], id)
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

	field, ok := tableInfo.tableStructType.FieldByName(strings.Title(column))
	if !ok {
		return nil, nil, errors.New("field not exist")
	}
	fieldType := field.Type

	rightValue, err := convertToType(rightValueStr, fieldType)
	if err != nil {
		return nil, nil, err
	}

	for _, id := range keys {
		key := tableName(c.ts[tableInfo.tableName], id)
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
