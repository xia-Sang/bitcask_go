package lsm3

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"unsafe"

	"github.com/xia-Sang/bitcask_go/bitcask"
	"github.com/xia-Sang/bitcask_go/parse"
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

func createStructType(columns []parse.ColumnDefinition) reflect.Type {
	var structFields []reflect.StructField
	for _, column := range columns {
		structFields = append(structFields, reflect.StructField{
			Name: strings.Title(column.Name),
			Type: getFieldType(column.DataType),
			Tag:  reflect.StructTag(fmt.Sprintf(`json:"%s"`, column.Name)),
		})
	}
	// 创建结构体类型
	structType := reflect.StructOf(structFields)
	return structType
}
func (ti *TableInfo) newTableStruct() interface{} {
	return reflect.New(ti.tableStructType).Interface()
}
func (ti *TableInfo) NewStructValues(values map[string]interface{}) interface{} {
	return fillStructValues(ti.newTableStruct(), ti.tableColumns, values)
}
func fillStructValues(instance interface{}, columns []parse.ColumnDefinition, values map[string]interface{}) interface{} {
	val := reflect.ValueOf(instance).Elem()

	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		column := columns[i]
		if field.CanSet() {
			ptr := unsafe.Pointer(field.UnsafeAddr())
			if value, ok := values[column.Name]; ok {
				switch field.Kind() {
				case reflect.String:
					*(*string)(ptr) = value.(string)
				case reflect.Int:
					*(*int)(ptr) = value.(int)
				case reflect.Float64:
					*(*float64)(ptr) = value.(float64)
				case reflect.Bool:
					*(*bool)(ptr) = value.(bool)
				default:
					*(*string)(ptr) = value.(string)
				}
			} else {
				switch field.Kind() {
				case reflect.String:
					*(*string)(ptr) = "nil"
				case reflect.Float64:
					*(*float64)(ptr) = 0.0
				case reflect.Int:
					*(*float64)(ptr) = 0
				case reflect.Bool:
					*(*bool)(ptr) = false
				default:
					*(*string)(ptr) = "nil"
				}
			}

		}
	}
	return val.Interface()
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
	// 确保插入的字段名与表结构匹配
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
		key, value := tableid_row(c.ts[table.tableName], table.rowIndex), serializedValue
		//fmt.Println("key, value", string(key), string(value))
		err = c.db.Set(key, value)
		if err != nil {
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
	// fmt.Println(selectAst)
	table, ok := c.table[selectAst.Table]
	if !ok {
		return errors.New("table not exist")
	}

	var keys []uint32
	for id := range table.tableMapIndex {
		keys = append(keys, id)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})
	var values []interface{}
	for _, id := range keys {
		key := tableid_row(c.ts[table.tableName], id)
		encValue, err := c.db.Query(key)
		if err != nil {
			return err
		}
		value := table.newTableStruct() //*struct
		if err := deserialize(encValue, value); err != nil {
			return err
		}
		values = append(values, value)
	}
	// fmt.Println(selectAst.Projects)
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

// getFieldType 将列的数据类型映射为Go的数据类型
func getFieldType(columnType string) reflect.Type {
	switch columnType {
	case "int":
		return reflect.TypeOf(int(0))
	case "string":
		return reflect.TypeOf("")
	case "bool":
		return reflect.TypeOf(false)
	case "float64":
		return reflect.TypeOf(0.0)
	default:
		return reflect.TypeOf("")
	}
}

// serialize 将interface{}类型的数据序列化为字节数组
func serialize(data interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(data)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// deserialize 将字节数组反序列化为interface{}类型的数据
func deserialize(data []byte, v interface{}) error {
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	return dec.Decode(v)
}

// tableid_row 生成一个唯一的行标识符
func tableid_row(tableIndex, rowIndex uint32) []byte {
	return []byte(fmt.Sprintf("%03d_%04d", tableIndex, rowIndex))
}
