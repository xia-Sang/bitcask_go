package lsm3

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/xia-Sang/bitcask_go/parse"
	"reflect"
	"strconv"
	"strings"
	"unsafe"
)

func evaluateCondition(op string, leftValue reflect.Value, rightValue interface{}) bool {
	switch op {
	case "=":
		return reflect.DeepEqual(leftValue.Interface(), rightValue)
	case ">":
		return compare(leftValue, rightValue) > 0
	case "<":
		return compare(leftValue, rightValue) < 0
	case ">=":
		return compare(leftValue, rightValue) >= 0
	case "<=":
		return compare(leftValue, rightValue) <= 0
	case "!=":
		return !reflect.DeepEqual(leftValue.Interface(), rightValue)
	default:
		return false
	}
}

func compare(a reflect.Value, b interface{}) int {
	bValue := reflect.ValueOf(b)
	switch a.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return int(a.Int() - bValue.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return int(a.Uint() - bValue.Uint())
	case reflect.Float32, reflect.Float64:
		diff := a.Float() - bValue.Float()
		if diff > 0 {
			return 1
		} else if diff < 0 {
			return -1
		}
		return 0
	case reflect.String:
		return strings.Compare(a.String(), bValue.String())
	default:
		panic(fmt.Sprintf("unsupported type for comparison: %v", a.Kind()))
	}
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
	return reflect.New(ti.TableStructType).Interface()
}
func (ti *TableInfo) NewStructValues(values map[string]interface{}) interface{} {
	return fillStructValues(ti.newTableStruct(), ti.TableColumns, values)
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
func convertToType(value string, targetType reflect.Type) (interface{}, error) {
	switch targetType.Kind() {
	case reflect.String:
		return value, nil
	case reflect.Int:
		return strconv.Atoi(value)
	case reflect.Float64:
		return strconv.ParseFloat(value, 64)
	case reflect.Bool:
		return strconv.ParseBool(value)
	default:
		return nil, fmt.Errorf("unsupported type: %v", targetType)
	}
}
