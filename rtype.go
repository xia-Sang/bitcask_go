package lsm3

import (
	"fmt"
	"reflect"
	"strings"
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
