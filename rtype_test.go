package lsm3

import (
	"testing"
)

func TestContains_CreatTable(t *testing.T) {
	//// int 测试案例
	//fmt.Println(evaluateCondition("=", 5, 5, reflect.TypeOf(5)))    // true
	//fmt.Println(evaluateCondition("!=", 5, 10, reflect.TypeOf(5)))  // true
	//fmt.Println(evaluateCondition(">", 10, 5, reflect.TypeOf(5)))   // true
	//fmt.Println(evaluateCondition("<", 5, 10, reflect.TypeOf(5)))   // true
	//fmt.Println(evaluateCondition(">=", 10, 10, reflect.TypeOf(5))) // true
	//fmt.Println(evaluateCondition("<=", 5, 5, reflect.TypeOf(5)))   // true
	//
	//// float64 测试案例
	//fmt.Println(evaluateCondition("=", 5.5, 5.5, reflect.TypeOf(5.5)))    // true
	//fmt.Println(evaluateCondition("!=", 5.5, 10.1, reflect.TypeOf(5.5)))  // true
	//fmt.Println(evaluateCondition(">", 10.2, 5.5, reflect.TypeOf(5.5)))   // true
	//fmt.Println(evaluateCondition("<", 5.5, 10.2, reflect.TypeOf(5.5)))   // true
	//fmt.Println(evaluateCondition(">=", 10.5, 10.5, reflect.TypeOf(5.5))) // true
	//fmt.Println(evaluateCondition("<=", 5.5, 5.5, reflect.TypeOf(5.5)))   // true
	//
	//// bool 测试案例
	//fmt.Println(evaluateCondition("=", true, true, reflect.TypeOf(true)))   // true
	//fmt.Println(evaluateCondition("!=", true, false, reflect.TypeOf(true))) // true
	//
	//// string 测试案例
	//fmt.Println(evaluateCondition("=", "hello", "hello", reflect.TypeOf("")))  // true
	//fmt.Println(evaluateCondition("!=", "hello", "world", reflect.TypeOf(""))) // true
	//fmt.Println(evaluateCondition(">", "world", "hello", reflect.TypeOf("")))  // true
	//fmt.Println(evaluateCondition("<", "hello", "world", reflect.TypeOf("")))  // true
	//fmt.Println(evaluateCondition(">=", "world", "world", reflect.TypeOf(""))) // true
	//fmt.Println(evaluateCondition("<=", "hello", "hello", reflect.TypeOf(""))) // true
	//
	//// 错误类型案例
	//fmt.Println(evaluateCondition("=", 5, "5", reflect.TypeOf(5))) // false，类型不匹配
}
