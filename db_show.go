package lsm3

import (
	"fmt"
	"reflect"
	"strings"
)

func (c *Contains) ShowTables() {
	fmt.Println("+-----------------+")
	fmt.Println("| Tables          |")
	fmt.Println("+-----------------+")

	for table := range c.ts {
		fmt.Printf("| %-15s |\n", table)
	}

	fmt.Println("+-----------------+")
}
func (c *Contains) ShowColumns(table string) {
	v, ok := c.table[table]
	if !ok {
		fmt.Printf("Table: %s 不存在！\n", table)
		return
	}
	fmt.Printf("+-----------------+-----------------+\n")
	fmt.Printf("| Column Name     | Data Type       |\n")
	fmt.Printf("+-----------------+-----------------+\n")

	for _, column := range v.tableColumns {
		fmt.Printf("| %-15s | %-15s |\n", column.Name, column.DataType)
	}

	fmt.Printf("+-----------------+-----------------+\n")
}

// fmt.Printf("Table %s not found\n", tableName)

func (c *Contains) Show() {
}
func (c *Contains) show() {
	for k, v := range c.table {
		fmt.Println(k, v)
	}
	fmt.Println("========================")
	err := c.db.Fold(func(key, value []byte) bool {
		fmt.Printf("(k:%s-v:%s)\n", key, value)
		return true
	})
	fmt.Println(err)
}
func printTable(values []interface{}) {
	if len(values) == 0 {
		return
	}

	firstValue := reflect.ValueOf(values[0]).Elem()
	t := firstValue.Type()

	// 获取每列的最大宽度
	widths := make([]int, firstValue.NumField())
	for i := 0; i < firstValue.NumField(); i++ {
		fieldName := t.Field(i).Name
		widths[i] = len(fieldName)
	}

	for _, value := range values {
		v := reflect.ValueOf(value).Elem()
		for i := 0; i < v.NumField(); i++ {
			fieldValue := fmt.Sprintf("%v", v.Field(i).Interface())
			if len(fieldValue) > widths[i] {
				widths[i] = len(fieldValue)
			}
		}
	}

	// 打印表头的分隔符
	printSeparator(widths)

	// 打印表头
	fmt.Print("|")
	for i := 0; i < firstValue.NumField(); i++ {
		fmt.Printf(" %-*s |", widths[i], t.Field(i).Name)
	}
	fmt.Println()

	// 打印表头下面的分隔符
	printSeparator(widths)

	// 打印每一行数据
	for _, value := range values {
		v := reflect.ValueOf(value).Elem()
		fmt.Print("|")
		for i := 0; i < v.NumField(); i++ {
			fieldValue := fmt.Sprintf("%v", v.Field(i).Interface())
			fmt.Printf(" %-*s |", widths[i], fieldValue)
		}
		fmt.Println()
		printSeparator(widths)
	}
}

func printSeparator(widths []int) {
	fmt.Print("+")
	for _, width := range widths {
		fmt.Print(strings.Repeat("-", width+2)) // +2 是为了两边的空格
		fmt.Print("+")
	}
	fmt.Println()
}

func printStruct(s interface{}) {
	v := reflect.ValueOf(s).Elem()
	t := v.Type()

	// 打印表头
	fmt.Println("+----------------+----------------+")
	fmt.Printf("| %-14s | %-14s |\n", "Field", "Value")
	fmt.Println("+----------------+----------------+")

	// 打印每个字段的名称和值
	for i := 0; i < v.NumField(); i++ {
		fieldName := t.Field(i).Name
		fieldValue := v.Field(i).Interface()
		fmt.Printf("| %-14s | %-14v |\n", fieldName, fieldValue)
	}

	fmt.Println("+----------------+----------------+")
}
func printTableWithColumns(values []interface{}, columns []string) {
	if len(values) == 0 || len(columns) == 0 {
		return
	}

	firstValue := reflect.ValueOf(values[0]).Elem()
	t := firstValue.Type()

	// 仅选择指定列
	selectedFields := make([]int, 0, len(columns))
	widths := make([]int, len(columns))
	notFields := make(map[int]string)
	for i, col := range columns {
		flag := false
		for j := 0; j < t.NumField(); j++ {
			if t.Field(j).Name == strings.Title(col) {
				flag = true
				selectedFields = append(selectedFields, j)
				widths[i] = len(col)
				break
			}
		}
		if !flag {
			notFields[i] = col
		}
	}
	for _, value := range values {
		v := reflect.ValueOf(value).Elem()
		for i, fieldIdx := range selectedFields {
			fieldValue := fmt.Sprintf("%v", v.Field(fieldIdx).Interface())
			if len(fieldValue) > widths[i] {
				widths[i] = len(fieldValue)
			}
		}
	}

	for k := 0; k < len(widths); k++ {
		if _, ok := notFields[k]; ok {
			widths = append(widths[:k], widths[k+1:]...)
		}
	}
	if len(notFields) != 0 {
		fmt.Printf("不存在的参数：[")
		for _, v := range notFields {
			fmt.Printf(" %s ", v)
		}
		fmt.Printf("]\n")
	}
	if len(notFields) == len(columns) {
		fmt.Printf("select语句错误，column数据没有找到,%v\n", columns)
		return
	}
	// fmt.Println(widths, len(widths), notFields)
	// 打印表头的分隔符
	printSeparator(widths)

	// 打印表头
	fmt.Print("|")
	for i, col := range columns {
		if _, ok := notFields[i]; ok {
			continue
		}
		fmt.Printf(" %-*s |", widths[i], col)
	}
	fmt.Println()

	// 打印表头下面的分隔符
	printSeparator(widths)

	// 打印每一行数据
	for _, value := range values {
		v := reflect.ValueOf(value).Elem()
		fmt.Print("|")
		for i, fieldIdx := range selectedFields {
			fieldValue := fmt.Sprintf("%v", v.Field(fieldIdx).Interface())
			fmt.Printf(" %-*s |", widths[i], fieldValue)
		}
		fmt.Println()
		printSeparator(widths)
	}
}
