package lsm3

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xia-Sang/bitcask_go/parse"
)

func TestDb(t *testing.T) {
	c, err := NewContains()
	assert.Nil(t, err)
	t.Log(c)
	c.ShowTables()
	c.ShowColumns("User")
	c.ShowColumns("my_table")

	selectSql1 := "SELECT * FROM User"
	err = c.SelectTable(selectSql1)
	assert.Nil(t, err)

	selectSql2 := "SELECT * FROM my_table"
	err = c.SelectTable(selectSql2)
	assert.Nil(t, err)
}
func TestDb1(t *testing.T) {
	c, err := NewContains()
	assert.Nil(t, err)
	sql := `CREATE TABLE my_table (
		id INT(11),
		name VARCHAR(255),
		balance DECIMAL(10,2),
		birthdate DATE(2024/8/8)
	)`
	err = c.CreatTable(sql)
	assert.Nil(t, err)
	for i := range 100 {
		insertSql := fmt.Sprintf("INSERT INTO my_table (id, name, balance, birthdate) VALUES (%d, 'sang', %d.%d, '19000401')", i, i, i)
		err = c.InsertTable(insertSql)
	}
	assert.Nil(t, err)

	selectSql := "SELECT * FROM my_table where id = 1"
	err = c.SelectTable(selectSql)
	assert.Nil(t, err)

	sql1 := `CREATE TABLE User (
		id INT(11),
		name VARCHAR(255),
		balance DECIMAL(10,2),
		birthdate DATE(2024/8/8)
	)`
	err = c.CreatTable(sql1)
	assert.Nil(t, err)
	for i := range 10 {
		insertSql := fmt.Sprintf("INSERT INTO User (id, name, balance, birthdate) VALUES (%d, 'sang', %d.%d, '19000401')", i, i, i)
		err = c.InsertTable(insertSql)
	}
	assert.Nil(t, err)

	selectSql1 := "SELECT * FROM User where id = 1"
	err = c.SelectTable(selectSql1)
	assert.Nil(t, err)
	c.ShowTables()
	c.ShowColumns("User")
	c.ShowColumns("my_table")
	selectSql2 := "SELECT (id, name, balance) FROM User where id = 1"
	err = c.SelectTable(selectSql2)
	assert.Nil(t, err)
	c.Close()
}
func TestDb2(t *testing.T) {
	c, err := NewContains()
	assert.Nil(t, err)

	sql1 := `CREATE TABLE User (
		id INT(11),
		name VARCHAR(255),
		balance DECIMAL(10,2),
		birthdate DATE(2024/8/8)
	)`
	err = c.CreatTable(sql1)
	assert.Nil(t, err)
	for i := range 10 {
		insertSql := fmt.Sprintf("INSERT INTO User (id, name, balance, birthdate) VALUES (%d, 'sang', %d.%d, '19000401')", i, i, i)
		err = c.InsertTable(insertSql)
	}
	assert.Nil(t, err)

	selectSql1 := "SELECT * FROM User where id = 1"
	err = c.SelectTable(selectSql1)
	assert.Nil(t, err)
	c.ShowTables()
	c.ShowColumns("User")
	selectSql2 := "SELECT (id, name, balance,age) FROM User where id = 1"
	err = c.SelectTable(selectSql2)
	assert.Nil(t, err)
	selectSql3 := "SELECT (id1, name1, balance1,age) FROM User where id = 1"
	err = c.SelectTable(selectSql3)
	assert.Nil(t, err)
}
func TestDb4(t *testing.T) {
	c, err := NewContains()
	assert.Nil(t, err)

	sql1 := `CREATE TABLE User (
		id INT(11),
		name VARCHAR(255),
		balance DECIMAL(10,2),
		birthdate DATE(2024/8/8)
	)`
	err = c.CreatTable(sql1)
	assert.Nil(t, err)
	for i := range 10 {
		insertSql := fmt.Sprintf("INSERT INTO User (id, name, balance, birthdate) VALUES (%d, 'sang', %d.%d, '19000401')", i, i, i)
		err = c.InsertTable(insertSql)
	}
	assert.Nil(t, err)

	selectSql1 := "SELECT * FROM User where id = 1"
	err = c.SelectTable(selectSql1)
	assert.Nil(t, err)
	c.ShowTables()
	c.ShowColumns("User")
	selectSql2 := "SELECT (id, name, balance,age) FROM User where id = 1"
	err = c.SelectTable(selectSql2)
	assert.Nil(t, err)
	selectSql3 := "SELECT (id1, name1, balance1,age) FROM User where id = 1"
	err = c.SelectTable(selectSql3)
	assert.Nil(t, err)
	deleteSql3 := "DELETE FROM User where id >= 2"
	err = c.DeleteTable(deleteSql3)
	assert.Nil(t, err)

	selectSql4 := "SELECT * FROM User"
	err = c.SelectTable(selectSql4)
	assert.Nil(t, err)
}
func TestDb3(t *testing.T) {
	c, err := NewContains()
	assert.Nil(t, err)

	sql1 := `CREATE TABLE User (id INT(11),name VARCHAR(255),balance DECIMAL(10,2),birthdate DATE(2024/8/8))`
	err = c.CreatTable(sql1)
	assert.Nil(t, err)
	for i := range 5 {
		insertSql := fmt.Sprintf("INSERT INTO User (id, name, balance, birthdate) VALUES (%d, 'sang', %d.%d, '19000401')", i, i, i)
		err = c.InsertTable(insertSql)
	}
	assert.Nil(t, err)

	selectSql1 := "SELECT * FROM User where name = 'sang'"
	err = c.SelectTable(selectSql1)
	assert.Nil(t, err)
	c.ShowTables()
	selectSql2 := "SELECT (id, name,balance) FROM User where name = 'sang'"
	err = c.SelectTable(selectSql2)
	assert.Nil(t, err)
	c.ShowTables()
}
func TestCreate(t *testing.T) {
	columns := []parse.ColumnDefinition{
		{Name: "id", DataType: "string", Constraints: []string{"PRIMARY KEY"}},
		{Name: "name", DataType: "string", Constraints: []string{"NOT NULL"}},
		{Name: "age", DataType: "int", Constraints: []string{"NOT NULL"}},
		{Name: "balance", DataType: "float64", Constraints: []string{"DEFAULT 0.0"}},
		//{Name: "gender", DataType: "string", Constraints: []string{"DEFAULT 0.0"}},
	}

	// 定义要填充的值
	values := map[string]interface{}{
		"id":      "123",
		"name":    "Alice",
		"age":     30,
		"balance": 1000.50,
		"gender":  "male",
	}

	structType := createStructType(columns)
	t.Log(structType)
	// 创建结构体实例
	instance := reflect.New(structType).Interface()

	// 填充值
	res := fillStructValues(instance, columns, values)
	t.Log(res)

}
