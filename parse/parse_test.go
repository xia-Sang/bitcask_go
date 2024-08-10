package parse

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParse(t *testing.T) {
	sql := "INSERT INTO my_table (id, name, balance, birthdate) VALUES (3, 'sang', 112.50, '19000401')"
	s := NewScanner()
	s.buffer = []byte(sql)
	s.length = len([]byte(sql))

	ast, err := s.ParseInsert()
	t.Log(ast)
	t.Log(err)
}
func TestParse1(t *testing.T) {
	sql := "SELECT  * FROM my_table where name = 'xia' "
	s := NewScanner()
	s.buffer = []byte(sql)
	s.length = len([]byte(sql))

	ast, err := s.ParseSelect()
	t.Log(ast)
	t.Log(err)
}
func TestParse4(t *testing.T) {
	sql := "SELECT (name,id,user) FROM my_table where name = 'xia' "
	// sql := "SELECT * FROM my_table where name = 'xia' "
	s := NewScanner()
	s.buffer = []byte(sql)
	s.length = len([]byte(sql))

	ast, err := s.ParseSelect()
	t.Log(ast)
	t.Log(err)
}
func TestParse2(t *testing.T) {
	// sql := `CREATE TABLE mytable (id INT PRIMARY KEY, name VARCHAR)`
	// sql := `CREATE TABLE mytable (id INT(11) PRIMARY KEY,age VARCHAR,name VARCHAR 255,balance DECIMAL)`
	// 	sql :=
	// 		`CREATE TABLE users (
	//     id INT AUTO_INCREMENT PRIMARY KEY,
	//     username VARCHAR(50) NOT NULL,
	//     email VARCHAR(100) NOT NULL,
	//     birthdate DATE,
	//     is_active BOOLEAN DEFAULT TRUE
	// )`
	// 	sql := `CREATE TABLE Persons
	// (
	// PersonID int,
	// LastName varchar(255),
	// FirstName varchar(255),
	// Address varchar(255),
	// City varchar(255)
	// )`
	// 	sql :=
	// 		`CREATE TABLE Person
	// (
	// LastName varchar,
	// FirstName varchar,
	// Address varchar,
	// Age int
	// ) `
	sql := `CREATE TABLE users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    email VARCHAR(100) NOT NULL,
    birthdate DATE,
    is_active BOOLEAN DEFAULT TRUE
)`
	s := NewScannerFromString(sql)
	s.buffer = []byte(sql)
	s.length = len([]byte(sql))
	ast, err := s.ParseCreate()
	assert.Nil(t, err)
	t.Log(ast)
}
