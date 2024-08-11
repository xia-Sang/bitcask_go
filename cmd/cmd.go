package main

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"strings"

	lsm3 "github.com/xia-Sang/bitcask_go"
)

func main() {

	c, err := lsm3.NewContains()
	if err != nil {
		fmt.Println("Error creating Contains:", err)
		return
	}

	for {
		fmt.Print("sql> ")
		reader := bufio.NewReader(os.Stdin)
		input, err := scanData(reader)
		if err != nil {
			fmt.Println("Error reading input:", err)
			return
		}
		// fmt.Println("input", input)
		if strings.HasPrefix(input, ".") {
			if strings.ToLower(input) == ".showtables" {
				c.ShowTables()
				continue
			} else if strings.ToLower(input) == ".exit" {
				if err := c.Close(); err != nil {
					fmt.Println("Error closing Container:", err)
				} else {
					fmt.Println("Exiting...")
				}
				return
			} else if strings.HasPrefix(strings.ToLower(input), ".show") {
				parts := strings.Fields(input)
				if len(parts) != 2 {
					fmt.Println("Usage: .show <table_name>")
				} else {
					tableName := parts[1]
					c.ShowColumns(tableName)
				}
			} else {
				fmt.Println("Unknown command:", input)
			}
		} else if strings.HasSuffix(input, ";") {
			input = strings.TrimSuffix(input, ";")
			if strings.HasPrefix(strings.ToLower(input), "create table") {
				err = c.CreatTable(input)
				if err != nil {
					fmt.Println("Error creating table:", err)
				} else {
					fmt.Println("Table created successfully")
				}
			} else if strings.HasPrefix(strings.ToLower(input), "insert into") {
				err = c.InsertTable(input)
				if err != nil {
					fmt.Println("Error inserting into table:", err)
				} else {
					fmt.Println("Data inserted successfully")
				}
			} else if strings.HasPrefix(strings.ToLower(input), "select") {
				err = c.SelectTable(input)
				if err != nil {
					fmt.Println("Error selecting from table:", err)
				} else {
					fmt.Println("Data selected successfully")
				}
			} else if strings.HasPrefix(strings.ToLower(input), "delete from") {
				err = c.DeleteTable(input)
				if err != nil {
					fmt.Println("Error deleting from table:", err)
				} else {
					fmt.Println("Data deleted successfully")
				}
			} else {
				fmt.Println("Unknown SQL command:", input)
			}
		} else {
			fmt.Println("Invalid input. SQL statements should end with ';' or use '.' for commands.")
		}
	}
}

const MaxBufferSize = 512

// 扫描数据 获取数据输入
// 注意：stdin输入的处理并不是安全的
func scanData(reader *bufio.Reader) (string, error) {
	var data []byte
	firstChar, err := reader.ReadByte()
	if err != nil {
		return "", err // 如果到达输入末尾，结束循环
	}
	//检测是否为.命令 否则就是以;结尾的常规命令
	if firstChar == '.' {
		line, _, err := reader.ReadLine()
		if err != nil {
			return "", err // 如果读取失败，结束循环
		}
		data = append([]byte("."), line...)
	} else {
		text := bytes.NewBuffer(nil)
		text.WriteByte(firstChar)
		for {
			char, err := reader.ReadByte()
			if err != nil {
				os.Exit(1)
			}
			if char == ';' {
				text.WriteByte(char)
				break // 如果读取失败或者遇到分号，结束循环
			}
			text.WriteByte(char)
		}
		data = text.Bytes()
		text = nil
	}

	//超过最大长度 标记错误
	if len(data) > MaxBufferSize {
		return "", fmt.Errorf("超过最大长度 标记错误")
	}

	return string(data), nil
}
