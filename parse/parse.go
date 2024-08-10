package parse

import (
	"fmt"
	"strconv"
	"strings"
)

// sql解析类型
type SqlType string

const (
	CREATE SqlType = "CREATE"
	SELECT SqlType = "SELECT"
	DELETE SqlType = "DELETE"
	INSERT SqlType = "INSERT"
)

// 其余相关常量
const (
	UNSUPPORTED = "N/A"
	FROM        = "FROM"
	WHERE       = "WHERE"
	LIMIT       = "LIMIT"
	INTO        = "INTO"
	VALUES      = "VALUES"
	ASTERISK    = "*"
	TABLE       = "TABLE"
)

type ColumnDefinition struct {
	Name        string
	DataType    string
	Constraints []string
}

type CreateTree struct {
	Table   string
	Columns []ColumnDefinition
}

func (c *CreateTree) Show() {
	fmt.Println(c.Table)
	for k, v := range c.Columns {
		fmt.Printf("(%d:%s:%s:%v)\n", k, v.Name, v.DataType, v.Constraints)
	}
}
func (s *Scanner) ParseCreate() (*CreateTree, error) {
	ast := &CreateTree{}

	// 检查CREATE关键字
	if s.next(); s.end || strings.ToUpper(string(s.curr)) != "CREATE" {
		return nil, fmt.Errorf("expected CREATE statement, found: %s", s.curr)
	}

	// 检查TABLE关键字
	if s.next(); s.end || strings.ToUpper(string(s.curr)) != TABLE {
		return nil, fmt.Errorf("expected TABLE after CREATE, found: %s", s.curr)
	}

	// 获取表名
	if s.next(); s.end {
		return nil, fmt.Errorf("expected table name after TABLE")
	}
	ast.Table = string(s.curr)

	// 解析字段定义
	if s.next(); s.end || string(s.curr) != "(" {
		return nil, fmt.Errorf("expected '(' after table name")
	}

	columns, err := s.parseColumns()
	if err != nil {
		return nil, err
	}
	ast.Columns = columns

	// 检查是否有多余的内容
	if s.next(); !s.end {
		return nil, fmt.Errorf("unexpected token after column definitions: %s", s.curr)
	}

	return ast, nil
}

func (s *Scanner) parseColumns() ([]ColumnDefinition, error) {
	var columns []ColumnDefinition

	for {
		if s.next(); s.end || string(s.curr) == ")" {
			return columns, nil
		}

		colDef, err := s.parseColumnDefinition()
		if err != nil {
			return nil, err
		}
		columns = append(columns, colDef)
		if s.end || string(s.curr) == ")" {
			return columns, nil
		} else if string(s.curr) != "," {
			return nil, fmt.Errorf("expected ',' or ')', found: %s", s.curr)
		}
	}
}

func (s *Scanner) parseColumnDefinition() (ColumnDefinition, error) {
	colDef := ColumnDefinition{}

	// 获取字段名称
	colDef.Name = string(s.curr)

	// 获取字段类型
	if s.next(); s.end {
		return colDef, fmt.Errorf("expected data type after column name: %s", colDef.Name)
	}
	colDef.DataType = string(s.curr)
	// 解析可能存在的字段约束
	if s.next(); !s.end {
		flag := false
		if string(s.curr) == "(" {
			// 继续往下走知道找到)
			temp := []string{}
			for s.next(); !s.end && string(s.curr) != ")"; s.next() {
				temp = append(temp, string(s.curr))
			}
			colDef.Constraints = append(colDef.Constraints, temp...)
			flag = true
		}
		if flag {
			for s.next(); !s.end && string(s.curr) != ")" && string(s.curr) != ","; s.next() {
				colDef.Constraints = append(colDef.Constraints, string(s.curr))
			}
		} else {
			for ; !s.end && string(s.curr) != ")" && string(s.curr) != ","; s.next() {
				colDef.Constraints = append(colDef.Constraints, string(s.curr))
			}
		}
	}
	return colDef, nil
}

// InsertTree insert解析
type InsertTree struct {
	Table   string     //table_name：需要插入新记录的表名
	Columns []string   //column1, column2, ...：需要插入的字段名
	Values  [][]string //value1, value2, ...：需要插入的字段值
}

// https://www.runoob.com/sql/sql-insert.html
/*
	INSERT INTO table_name
	VALUES (value1,value2,value3,...);

	INSERT INTO table_name (column1,column2,column3,...)
	VALUES (value1,value2,value3,...);
*/
func (s *Scanner) ParseInsert() (ast *InsertTree, err error) {
	ast = &InsertTree{}

	if s.next(); s.end || strings.ToUpper(string(s.curr)) != string(INSERT) {
		err = fmt.Errorf("%s is not INSERT statement,error token: %s", s.buffer, s.curr)
		return
	}
	// 解析 into
	if s.next(); s.end || strings.ToUpper(string(s.curr)) != INTO {
		err = fmt.Errorf("%s is not INSERT statement,error token: %s", s.buffer, s.curr)
		return
	}
	// 解析table name
	if s.next(); s.end {
		err = fmt.Errorf("%s expect table after INSERT INTO", s.buffer)
		return
	} else {
		ast.Table = string(s.curr)
	}
	// 解析column或者values
	if s.next(); s.end {
		err = fmt.Errorf("%s expect VALUES or (colNames),error token:%s", s.buffer, s.curr)
		return
	} else {
		currToken := strings.ToUpper(string(s.curr))
		if currToken == "(" {
			ast.Columns = make([]string, 0)
			for {
				if s.next(); s.end {
					if len(ast.Columns) == 0 {
						err = fmt.Errorf("%s get Columns failed", s.buffer)
					}
					return
				} else {
					currToken := string(s.curr)
					if currToken == "," {
						continue
					} else if currToken == ")" {
						break
					} else if strings.ToUpper(currToken) == VALUES {
						break
					} else {
						ast.Columns = append(ast.Columns, currToken)
					}
				}
			}
		} else if currToken != VALUES {
			err = fmt.Errorf("%s expect VALUES or '(' here,error token:%s", s.buffer, s.curr)
			return
		}
	}
	columnCount := len(ast.Columns)
	ast.Values = make([][]string, 0)

rawLoop:
	for {
		if s.next(); s.end {
			break rawLoop
		} else {
			currToken := string(s.curr)
			if currToken == "," {
				continue
			}
			if currToken == "(" {
				var row []string
				if columnCount != 0 {
					row = make([]string, 0, columnCount)
				} else {
					row = make([]string, 0)
				}
				for {
					if s.next(); s.end {
						break rawLoop
					} else {
						currToken := string(s.curr)
						if currToken == "," {
							continue
						} else if currToken == ")" {
							if columnCount != 0 && len(row) != columnCount {
								err = fmt.Errorf(
									"%s expected column count is %d, got %d, %v",
									s.buffer, columnCount, len(row), row,
								)
								return
							}
							ast.Values = append(ast.Values, row)
							if columnCount == 0 {
								columnCount = len(row)
							}
							break
						} else {
							row = append(row, currToken)
						}
					}
				}
			}
		}
	}
	return
}

/*
SelectTree 需要来实现select基础功能
* SELECT * FROM foo WHERE id < 3 LIMIT 1;
*/
type SelectTree struct {
	Projects []string
	Table    string   //table_name：要查询的表名称
	Where    []string //column1, column2, ...：要选择的字段名称，可以为多个字段。如果不指定字段名称，则会选择所有字段
	Limit    int64
}

func (s *Scanner) ParseSelect() (ast *SelectTree, err error) {
	ast = &SelectTree{}
	//不需要对于select进行处理
	if s.next(); s.end || strings.ToUpper(string(s.curr)) != string(SELECT) {
		err = fmt.Errorf("%s is not select statement,error token: %s", s.buffer, s.curr)
		return
	}
	// 直接处理 */project
	ast.Projects = make([]string, 0)
	for {
		if s.next(); s.end {
			if len(ast.Projects) == 0 {
				err = fmt.Errorf("%s get select projects failed", s.buffer)
			}
			return
		} else {
			// *
			if string(s.curr) == ASTERISK {
				ast.Projects = append(ast.Projects, ASTERISK)
				s.next()
				break
			}
			if string(s.curr) == "(" {
				for s.next(); !s.end && string(s.curr) != ")"; s.next() {
					if string(s.curr) == "," {
						continue
					} else {
						ast.Projects = append(ast.Projects, string(s.curr))
					}
				}
				if string(s.curr) == ")" {
					s.next()
					break
				}
			}
		}
	}
	if string(s.curr) != FROM {
		return nil, fmt.Errorf("from get failed")
	}
	// 获取到table
	if s.next(); s.end {
		return
	} else {
		ast.Table = string(s.curr)
	}
	// 获取到Where这个并不是必要的
	if s.next(); s.end {
		return
	}
	currToken := strings.ToUpper(string(s.curr))
	if currToken == WHERE {
		ast.Where = make([]string, 0)
		for {
			if s.next(); s.end {
				if len(ast.Where) == 0 {
					err = fmt.Errorf("missing WHERE clause")
				}
				return
			}
			currToken := string(s.curr)
			if strings.ToUpper(currToken) == LIMIT {
				break
			}
			ast.Where = append(ast.Where, currToken)
		}
	} else if currToken != LIMIT {
		err = fmt.Errorf("expect WHERE or LIMIT here")
		return
	}

	if s.next(); s.end {
		err = fmt.Errorf("expect LIMIT clause here")
		return
	}
	currToken = string(s.curr)
	ast.Limit, err = strconv.ParseInt(currToken, 10, 64)

	return
}

// DeleteTree 实现对于Delete的处理
type DeleteTree struct {
	Table string   //table_name：需要插入新记录的表名
	Where []string //column1, column2, ...：要选择的字段名称，可以为多个字段。如果不指定字段名称，则会选择所有字段
}

func (s *Scanner) ParseDelete() (ast *DeleteTree, err error) {
	ast = &DeleteTree{}
	// 不需要进行 解析 insert
	// 解析 into
	if s.next(); s.end || strings.ToUpper(string(s.curr)) != FROM {
		err = fmt.Errorf("%s is not DElETE statement,error token: %s", s.buffer, s.curr)
		return
	}
	// 解析table name
	if s.next(); s.end {
		err = fmt.Errorf("%s expect table after DELETE FROM", s.buffer)
		return
	} else {
		ast.Table = string(s.curr)
	}

	// 获取到Where这个并不是必要的
	if s.next(); s.end {
		return
	}
	currToken := strings.ToUpper(string(s.curr))
	if currToken == WHERE {
		ast.Where = make([]string, 0)
		for {
			if s.next(); s.end {
				if len(ast.Where) == 0 {
					err = fmt.Errorf("missing WHERE clause")
				}
				return
			}
			currToken := string(s.curr)
			if strings.ToUpper(currToken) == LIMIT {
				break
			}
			ast.Where = append(ast.Where, currToken)
		}
	}
	return
}
