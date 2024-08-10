package parse

import (
	"bufio"
	"bytes"
	"strings"
)

// Scanner 定义一个扫描器
type Scanner struct {
	buffer []byte
	index  int
	length int
	line   int

	curr []byte //存储当前的token

	end  bool //标记是否结束
	flag bool //标记是否是行

	count int // 计数器
}

// NewScanner 生成一个最基础的扫描器
func NewScanner() *Scanner {
	return &Scanner{
		buffer: nil,
		index:  0,
		length: 0,
		line:   1,
		curr:   nil,
		end:    false,
		flag:   false,
	}
}
func NewScannerFromString(sql string) *Scanner {
	sql = strings.TrimSpace(sql)
	sql = strings.ReplaceAll(sql, "\n", " ")
	sql = strings.ReplaceAll(sql, "\t", " ")
	sql = strings.ReplaceAll(sql, "  ", " ")
	sql = strings.ReplaceAll(sql, " \r", " ")
	//fmt.Println(sql)
	return &Scanner{
		buffer: []byte(sql),
		index:  0,
		length: len([]byte(sql)),
		line:   1,
		curr:   nil,
		end:    false,
		flag:   false,
	}
}

// 一定是要进行更新的 不更新会出错的
func (s *Scanner) reset() {
	s.index = 0
	s.curr = []byte{}
	s.end = false
}

func (s *Scanner) check() bool {
	return s.count == 0
}
func (s *Scanner) scanData(reader *bufio.Reader) error {
	firstChar, err := reader.ReadByte()
	if err != nil {
		return err // handle EOF or other errors
	}

	if firstChar == '.' {
		line, _, err := reader.ReadLine()
		if err != nil {
			return err
		}
		s.buffer = line
	} else {
		text := bytes.Buffer{}
		text.WriteByte(firstChar)
		for {
			char, err := reader.ReadByte()
			if err != nil {
				return err // handle EOF or other errors
			}
			if char == ';' {
				break
			}
			text.WriteByte(char)
		}
		s.buffer = text.Bytes()
		s.flag = true
	}

	if len(s.buffer) > MaxBufferSize {
		return ErrorBufferOverflow
	}

	s.reset()
	s.length = len(s.buffer)
	s.line++
	return nil
}

func (s *Scanner) next() {
	s.skipWhitespace()
	if s.index >= s.length {
		s.end = true
		return
	}

	start := s.index
	switch s.buffer[s.index] {
	case ',', '(', ')':
		s.handleSpecialChars()
	default:
		s.handleRegularChars(start)
	}
}

func (s *Scanner) skipWhitespace() {
	for s.index < s.length && (s.buffer[s.index] == ' ' || s.buffer[s.index] == '\n' || s.buffer[s.index] == '\r') {
		s.index++
	}
}

func (s *Scanner) handleSpecialChars() {
	if s.buffer[s.index] == '(' {
		s.count++
	} else if s.buffer[s.index] == ')' {
		s.count--
	}
	s.curr = s.buffer[s.index : s.index+1]
	s.index++
}

func (s *Scanner) handleRegularChars(start int) {
	for s.index < s.length && !isSpecialChar(s.buffer[s.index]) {
		s.index++
	}
	s.curr = s.buffer[start:s.index]
}

func isSpecialChar(c byte) bool {
	return c == ' ' || c == '\n' || c == '\r' || c == ',' || c == '(' || c == ')'
}
