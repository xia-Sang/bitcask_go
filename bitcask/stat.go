package bitcask

import (
	"fmt"
)

// Stat 统计信息
type Stat struct {
	WalFileSize uint32 // WAL 文件大小
	UselessSize uint32 // 无用数据大小
}

func (s *Stat) String() string {
	return fmt.Sprintf("WalFileSize: %d, UselessSize: %d", s.WalFileSize, s.UselessSize)
}
func NewStat() *Stat {
	return &Stat{}
}
