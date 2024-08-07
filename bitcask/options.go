package bitcask

import (
	util "bitcask/util"
	"errors"
	"fmt"
	"path"
	"strconv"
	"strings"
)

const (
	WalSuffix = ".wal"
)

var ErrorNotExist = errors.New("key not exist")

func getWalFileIndex(walFile string) int {
	rawIndex := strings.Replace(walFile, WalSuffix, "", -1)
	index, _ := strconv.Atoi(rawIndex)
	return index
}
func walFile(dirPath string, index uint32) string {
	return path.Join(dirPath, fmt.Sprintf("%09d%s", index, WalSuffix))
}

type Options struct {
	dirPath     string //配置文件
	maxWalSize  uint32 //sst size
	maxLevel    int    //最大等级
	maxLevelNum int    //每一层最多sst数量
	tableNum    int    // 一个sst 里面有block的个数
}

type Option func(*Options)

func WithMaxSSTSize(size uint32) Option {
	return func(o *Options) {
		o.maxWalSize = size
	}
}

func WithMaxLevel(level int) Option {
	return func(o *Options) {
		o.maxLevel = level
	}
}

func WithMaxLevelNum(num int) Option {
	return func(o *Options) {
		o.maxLevelNum = num
	}
}

func WithTableNum(num int) Option {
	return func(o *Options) {
		o.tableNum = num
	}
}
func (o *Options) defaultOptions() {
	if o.maxLevelNum <= 0 {
		o.maxLevelNum = 10
	}
	if o.tableNum <= 0 {
		o.tableNum = 10
	}
	if o.maxLevel <= 0 {
		o.maxLevel = 7
	}
	if o.maxWalSize <= 0 {
		o.maxWalSize = 1024
	}
}
func NewOptions(dirPath string, opts ...Option) (*Options, error) {
	options := &Options{dirPath: dirPath}

	for _, opt := range opts {
		opt(options)
	}

	options.defaultOptions()

	return options, options.check()
}
func (o *Options) check() error {
	if err := util.MakeDirPath(o.dirPath); err != nil {
		return err
	}

	return nil
}
