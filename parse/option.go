package parse

import "errors"

var (
	ErrorCommand        = errors.New("error command")
	ErrorBufferOverflow = errors.New("input exceeds buffer size")
)

const (
	MaxBufferSize = 512
)
