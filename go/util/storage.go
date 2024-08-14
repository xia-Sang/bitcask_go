package util

import (
	"syscall"
	"unsafe"
)

// DiskUsage 结构体表示磁盘容量信息
type DiskUsage struct {
	Total uint64
	Free  uint64
}

// GetDiskUsage 获取当前文件夹所对应的磁盘容量 (Windows)
func GetDiskUsage(path string) (usage *DiskUsage, err error) {
	h := syscall.MustLoadDLL("kernel32.dll")
	c := h.MustFindProc("GetDiskFreeSpaceExW")

	lpFreeBytesAvailable := int64(0)
	lpTotalNumberOfBytes := int64(0)
	lpTotalNumberOfFreeBytes := int64(0)

	ret, _, err := c.Call(
		uintptr(unsafe.Pointer(syscall.StringToUTF16Ptr(path))),
		uintptr(unsafe.Pointer(&lpFreeBytesAvailable)),
		uintptr(unsafe.Pointer(&lpTotalNumberOfBytes)),
		uintptr(unsafe.Pointer(&lpTotalNumberOfFreeBytes)),
	)
	if ret == 0 {
		return nil, err
	}
	return &DiskUsage{
		Total: uint64(lpTotalNumberOfBytes),
		Free:  uint64(lpTotalNumberOfFreeBytes),
	}, nil
}
