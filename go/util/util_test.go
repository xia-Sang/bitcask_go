package util

import "testing"

func TestGetDiskUsage(t *testing.T) {
	usage, err := GetDiskUsage("../")
	if err != nil {
		t.Errorf("GetDiskUsage error: %v", err)
	}
	t.Logf("usage: %v", usage)
}
