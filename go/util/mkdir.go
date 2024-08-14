package util

import "os"

func MakeDirPath(dirPath string) error {
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
			return err
		}
		return nil
	}
	return nil
}

func RemoveDir(dirPath string) error {
	if err := os.RemoveAll(dirPath); err != nil {
		return err
	}
	return nil
}
