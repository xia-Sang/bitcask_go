package util

import (
	"fmt"
	"math/rand"
	"time"
)

func GenerateRandomBytes(length int) []byte {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	bytes := make([]byte, length)
	for i := range bytes {
		bytes[i] = charset[rand.Intn(len(charset))]
	}
	return bytes
}
func GenerateKey(key int) []byte {
	return []byte(fmt.Sprintf("test-key-%09d", key))
}
func GenerateKeyString(key int) string {
	return string(GenerateKey(key))
}
func GenerateValueString(length int) string {
	return string(GenerateRandomBytes(length))
}

// 初始化随机数生成器的种子
func init() {
	rand.Seed(time.Now().UnixNano())
}

// RandomInts 生成一组随机整数
func RandomInts(n, max int) []int {
	if n <= 0 {
		panic("n should be greater than 0")
	}
	if max <= 0 {
		panic("max should be greater than 0")
	}
	nums := make([]int, n)
	for i := 0; i < n; i++ {
		nums[i] = rand.Intn(max)
	}
	return nums
}

// RandomIntsInRange 生成一组在 [min, max) 之间的随机整数
func RandomIntsInRange(n, min, max int) []int {
	if n <= 0 {
		panic("n should be greater than 0")
	}
	if min >= max {
		panic("min should be less than max")
	}
	nums := make([]int, n)
	for i := 0; i < n; i++ {
		nums[i] = rand.Intn(max-min) + min
	}
	return nums
}

// RandomFloats 生成一组随机浮点数
func RandomFloats(n int) []float64 {
	if n <= 0 {
		panic("n should be greater than 0")
	}
	nums := make([]float64, n)
	for i := 0; i < n; i++ {
		nums[i] = rand.Float64()
	}
	return nums
}

// RandomFloatsInRange 生成一组在 [min, max) 之间的随机浮点数
func RandomFloatsInRange(n int, min, max float64) []float64 {
	if n <= 0 {
		panic("n should be greater than 0")
	}
	if min >= max {
		panic("min should be less than max")
	}
	nums := make([]float64, n)
	for i := 0; i < n; i++ {
		nums[i] = min + rand.Float64()*(max-min)
	}
	return nums
}
