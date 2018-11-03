package utils

import "unsafe"

// 置换底层指针，len=cap 效率高，zero-copy
func Str2Bytes(s string) []byte{
	x := (*[2]uintptr)(unsafe.Pointer(&s))
	h := [3]uintptr{x[0],x[1],x[1]}
	return *(*[]byte)(unsafe.Pointer(&h))
}

// 置换底层指针，舍弃cap，效率高，zero-copy
func  Bytes2Str(b []byte) string{
	return *(*string)(unsafe.Pointer(&b))
}

