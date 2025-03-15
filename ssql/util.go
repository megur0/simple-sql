package ssql

import "strings"

func StrContainWithIgnoreCase(target string, str string) bool {
	return strings.Contains(strings.ToLower(target), strings.ToLower(str))
}

func StrContainListWithIgnoreCase(target string, str ...string) bool {
	for _, s := range str {
		if StrContainWithIgnoreCase(target, s) {
			return true
		}
	}
	return false
}

func Ptr[T any](a T) *T {
	return &a
}
