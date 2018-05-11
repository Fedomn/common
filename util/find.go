package util

import "reflect"

type validator func(interface{}) bool

func Contains(source interface{}, find interface{}) bool {
	sourceVal := reflect.ValueOf(source)
	if sourceVal.Type().Kind() != reflect.Slice {
		return false
	}

	for i := 0; i < sourceVal.Len(); i++ {
		val := sourceVal.Index(i).Interface()
		if val == find {
			return true
		}
	}
	return false
}

func ContainsBy(source interface{}, fn validator) bool {
	sourceVal := reflect.ValueOf(source)
	if sourceVal.Type().Kind() != reflect.Slice {
		return false
	}

	for i := 0; i < sourceVal.Len(); i++ {
		val := sourceVal.Index(i).Interface()
		if match := fn(val); match == true {
			return true
		}
	}
	return false
}
