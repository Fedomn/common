package util

import (
	"fmt"
	"reflect"
)

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

func FindBy(source interface{}, fn validator) (interface{}, error) {
	sliceVal := reflect.ValueOf(source)
	if sliceVal.Type().Kind() != reflect.Slice {
		return nil, fmt.Errorf("parameter 1 must be a slice")
	}

	for i := 0; i < sliceVal.Len(); i++ {
		val := sliceVal.Index(i).Interface()
		if match := fn(val); match == true {
			return val, nil
		}
	}
	return nil, nil
}
