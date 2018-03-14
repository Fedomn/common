package testing

import (
	"fmt"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
)

func Assert(tb testing.TB, condition bool, msg string, v ...interface{}) {
	if !condition {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("%s:%d: "+msg+"\n\n", append([]interface{}{filepath.Base(file), line}, v...)...)
		tb.FailNow()
	}
}

func Equals(tb testing.TB, msg string, wat, got interface{}) {
	if !reflect.DeepEqual(wat, got) {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("%s:%d: %s \n\n\twat: %#v\n\n\tgot: %#v\n\n", filepath.Base(file), line, msg, wat, got)
		tb.FailNow()
	}
}

func Ok(tb testing.TB, msg string, err error) {
	if err != nil {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("%s:%d: %s \n\n unexpected error: %s\n\n", filepath.Base(file), line, msg, err.Error())
		tb.FailNow()
	}
}
