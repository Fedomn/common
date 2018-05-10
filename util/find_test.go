package util_test

import (
	. "fedomn/common/testing"
	. "fedomn/common/util"
	"fmt"
	"testing"
)

func TestContains(t *testing.T) {
	var disTest = []struct {
		source interface{}
		find   interface{}
		wat    bool
	}{
		{[]string{"1", "2", "3"}, "1", true},
		{[]int{1, 2, 3}, 0, false},
		{1, 0, false},
	}

	for _, tt := range disTest {
		got := Contains(tt.source, tt.find)
		Equals(t, fmt.Sprintf("source: %+v", tt.source), tt.wat, got)
	}
}
