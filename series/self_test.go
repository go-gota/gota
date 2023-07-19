package series

import (
	"reflect"
	"testing"
)

func TestSeries_Self_Apply(t *testing.T) {
	tests := []struct {
		series   Series
		f        func(ele Element, index int)
		expected Series
	}{
		{
			Floats([]string{"1.5", "-3.23", "0.337397", "0.380079", "1.60979"}),
			func(ele Element, index int) {
				ele.SetFloat(ele.Float() + 2)
			},
			Floats([]string{"3.5", "-1.23", "2.337397", "2.380079", "3.60979"}),
		},
		{
			Ints([]string{"23", "13", "101", "-64", "-3"}),
			func(ele Element, index int) {
				ele.SetFloat(ele.Float() - 2)
			},
			Ints([]string{"21", "11", "99", "-66", "-5"}),
		},
	}

	for testnum, test := range tests {
		expected := test.expected.Records()
		b := test.series.Copy()
		b.Self().Apply(test.f)
		received := b.Records()
		if !reflect.DeepEqual(expected, received) {
			t.Errorf(
				"Test:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, expected, received,
			)
		}
	}
}