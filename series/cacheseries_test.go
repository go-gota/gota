package series

import (
	"fmt"
	"reflect"
	"testing"
)


func TestCacheSeries_Add(t *testing.T) {
	tests := []struct {
		series   Series
		addConst float64
		expected Series
	}{
		{
			Floats([]float64{1.5, -3.23, -0.33, -0.38, 1.6, 34.}),
			1,
			Floats([]float64{2.5, -2.23, 0.67, 0.62, 2.6, 35.}),
		},
		{
			Ints([]int{23, 13, 101, -6, -3}),
			2,
			Ints([]int{25, 15, 103, -4, -1}),
		},
	}

	for testnum, test := range tests {

		tmpSeries := test.series.CacheAble()

		expected := test.expected
		_ = tmpSeries.AddConst(test.addConst)

		received := tmpSeries.AddConst(test.addConst)

		exp := expected.Records()
		rev := received.Records()

		if !reflect.DeepEqual(exp, rev) {
			t.Errorf(
				"Test:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, exp, rev,
			)
		}
		fmt.Printf("testnum[%d] series state info:\n %s", testnum, tmpSeries.Str())
	}

}
