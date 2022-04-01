package series

import (
	"testing"
)

func TestSeries_When(t *testing.T) {
	tests := []struct {
		series     Series
		whenF      WhenFilterFunction
		whenApplyF WhenApplyFunction
		expected   Series
	}{
		{
			Floats([]float64{1.5, -3.23, -0.33, -0.38, 1.6, 34.}),
			func(ele Element, index int) bool {
				return index%2 == 0
			},
			func(newEle Element, index int) {
				newEle.SetFloat(formatFloat(newEle.Float()+1, "%.6f"))
			},
			Floats([]float64{2.5, -3.23, 0.67, -0.38, 2.6, 34.}),
		},
		{
			Ints([]int{23, 13, 101, -6, -3}),
			func(ele Element, index int) bool {
				v, _ := ele.Int()
				return v < 0
			},
			func(newEle Element, index int) {
				v, _ := newEle.Int()
				newEle.SetInt(v + 1)
			},
			Ints([]int{23, 13, 101, -5, -2}),
		},
	}

	for testnum, test := range tests {
		expected := test.expected
		received := test.series.When(test.whenF).Apply(test.whenApplyF)

		for i := 0; i < expected.Len(); i++ {
			if !compareFloats(expected.Elem(i).Float(),
				received.Elem(i).Float(), 6) {
				t.Errorf(
					"Test:%v\nExpected:\n%v\nReceived:\n%v",
					testnum, expected, received,
				)
			}
		}
	}

}
