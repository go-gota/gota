package series

import (
	"fmt"
	"reflect"
	"strconv"
	"testing"
)

func TestSeries_Wrap_FloatApply(t *testing.T) {
	tests := []struct {
		series    Series
		addSeries Series
		subSeries Series
		addConst  float64
		expected  Series
	}{
		{
			Floats([]float64{1.5, -3.23, -0.33, -0.38, 1.6, 34.}),
			Floats([]float64{3, -6.46, -0.67, -0.76, 3.2, 68.}),
			Floats([]float64{1, -2, -3, -4, 3.2, 5.}),
			1,
			Floats([]float64{4.5, -6.69, 3, 3.86, 2.6, 98.}),
		},
		{
			Ints([]int{23, 13, 101, -6, -3}),
			Ints([]int{28, 18, 106, -5, 2}),
			Ints([]int{1, 2, 3, -4, 5}),
			2,
			Ints([]int{52, 31, 206, -5, -4}),
		},
	}

	for testnum, test := range tests {
		expected := test.expected
		received := test.series.Wrap(test.addSeries, test.subSeries).FloatApply(func(thisValue float64, wrapValues []float64) float64 {
			res := thisValue + wrapValues[0] - wrapValues[1] + test.addConst
			ret := formatFloat(res, "%.6f")
			return ret
		})

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

func TestSeries_Wrap_BoolApply(t *testing.T) {
	tests := []struct {
		series    Series
		andSeries Series
		orSeries  Series
		expected  Series
	}{
		{
			Bools([]bool{false, true, false, true, false, false}),
			Bools([]bool{true, false, false, true, true, false}),
			Bools([]bool{true, true, false, false, false, true}),
			Bools([]bool{true, true, false, true, false, true}),
		},
	}

	for testnum, test := range tests {
		expected := test.expected
		received := test.series.Wrap(test.andSeries, test.orSeries).BoolApply(func(thisValue bool, wrapValues []bool) bool {
			return thisValue && wrapValues[0] || wrapValues[1]
		})

		for i := 0; i < expected.Len(); i++ {
			if !reflect.DeepEqual(expected, received) {
				t.Errorf(
					"Test:%v\nExpected:\n%v\nReceived:\n%v",
					testnum, expected, received,
				)
			}
			if err := checkTypes(received); err != nil {
				t.Errorf(
					"Test:%v\nError:%v",
					testnum, err,
				)
			}
		}
	}

}

func formatFloat(f float64, format string) float64 {
	f1 := fmt.Sprintf(format, f)
	f2, _ := strconv.ParseFloat(f1, 64)
	return f2
}
