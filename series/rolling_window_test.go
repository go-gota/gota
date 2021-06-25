package series

import (
	"math"
	"strings"
	"testing"
)

func TestSeries_RollingMean(t *testing.T) {
	tests := []struct {
		window   int
		series   Series
		expected Series
	}{
		{
			3,
			Ints([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
			Floats([]float64{math.NaN(), math.NaN(), 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0}),
		},
		{
			2,
			Floats([]float64{1.0, 2.0, 3.0}),
			Floats([]float64{math.NaN(), 1.5, 2.5}),
		},
		{
			0,
			Floats([]float64{}),
			Floats([]float64{}),
		},
	}

	for testnum, test := range tests {
		expected := test.expected
		received := test.series.Rolling(test.window).Mean()

		for i := 0; i < expected.Len(); i++ {
			if strings.Compare(expected.Elem(i).String(),
				received.Elem(i).String()) != 0 {
				t.Errorf(
					"Test:%v\nExpected:\n%v\nReceived:\n%v",
					testnum, expected, received,
				)
			}
		}
	}
}

func TestSeries_RollingStdDev(t *testing.T) {
	tests := []struct {
		window   int
		series   Series
		expected Series
	}{
		{
			3,
			Ints([]int{5, 5, 6, 7, 5, 5, 5}),
			Floats([]float64{math.NaN(), math.NaN(), 0.5773502691896257, 1.0, 1.0, 1.1547005383792515, 0.0}),
		},
		{
			2,
			Floats([]float64{1.0, 2.0, 3.0}),
			Floats([]float64{math.NaN(), 0.7071067811865476, 0.7071067811865476}),
		},
		{
			0,
			Floats([]float64{}),
			Floats([]float64{}),
		},
	}

	for testnum, test := range tests {
		expected := test.expected
		received := test.series.Rolling(test.window).StdDev()

		for i := 0; i < expected.Len(); i++ {
			if strings.Compare(expected.Elem(i).String(),
				received.Elem(i).String()) != 0 {
				t.Errorf(
					"Test:%v\nExpected:\n%v\nReceived:\n%v",
					testnum, expected, received,
				)
			}
		}
	}
}
