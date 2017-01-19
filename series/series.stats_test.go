package series

import (
	"math"
	"testing"
)

func TestStats_Median(t *testing.T) {
	tests := []struct {
		serie    Series
		expected float64
	}{
		{Ints([]int{4, 5, 6, 7, 8, 9, 120}), 7},
		{Ints([]int{4, 5, 6, 7, 8, 1, 9, 120}), 6.5},
		{Ints([]int{4, 5, 6, 7, 8, 1, 2, 9, 120}), 6},
	}

	for nr, test := range tests {
		expected := test.expected
		received := test.serie.Median()

		if expected != received {
			t.Errorf(
				"Test:%v\nExpected:\n%v\nReceived:\n%v",
				nr, expected, received,
			)
		}
	}
}

func TestStats_Mean(t *testing.T) {
	tests := []struct {
		serie    Series
		expected float64
	}{
		{Ints([]int{6, 7}), 6.5},
		{Ints([]int{4, 5, 6, 7, 8, 1, 9, 120}), 20},
		{Ints([]int{4, 5, 6, 7, 8, 1, 2, 9, 120}), 18},
	}

	for nr, test := range tests {
		expected := test.expected
		received := test.serie.Mean()

		if expected != received {
			t.Errorf(
				"Test:%v\nExpected:\n%v\nReceived:\n%v",
				nr, expected, received,
			)
		}
	}
}

func TestStats_Max(t *testing.T) {
	maxInt := 9
	maxFloat := 9.
	expectedStr := "z"
	tests := []struct {
		series   Series
		expected Element
	}{
		{
			Ints([]int{0, 2, 1, 5, maxInt, 8}),
			intElement{e: &maxInt},
		},
		{
			Floats([]float64{-3., 2, 1, 5, maxFloat}),
			floatElement{e: &maxFloat},
		},
		{
			Floats([]float64{0.9, math.NaN(), 2, 1, 5, maxFloat}),
			floatElement{e: &maxFloat},
		},
		{
			Floats([]float64{math.NaN(), 2, 1, 5, maxFloat}),
			floatElement{e: &maxFloat},
		},
		{
			Strings([]string{expectedStr, "AB", "ABC", "B"}),
			stringElement{e: &expectedStr},
		},
		{
			Strings([]string{expectedStr, "", "ABC", "B"}),
			stringElement{e: &expectedStr},
		},
		{
			Strings([]string{expectedStr, "", "tABC", "B"}),
			stringElement{e: &expectedStr},
		},
	}

	for testn, test := range tests {
		expected := test.expected
		received, _ := Max(test.series)

		if !expected.Eq(received) {
			t.Errorf(
				"Test:%v\nExpected:\n%v\nReceived:\n%v",
				testn, expected, received,
			)
		}
	}

}
