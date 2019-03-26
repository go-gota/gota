package series

import (
	"math"
	"reflect"
	"testing"
)

func TestStats_Percentile(t *testing.T) {
	tests := []struct {
		s           Series
		percentile  float64
		expected    float64
		expectedErr error
	}{
		{Floats([]float64{43, 54, 56, 61, 62, 66}), 90, 66, nil},
		{Floats([]float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}), 50, 5.0, nil},
		{Floats([]float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}), 99.9, 10., nil},
		{Floats([]float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}), 100.0, 10., nil},
		{Floats([]float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}), 0.0, 1, ErrBounds},
		{Floats([]float64{}), 99.9, 0, ErrEmptyInput},

		{Floats([]float64{1, 2, 3, 4, 5}), 0.13, 1, nil},
		{Floats([]float64{1, 2, 3, 4, 5}), 101.0, 0, ErrBounds},
	}

	for _, test := range tests {
		m, err := test.s.Percentile(test.percentile)
		if err != nil && test.expectedErr == nil {
			t.Errorf("Excepted error is nil but got %v", err)
		}

		if test.expectedErr != nil && err == nil {
			t.Errorf("Excepted error %v but got nil", test.expectedErr)
		}

		if err != nil && test.expectedErr != nil && err != test.expectedErr {
			t.Errorf("Excepted error %v but got err %v", test.expectedErr, err)
		}

		if test.expectedErr == nil && err == nil {
			if m != test.expected {
				t.Errorf("Excepted values is %.1f but got %.1f", test.expected, m)
			}
		}
	}
}

func TestStats_Percentiles(t *testing.T) {
	tests := []struct {
		s           Series
		percentiles []float64
		excepted    []float64
	}{
		{Floats([]float64{-7.0, 10, 9, 8, 9, 13, 16, 17, 21, 3, 34, 26, 38, 21, 11, 2, 3, 9, 10, 20.0}),
			[]float64{37, 73}, []float64{9.0, 20.0}},
	}

	for nr, test := range tests {
		r, _ := test.s.Percentiles(test.percentiles...)

		if !reflect.DeepEqual(test.excepted, r) {
			t.Errorf(
				"Test:%v\nExpected:\n%v\nReceived:\n%v",
				nr, test.excepted, r,
			)
		}
	}
}
func TestStats_Outliers(t *testing.T) {
	tests := []struct {
		serie    Series
		expected []float64
	}{
		{Floats([]float64{-7.0, 10, 9, 8, 9, 9, 10, 20.0}), []float64{-7, 20}},
		{Floats([]float64{6.25, 10, 9, 8, 9, 9, 10, 11.0}), []float64{}},
	}

	for nr, test := range tests {
		expected := test.expected
		received := test.serie.Outliers()

		if len(expected) != len(received) {
			t.Errorf(
				"Test:%v\nExpected len:\n%v\nReceived len:\n%v",
				nr, len(expected), len(received),
			)
		}

		if len(received) > 0 && !reflect.DeepEqual(expected, received) {

			t.Errorf(
				"Test:%v\nExpected:\n%v\nReceived:\n%v",
				nr, expected, received,
			)
		}
	}
}

func TestStats_Median(t *testing.T) {
	tests := []struct {
		serie    Series
		expected float64
	}{
		{Ints([]int{4, 5, 6, 7, 8, 9, 120}), 7},
		{Ints([]int{4, 5, 6, 7, 8, 1, 9, 120}), 6.5},
		{Ints([]int{4, 5, 6, 7, 8, 1, 2, 9, 120}), 6},
		{Floats([]float64{math.NaN(), math.NaN()}), math.NaN()},
	}

	for nr, test := range tests {
		expected := test.expected
		received := test.serie.Median()
		if expected != received && (math.IsNaN(expected) && !math.IsNaN(received)) {
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
