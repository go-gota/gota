package series

import (
	"reflect"
	"testing"
)

func TestSeries_Logic(t *testing.T) {
	tests := []struct {
		series       Series
		another	interface{}
		andExpected  Series
		orExpected  Series
		notExpected  Series
	}{
		{
			Bools([]string{"false", "true", "false", "false", "true"}),
			"true",
			Bools([]string{"false", "true", "false", "false", "true"}),
			Bools([]string{"true", "true", "true", "true", "true"}),
			Bools([]string{"true", "false", "true", "true", "false"}),
		},
		{
			Bools([]string{"false", "true", "false", "false", "true"}),
			[]string	  {"true", "false", "true", "false", "false"},
			Bools([]string{"false", "false", "false", "false", "false"}),
			Bools([]string{"true", "true", "true", "false", "true"}),
			Bools([]string{"true", "false", "true", "true", "false"}),
		},
		{
			Bools([]string{"false", "true", "false", "false", "true"}),
			Bools([]string{"true", "false", "true", "false", "false"}),
			Bools([]string{"false", "false", "false", "false", "false"}),
			Bools([]string{"true", "true", "true", "false", "true"}),
			Bools([]string{"true", "false", "true", "true", "false"}),
		},
		{
			Bools([]string{"false", "true", "false", "false", "true"}),
			[]string	  {"1", "0", "1", "0", "0"},
			Bools([]string{"false", "false", "false", "false", "false"}),
			Bools([]string{"true", "true", "true", "false", "true"}),
			Bools([]string{"true", "false", "true", "true", "false"}),
		},
		{
			Bools([]string{"false", "true", "false", "false", "true"}),
			[]float64	  {1, 0, 1, 0, 0},
			Bools([]string{"false", "false", "false", "false", "false"}),
			Bools([]string{"true", "true", "true", "false", "true"}),
			Bools([]string{"true", "false", "true", "true", "false"}),
		},
		{
			Bools([]string{"false", "true", "false", "false", "true"}),
			[]int	  {1, 0, 1, 0, 0},
			Bools([]string{"false", "false", "false", "false", "false"}),
			Bools([]string{"true", "true", "true", "false", "true"}),
			Bools([]string{"true", "false", "true", "true", "false"}),
		},
		{
			Bools([]string{"false", "true", "false", "false", "123"}),
			[]int	  {7, 0, 1, 0, 0},
			Bools([]string{"NaN", "false", "false", "false", "NaN"}),
			Bools([]string{"NaN", "true", "true", "false", "NaN"}),
			Bools([]string{"true", "false", "true", "true", "NaN"}),
		},
	}

	for testnum, test := range tests {
		expected := test.andExpected.Records()
		b := test.series.And(test.another)
		received := b.Records()
		if !reflect.DeepEqual(expected, received) {
			t.Errorf(
				"Test-And:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, expected, received,
			)
		}

		expected = test.orExpected.Records()
		b = test.series.Or(test.another)
		received = b.Records()
		if !reflect.DeepEqual(expected, received) {
			t.Errorf(
				"Test-Or:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, expected, received,
			)
		}
		expected = test.notExpected.Records()
		b = test.series.Not()
		received = b.Records()
		if !reflect.DeepEqual(expected, received) {
			t.Errorf(
				"Test-Not:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, expected, received,
			)
		}
	}
}

