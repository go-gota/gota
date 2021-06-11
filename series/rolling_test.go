package series

import (
	"reflect"
	"testing")


func TestSeries_Rolling(t *testing.T) {
	tests := []struct {
		series   Series
		window    int
		minPeriod int
		maxExpected Series
		minExpected Series
	}{
		{
			Bools([]string{"false", "true", "false", "false", "true"}),
			2,
			1,
			Bools([]string{"false", "true", "true", "false", "true"}),
			Bools([]string{"false", "false", "false", "false", "false"}),
		},
		{
			Floats([]string{"1.5", "-3.23", "-0.337397", "-0.380079", "1.60979", "34."}),
			3,
			2,
			Floats([]string{NaN, "1.5", "1.5", "-0.337397", "1.60979", "34."}),
			Floats([]string{NaN, "-3.23", "-3.23", "-3.23", "-0.380079", "-0.380079"}),
		},
		{
			Strings([]string{"20210618", "20200909", "20200910", "20200912","20200911"}),
			3,
			2,
			Strings([]string{NaN, "20210618", "20210618", "20200912", "20200912"}),
			Strings([]string{NaN, "20200909", "20200909", "20200909", "20200910"}),
		},
		{
			Ints([]string{"23", "13", "101", "-64", "-3"}),
			3,
			1,
			Ints([]string{"23", "23", "101", "101", "101"}),
			Ints([]string{"23", "13", "13", "-64", "-64"}),
		},
	}

	for testnum, test := range tests {
		expected := test.maxExpected.Records()
		b := test.series.Rolling(test.window, test.minPeriod).Max()
		received := b.Records()
		if !reflect.DeepEqual(expected, received) {
			t.Errorf(
				"Test-Max:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, expected, received,
			)
		}

		expected = test.minExpected.Records()
		b = test.series.Rolling(test.window, test.minPeriod).Min()
		received = b.Records()
		if !reflect.DeepEqual(expected, received) {
			t.Errorf(
				"Test-Min:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, expected, received,
			)
		}
	}
}