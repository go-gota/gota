package series

import (
	"reflect"
	"testing"
)

func TestSeries_Rolling(t *testing.T) {
	tests := []struct {
		series       Series
		window       int
		minPeriod    int
		maxExpected  Series
		minExpected  Series
		meanExpected Series
		quantile    float64
		quantileExpected Series
		medianExpected Series
		stdDevExpected Series
	}{
		{
			Bools([]string{"false", "true", "false", "false", "true"}),
			2,
			1,
			Bools([]string{"false", "true", "true", "false", "true"}),
			Bools([]string{"false", "false", "false", "false", "false"}),
			Floats([]string{"0.000000", "0.500000", "0.500000", "0.000000", "0.500000"}),
			0.8,
			Bools([]string{"false", "true", "true", "false", "true"}),
			Bools([]string{NaN, NaN, NaN, NaN, NaN}),
			Floats([]string{NaN, "0.707106781", "0.707106781", "0.000000", "0.707106781"}),
		},
		{
			Floats([]string{"1.5", "-3.23", "-0.337397", "-0.380079", "1.60979", "34."}),
			3,
			2,
			Floats([]string{NaN, "1.5", "1.5", "-0.337397", "1.60979", "34."}),
			Floats([]string{NaN, "-3.23", "-3.23", "-3.23", "-0.380079", "-0.380079"}),
			Floats([]string{NaN, "-0.865", "-0.689132333", "-1.315825333", "0.297438", "11.743237"}),
			0.7,
			Floats([]string{NaN, "1.500000", "1.500000", "-0.337397", "1.609790", "34.000000"}),
			Floats([]string{NaN, "-0.865", "-0.337397", "-0.380079", "-0.337397", "1.60979"}),
			Floats([]string{NaN, "3.344615075", "2.384536288", "1.657861251", "1.136730517", "19.30058339"}),
		},
		{
			Strings([]string{"20210618", "20200909", "20200910", "20200912", "20200911"}),
			3,
			2,
			Strings([]string{NaN, "20210618", "20210618", "20200912", "20200912"}),
			Strings([]string{NaN, "20200909", "20200909", "20200909", "20200910"}),
			Floats([]string{NaN, "20205763.500000", "20204145.666667", "20200910.333333", "20200911.000000"}),
			0.8,
			Strings([]string{NaN, "20210618.000000", "20210618.000000", "20200912.000000", "20200912.000000"}),
			Strings([]string{NaN, NaN, NaN, NaN, NaN}),
			Strings([]string{NaN, "6865.299739", "5605.205111", "1.527525", "1.000000"}),
		},
		{
			Ints([]string{"23", "13", "101", "-64", "-3"}),
			3,
			1,
			Ints([]string{"23", "23", "101", "101", "101"}),
			Ints([]string{"23", "13", "13", "-64", "-64"}),
			Floats([]string{"23.000000", "18.000000", "45.666667", "16.666667", "11.333333"}),
			0.8,
			Ints([]string{"23", "23", "101", "101", "101"}),
			Ints([]string{"23", "18", "23", "13", "-3"}),
			Floats([]string{NaN, "7.071067812", "48.18021724", "82.56108849", "83.4286122"}),
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

		expected = test.meanExpected.Records()
		b = test.series.Rolling(test.window, test.minPeriod).Mean()
		received = b.Records()
		if !reflect.DeepEqual(expected, received) {
			t.Errorf(
				"Test-Mean:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, expected, received,
			)
		}

		expected = test.quantileExpected.Records()
		b = test.series.Rolling(test.window, test.minPeriod).Quantile(test.quantile)
		received = b.Records()
		if !reflect.DeepEqual(expected, received) {
			t.Errorf(
				"Test-Quantile:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, expected, received,
			)
		}

		expected = test.medianExpected.Records()
		b = test.series.Rolling(test.window, test.minPeriod).Median()
		received = b.Records()
		if !reflect.DeepEqual(expected, received) {
			t.Errorf(
				"Test-Median:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, expected, received,
			)
		}

		expected = test.stdDevExpected.Records()
		b = test.series.Rolling(test.window, test.minPeriod).StdDev()
		received = b.Records()
		if !reflect.DeepEqual(expected, received) {
			t.Errorf(
				"Test-StdDev:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, expected, received,
			)
		}
	}
}


func TestSeries_MeanByWeights(t *testing.T) {
	tests := []struct {
		series       Series
		window       int
		minPeriod    int
		weights  []float64
		meanExpected Series
	}{
		{
			Floats([]string{"1.5", "-3.23", "-0.337397", "-0.380079", "1.60979", "34."}),
			3,
			2,
			[]float64{0.5, 0.3, 0.2},
			Floats([]string{NaN, "-0.392", "-0.2864794", "-1.7922349", "0.0392358", "7.0928975"}),
		},
		{
			Floats([]string{"23", "13", "101", "-64", "-3"}),
			3,
			1,
			[]float64{5, 3, 2},
			Floats([]string{"23", "19", "35.6", "24", "30.7"}),
		},
	}

	for testnum, test := range tests {
		expected := test.meanExpected.Records()
		b := test.series.Rolling(test.window, test.minPeriod).MeanByWeights(test.weights)
		received := b.Records()
		if !reflect.DeepEqual(expected, received) {
			t.Errorf(
				"Test-MeanByWeights:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, expected, received,
			)
		}
	}
}

func TestSeries_RollingApply(t *testing.T) {
	tests := []struct {
		series       Series
		window       int
		minPeriod    int
		applyExpected  Series
		applyFunc  func([]float64, []Element) interface{}
	}{
		{
			Floats([]string{"1.5", "-3.23", "-0.337397", "-0.380079", "1.60979", "34."}),
			3,
			2,
			Floats([]string{NaN, "2.5", "2.5", "-2.23", "0.662603", "0.619921"}),
			func(f []float64, e []Element) interface{} {
				return f[0] + 1
			},
		},
		{
			Strings([]string{"20210618", "20200909", "20200910", "20200912", "20200911"}),
			3,
			2,
			Strings([]string{NaN, "20210618-", "20210618-", "20200909-", "20200910-"}),
			func(f []float64, e []Element) interface{} {
				return e[0].String() + "-"
			},
		},
		{
			Ints([]string{"23", "13", "101", "-64", "-3"}),
			3,
			1,
			Ints([]string{"24", "14", "102", "-63", "-2"}),
			func(f []float64, e []Element) interface{} {
				return f[len(f)-1]+1
			},
		},
	}

	for testnum, test := range tests {
		expected := test.applyExpected.Records()
		b := test.series.Rolling(test.window, test.minPeriod).Apply(test.applyFunc)
		received := b.Records()
		if !reflect.DeepEqual(expected, received) {
			t.Errorf(
				"Test-Apply:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, expected, received,
			)
		}
	}
}

