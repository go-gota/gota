package series_test

import (
	"reflect"
	"testing"

	"github.com/mengqingyan/gota/series"
)

func Test_Sub(t *testing.T) {
	tests := []struct {
		number series.Number
		s      series.Series
		expect series.Series
	}{
		{
			5,
			series.Floats([]string{series.NaN, "1.5", "1.5", "-0.3", "1.6", "34."}),
			series.Floats([]string{series.NaN, "3.5", "3.5", "5.3", "3.4", "-29"}),
		},
		{
			5,
			series.Ints([]string{series.NaN, "1", "2", "3", "4", "34"}),
			series.Ints([]string{series.NaN, "4", "3", "2", "1", "-29"}),
		},
	}
	for testnum, test := range tests {
		expected := test.expect.Records()
		b := test.number.Sub(test.s)
		received := b.Records()
		if !reflect.DeepEqual(expected, received) {
			t.Errorf(
				"Test-Sub:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, expected, received,
			)
		}
		if test.expect.Type() != b.Type() {
			t.Errorf(
				"Test-Sub-typeError:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, test.expect.Type(), b.Type(),
			)
		}
	}
}

func Test_Div(t *testing.T) {
	tests := []struct {
		number series.Number
		s      series.Series
		expect series.Series
	}{
		{
			5,
			series.Floats([]string{series.NaN, "1.5", "1.5", "-0.3", "1.6", "34."}),
			series.Floats([]string{series.NaN, "3.333333", "3.333333", "-16.666667", "3.125000", "0.147059"}),
		},
		{
			5,
			series.Ints([]string{series.NaN, "1", "2", "3", "4", "34"}),
			series.Ints([]string{series.NaN, "5", "2", "1", "1", "0"}),
		},
	}
	for testnum, test := range tests {
		expected := test.expect.Records()
		b := test.number.Div(test.s)
		received := b.Records()
		if !reflect.DeepEqual(expected, received) {
			t.Errorf(
				"Test-Div:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, expected, received,
			)
		}
		if test.expect.Type() != b.Type() {
			t.Errorf(
				"Test-Div-typeError:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, test.expect.Type(), b.Type(),
			)
		}
	}
}

func Test_Mod(t *testing.T) {
	tests := []struct {
		number series.Number
		s      series.Series
		expect series.Series
	}{
		{
			5,
			series.Floats([]string{series.NaN, "1.5", "1.5", "-0.3", "1.6", "34."}),
			series.Floats([]string{series.NaN, "0.500000", "0.500000", "0.200000", "0.200000", "5.000000"}),
		},
		{
			5,
			series.Ints([]string{series.NaN, "1", "2", "3", "4", "34"}),
			series.Ints([]string{series.NaN, "0", "1", "2", "1", "5"}),
		},
	}
	for testnum, test := range tests {
		expected := test.expect.Records()
		b := test.number.Mod(test.s)
		received := b.Records()
		if !reflect.DeepEqual(expected, received) {
			t.Errorf(
				"Test-Mod:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, expected, received,
			)
		}
		if test.expect.Type() != b.Type() {
			t.Errorf(
				"Test-Mod-typeError:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, test.expect.Type(), b.Type(),
			)
		}
	}
}
