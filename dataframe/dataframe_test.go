package dataframe

import (
	"bytes"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"math"

	"github.com/go-gota/gota/series"
)

// compareFloats compares floating point values up to the number of digits specified.
// Returns true if both values are equal with the given precision
func compareFloats(lvalue, rvalue float64, digits int) bool {
	if math.IsNaN(lvalue) || math.IsNaN(rvalue) {
		return math.IsNaN(lvalue) && math.IsNaN(rvalue)
	}
	d := math.Pow(10.0, float64(digits))
	lv := int(lvalue * d)
	rv := int(rvalue * d)
	return lv == rv
}

func TestDataFrame_New(t *testing.T) {
	series := []series.Series{
		series.Strings([]int{1, 2, 3, 4, 5}),
		series.New([]int{1, 2, 3, 4, 5}, series.String, "0"),
		series.Ints([]int{1, 2, 3, 4, 5}),
		series.New([]int{1, 2, 3, 4, 5}, series.String, "0"),
		series.New([]int{1, 2, 3, 4, 5}, series.Float, "1"),
		series.New([]int{1, 2, 3, 4, 5}, series.Bool, "1"),
	}
	d := New(series...)

	// Check that the names are renamed properly
	received := d.Names()
	expected := []string{"X0", "0_0", "X1", "0_1", "1_0", "1_1"}
	if !reflect.DeepEqual(received, expected) {
		t.Errorf(
			"Expected:\n%v\nReceived:\n%v",
			expected, received,
		)
	}
}

func TestDataFrame_Copy(t *testing.T) {
	a := New(
		series.New([]string{"b", "a"}, series.String, "COL.1"),
		series.New([]int{1, 2}, series.Int, "COL.2"),
		series.New([]float64{3.0, 4.0}, series.Float, "COL.3"),
	)
	b := a.Copy()

	// Check that there are no shared memory addresses between DataFrames
	//if err := checkAddrDf(a, b); err != nil {
	//t.Error(err)
	//}
	// Check that the types are the same between both DataFrames
	if !reflect.DeepEqual(a.Types(), b.Types()) {
		t.Errorf("Different types:\nA:%v\nB:%v", a.Types(), b.Types())
	}
	// Check that the values are the same between both DataFrames
	if !reflect.DeepEqual(a.Records(), b.Records()) {
		t.Errorf("Different values:\nA:%v\nB:%v", a.Records(), b.Records())
	}
}

func TestDataFrame_Subset(t *testing.T) {
	a := New(
		series.New([]string{"b", "a", "b", "c", "d"}, series.String, "COL.1"),
		series.New([]int{1, 2, 4, 5, 4}, series.Int, "COL.2"),
		series.New([]float64{3.0, 4.0, 5.3, 3.2, 1.2}, series.Float, "COL.3"),
	)
	table := []struct {
		indexes interface{}
		expDf   DataFrame
	}{
		{
			[]int{1, 2},
			New(
				series.New([]string{"a", "b"}, series.String, "COL.1"),
				series.New([]int{2, 4}, series.Int, "COL.2"),
				series.New([]float64{4.0, 5.3}, series.Float, "COL.3"),
			),
		},
		{
			[]bool{false, true, true, false, false},
			New(
				series.New([]string{"a", "b"}, series.String, "COL.1"),
				series.New([]int{2, 4}, series.Int, "COL.2"),
				series.New([]float64{4.0, 5.3}, series.Float, "COL.3"),
			),
		},
		{
			series.Ints([]int{1, 2}),
			New(
				series.New([]string{"a", "b"}, series.String, "COL.1"),
				series.New([]int{2, 4}, series.Int, "COL.2"),
				series.New([]float64{4.0, 5.3}, series.Float, "COL.3"),
			),
		},
		{
			[]int{0, 0, 1, 1, 2, 2, 3, 4},
			New(
				series.New([]string{"b", "b", "a", "a", "b", "b", "c", "d"}, series.String, "COL.1"),
				series.New([]int{1, 1, 2, 2, 4, 4, 5, 4}, series.Int, "COL.2"),
				series.New([]float64{3.0, 3.0, 4.0, 4.0, 5.3, 5.3, 3.2, 1.2}, series.Float, "COL.3"),
			),
		},
	}

	for i, tc := range table {
		b := a.Subset(tc.indexes)

		if b.Err != nil {
			t.Errorf("Test: %d\nError:%v", i, b.Err)
		}
		//if err := checkAddrDf(a, b); err != nil {
		//t.Error(err)
		//}
		// Check that the types are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Types(), b.Types()) {
			t.Errorf("Test: %d\nDifferent types:\nA:%v\nB:%v", i, tc.expDf.Types(), b.Types())
		}
		// Check that the colnames are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Names(), b.Names()) {
			t.Errorf("Test: %d\nDifferent colnames:\nA:%v\nB:%v", i, tc.expDf.Names(), b.Names())
		}
		// Check that the values are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Records(), b.Records()) {
			t.Errorf("Test: %d\nDifferent values:\nA:%v\nB:%v", i, tc.expDf.Records(), b.Records())
		}
	}
}

func TestDataFrame_Select(t *testing.T) {
	a := New(
		series.New([]string{"b", "a", "b", "c", "d"}, series.String, "COL.1"),
		series.New([]int{1, 2, 4, 5, 4}, series.Int, "COL.2"),
		series.New([]float64{3.0, 4.0, 5.3, 3.2, 1.2}, series.Float, "COL.3"),
	)
	table := []struct {
		indexes interface{}
		expDf   DataFrame
	}{
		{
			series.Bools([]bool{false, true, true}),
			New(
				series.New([]int{1, 2, 4, 5, 4}, series.Int, "COL.2"),
				series.New([]float64{3.0, 4.0, 5.3, 3.2, 1.2}, series.Float, "COL.3"),
			),
		},
		{
			[]bool{false, true, true},
			New(
				series.New([]int{1, 2, 4, 5, 4}, series.Int, "COL.2"),
				series.New([]float64{3.0, 4.0, 5.3, 3.2, 1.2}, series.Float, "COL.3"),
			),
		},
		{
			series.Ints([]int{1, 2}),
			New(
				series.New([]int{1, 2, 4, 5, 4}, series.Int, "COL.2"),
				series.New([]float64{3.0, 4.0, 5.3, 3.2, 1.2}, series.Float, "COL.3"),
			),
		},
		{
			[]int{1, 2},
			New(
				series.New([]int{1, 2, 4, 5, 4}, series.Int, "COL.2"),
				series.New([]float64{3.0, 4.0, 5.3, 3.2, 1.2}, series.Float, "COL.3"),
			),
		},
		{
			[]int{1},
			New(
				series.New([]int{1, 2, 4, 5, 4}, series.Int, "COL.2"),
			),
		},
		{
			1,
			New(
				series.New([]int{1, 2, 4, 5, 4}, series.Int, "COL.2"),
			),
		},
		{
			[]int{1, 2, 0},
			New(
				series.New([]int{1, 2, 4, 5, 4}, series.Int, "COL.2"),
				series.New([]float64{3.0, 4.0, 5.3, 3.2, 1.2}, series.Float, "COL.3"),
				series.New([]string{"b", "a", "b", "c", "d"}, series.String, "COL.1"),
			),
		},
		{
			[]int{0, 0},
			New(
				series.New([]string{"b", "a", "b", "c", "d"}, series.String, "COL.1"),
				series.New([]string{"b", "a", "b", "c", "d"}, series.String, "COL.1"),
			),
		},
		{
			"COL.3",
			New(
				series.New([]float64{3.0, 4.0, 5.3, 3.2, 1.2}, series.Float, "COL.3"),
			),
		},
		{
			[]string{"COL.3"},
			New(
				series.New([]float64{3.0, 4.0, 5.3, 3.2, 1.2}, series.Float, "COL.3"),
			),
		},
		{
			[]string{"COL.3", "COL.1"},
			New(
				series.New([]float64{3.0, 4.0, 5.3, 3.2, 1.2}, series.Float, "COL.3"),
				series.New([]string{"b", "a", "b", "c", "d"}, series.String, "COL.1"),
			),
		},
		{
			series.Strings([]string{"COL.3", "COL.1"}),
			New(
				series.New([]float64{3.0, 4.0, 5.3, 3.2, 1.2}, series.Float, "COL.3"),
				series.New([]string{"b", "a", "b", "c", "d"}, series.String, "COL.1"),
			),
		},
	}

	for i, tc := range table {
		b := a.Select(tc.indexes)

		if b.Err != nil {
			t.Errorf("Test: %d\nError:%v", i, b.Err)
		}
		//if err := checkAddrDf(a, b); err != nil {
		//t.Error(err)
		//}
		// Check that the types are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Types(), b.Types()) {
			t.Errorf("Test: %d\nDifferent types:\nA:%v\nB:%v", i, tc.expDf.Types(), b.Types())
		}
		// Check that the colnames are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Names(), b.Names()) {
			t.Errorf("Test: %d\nDifferent colnames:\nA:%v\nB:%v", i, tc.expDf.Names(), b.Names())
		}
		// Check that the values are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Records(), b.Records()) {
			t.Errorf("Test: %d\nDifferent values:\nA:%v\nB:%v", i, tc.expDf.Records(), b.Records())
		}
	}
}

func TestDataFrame_Drop(t *testing.T) {
	a := New(
		series.New([]string{"b", "a", "b", "c", "d"}, series.String, "COL.1"),
		series.New([]int{1, 2, 4, 5, 4}, series.Int, "COL.2"),
		series.New([]float64{3.0, 4.0, 5.3, 3.2, 1.2}, series.Float, "COL.3"),
	)
	table := []struct {
		indexes interface{}
		expDf   DataFrame
	}{
		{
			series.Bools([]bool{false, true, true}),
			New(
				series.New([]string{"b", "a", "b", "c", "d"}, series.String, "COL.1"),
			),
		},
		{
			[]bool{false, true, true},
			New(
				series.New([]string{"b", "a", "b", "c", "d"}, series.String, "COL.1"),
			),
		},
		{
			series.Ints([]int{1, 2}),
			New(
				series.New([]string{"b", "a", "b", "c", "d"}, series.String, "COL.1"),
			),
		},
		{
			[]int{1, 2},
			New(
				series.New([]string{"b", "a", "b", "c", "d"}, series.String, "COL.1"),
			),
		},
		{
			[]int{1},
			New(
				series.New([]string{"b", "a", "b", "c", "d"}, series.String, "COL.1"),
				series.New([]float64{3.0, 4.0, 5.3, 3.2, 1.2}, series.Float, "COL.3"),
			),
		},
		{
			1,
			New(
				series.New([]string{"b", "a", "b", "c", "d"}, series.String, "COL.1"),
				series.New([]float64{3.0, 4.0, 5.3, 3.2, 1.2}, series.Float, "COL.3"),
			),
		},
		{
			[]int{0, 0},
			New(
				series.New([]int{1, 2, 4, 5, 4}, series.Int, "COL.2"),
				series.New([]float64{3.0, 4.0, 5.3, 3.2, 1.2}, series.Float, "COL.3"),
			),
		},
		{
			"COL.3",
			New(
				series.New([]string{"b", "a", "b", "c", "d"}, series.String, "COL.1"),
				series.New([]int{1, 2, 4, 5, 4}, series.Int, "COL.2"),
			),
		},
		{
			[]string{"COL.3"},
			New(
				series.New([]string{"b", "a", "b", "c", "d"}, series.String, "COL.1"),
				series.New([]int{1, 2, 4, 5, 4}, series.Int, "COL.2"),
			),
		},
		{
			[]string{"COL.3", "COL.1"},
			New(
				series.New([]int{1, 2, 4, 5, 4}, series.Int, "COL.2"),
			),
		},
		{
			series.Strings([]string{"COL.3", "COL.1"}),
			New(
				series.New([]int{1, 2, 4, 5, 4}, series.Int, "COL.2"),
			),
		},
	}

	for i, tc := range table {
		b := a.Drop(tc.indexes)

		if b.Err != nil {
			t.Errorf("Test: %d\nError:%v", i, b.Err)
		}
		// Check that the types are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Types(), b.Types()) {
			t.Errorf("Test: %d\nDifferent types:\nA:%v\nB:%v", i, tc.expDf.Types(), b.Types())
		}
		// Check that the colnames are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Names(), b.Names()) {
			t.Errorf("Test: %d\nDifferent colnames:\nA:%v\nB:%v", i, tc.expDf.Names(), b.Names())
		}
		// Check that the values are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Records(), b.Records()) {
			t.Errorf("Test: %d\nDifferent values:\nA:%v\nB:%v", i, tc.expDf.Records(), b.Records())
		}
	}
}

func TestDataFrame_Rename(t *testing.T) {
	a := New(
		series.New([]string{"b", "a", "b", "c", "d"}, series.String, "COL.1"),
		series.New([]int{1, 2, 4, 5, 4}, series.Int, "COL.2"),
		series.New([]float64{3.0, 4.0, 5.3, 3.2, 1.2}, series.Float, "COL.3"),
	)
	table := []struct {
		newname string
		oldname string
		expDf   DataFrame
	}{
		{
			"NEWCOL.1",
			"COL.1",
			New(
				series.New([]string{"b", "a", "b", "c", "d"}, series.String, "NEWCOL.1"),
				series.New([]int{1, 2, 4, 5, 4}, series.Int, "COL.2"),
				series.New([]float64{3.0, 4.0, 5.3, 3.2, 1.2}, series.Float, "COL.3"),
			),
		},
		{
			"NEWCOL.3",
			"COL.3",
			New(
				series.New([]string{"b", "a", "b", "c", "d"}, series.String, "COL.1"),
				series.New([]int{1, 2, 4, 5, 4}, series.Int, "COL.2"),
				series.New([]float64{3.0, 4.0, 5.3, 3.2, 1.2}, series.Float, "NEWCOL.3"),
			),
		},
		{
			"NEWCOL.2",
			"COL.2",
			New(
				series.New([]string{"b", "a", "b", "c", "d"}, series.String, "COL.1"),
				series.New([]int{1, 2, 4, 5, 4}, series.Int, "NEWCOL.2"),
				series.New([]float64{3.0, 4.0, 5.3, 3.2, 1.2}, series.Float, "COL.3"),
			),
		},
	}
	for i, tc := range table {
		b := a.Rename(tc.newname, tc.oldname)

		if b.Err != nil {
			t.Errorf("Test: %d\nError:%v", i, b.Err)
		}
		//if err := checkAddrDf(a, b); err != nil {
		//t.Error(err)
		//}
		// Check that the types are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Types(), b.Types()) {
			t.Errorf("Test: %d\nDifferent types:\nA:%v\nB:%v", i, tc.expDf.Types(), b.Types())
		}
		// Check that the colnames are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Names(), b.Names()) {
			t.Errorf("Test: %d\nDifferent colnames:\nA:%v\nB:%v", i, tc.expDf.Names(), b.Names())
		}
		// Check that the values are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Records(), b.Records()) {
			t.Errorf("Test: %d\nDifferent values:\nA:%v\nB:%v", i, tc.expDf.Records(), b.Records())
		}
	}
}

func TestDataFrame_CBind(t *testing.T) {
	a := New(
		series.New([]string{"b", "a", "b", "c", "d"}, series.String, "COL.1"),
		series.New([]int{1, 2, 4, 5, 4}, series.Int, "COL.2"),
		series.New([]float64{3.0, 4.0, 5.3, 3.2, 1.2}, series.Float, "COL.3"),
	)
	table := []struct {
		dfb   DataFrame
		expDf DataFrame
	}{
		{
			New(
				series.New([]string{"b", "a", "b", "c", "d"}, series.String, "COL.4"),
				series.New([]int{1, 2, 4, 5, 4}, series.Int, "COL.5"),
				series.New([]float64{3.0, 4.0, 5.3, 3.2, 1.2}, series.Float, "COL.6"),
			),
			New(
				series.New([]string{"b", "a", "b", "c", "d"}, series.String, "COL.1"),
				series.New([]int{1, 2, 4, 5, 4}, series.Int, "COL.2"),
				series.New([]float64{3.0, 4.0, 5.3, 3.2, 1.2}, series.Float, "COL.3"),
				series.New([]string{"b", "a", "b", "c", "d"}, series.String, "COL.4"),
				series.New([]int{1, 2, 4, 5, 4}, series.Int, "COL.5"),
				series.New([]float64{3.0, 4.0, 5.3, 3.2, 1.2}, series.Float, "COL.6"),
			),
		},
		{
			New(
				series.New([]string{"b", "a", "b", "c", "d"}, series.String, "COL.4"),
			),
			New(
				series.New([]string{"b", "a", "b", "c", "d"}, series.String, "COL.1"),
				series.New([]int{1, 2, 4, 5, 4}, series.Int, "COL.2"),
				series.New([]float64{3.0, 4.0, 5.3, 3.2, 1.2}, series.Float, "COL.3"),
				series.New([]string{"b", "a", "b", "c", "d"}, series.String, "COL.4"),
			),
		},
		{
			New(
				series.New([]string{"b", "a", "b", "c", "d"}, series.String, "COL.4"),
				series.New([]float64{3.0, 4.0, 5.3, 3.2, 1.2}, series.Float, "COL.6"),
			),
			New(
				series.New([]string{"b", "a", "b", "c", "d"}, series.String, "COL.1"),
				series.New([]int{1, 2, 4, 5, 4}, series.Int, "COL.2"),
				series.New([]float64{3.0, 4.0, 5.3, 3.2, 1.2}, series.Float, "COL.3"),
				series.New([]string{"b", "a", "b", "c", "d"}, series.String, "COL.4"),
				series.New([]float64{3.0, 4.0, 5.3, 3.2, 1.2}, series.Float, "COL.6"),
			),
		},
	}
	for i, tc := range table {
		b := a.CBind(tc.dfb)

		if b.Err != nil {
			t.Errorf("Test: %d\nError:%v", i, b.Err)
		}
		//if err := checkAddrDf(a, b); err != nil {
		//t.Error(err)
		//}
		// Check that the types are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Types(), b.Types()) {
			t.Errorf("Test: %d\nDifferent types:\nA:%v\nB:%v", i, tc.expDf.Types(), b.Types())
		}
		// Check that the colnames are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Names(), b.Names()) {
			t.Errorf("Test: %d\nDifferent colnames:\nA:%v\nB:%v", i, tc.expDf.Names(), b.Names())
		}
		// Check that the values are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Records(), b.Records()) {
			t.Errorf("Test: %d\nDifferent values:\nA:%v\nB:%v", i, tc.expDf.Records(), b.Records())
		}
	}
}

func TestDataFrame_RBind(t *testing.T) {
	a := New(
		series.New([]string{"b", "a", "b", "c", "d"}, series.String, "COL.1"),
		series.New([]int{1, 2, 4, 5, 4}, series.Int, "COL.2"),
		series.New([]float64{3.0, 4.0, 5.3, 3.2, 1.2}, series.Float, "COL.3"),
	)
	table := []struct {
		dfb   DataFrame
		expDf DataFrame
	}{
		{
			New(
				series.New([]string{"b", "a", "b", "c", "d"}, series.String, "COL.1"),
				series.New([]int{1, 2, 4, 5, 4}, series.Int, "COL.2"),
				series.New([]float64{3.0, 4.0, 5.3, 3.2, 1.2}, series.Float, "COL.3"),
			),
			New(
				series.New([]string{"b", "a", "b", "c", "d", "b", "a", "b", "c", "d"}, series.String, "COL.1"),
				series.New([]int{1, 2, 4, 5, 4, 1, 2, 4, 5, 4}, series.Int, "COL.2"),
				series.New([]float64{3.0, 4.0, 5.3, 3.2, 1.2, 3.0, 4.0, 5.3, 3.2, 1.2}, series.Float, "COL.3"),
			),
		},
		{
			New(
				series.New([]int{1, 2, 4, 5, 4}, series.Int, "COL.1"),
				series.New([]int{1, 2, 4, 5, 4}, series.Int, "COL.2"),
				series.New([]float64{3.0, 4.0, 5.3, 3.2, 1.2}, series.Float, "COL.3"),
			),
			New(
				series.New([]string{"b", "a", "b", "c", "d", "1", "2", "4", "5", "4"}, series.String, "COL.1"),
				series.New([]int{1, 2, 4, 5, 4, 1, 2, 4, 5, 4}, series.Int, "COL.2"),
				series.New([]float64{3.0, 4.0, 5.3, 3.2, 1.2, 3.0, 4.0, 5.3, 3.2, 1.2}, series.Float, "COL.3"),
			),
		},
	}
	for i, tc := range table {
		b := a.RBind(tc.dfb)

		if b.Err != nil {
			t.Errorf("Test: %d\nError:%v", i, b.Err)
		}
		//if err := checkAddrDf(a, b); err != nil {
		//t.Error(err)
		//}
		// Check that the types are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Types(), b.Types()) {
			t.Errorf("Test: %d\nDifferent types:\nA:%v\nB:%v", i, tc.expDf.Types(), b.Types())
		}
		// Check that the colnames are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Names(), b.Names()) {
			t.Errorf("Test: %d\nDifferent colnames:\nA:%v\nB:%v", i, tc.expDf.Names(), b.Names())
		}
		// Check that the values are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Records(), b.Records()) {
			t.Errorf("Test: %d\nDifferent values:\nA:%v\nB:%v", i, tc.expDf.Records(), b.Records())
		}
	}
}

func TestDataFrame_Concat(t *testing.T) {
	type NA struct{}

	a := New(
		series.New([]string{"b", "a", "b", "c", "d"}, series.String, "COL.1"),
		series.New([]int{1, 2, 4, 5, 4}, series.Int, "COL.2"),
		series.New([]float64{3.0, 4.0, 5.3, 3.2, 1.2}, series.Float, "COL.3"),
	)
	table := []struct {
		dfa   DataFrame
		dfb   DataFrame
		expDf DataFrame
	}{
		{
			a,
			New(
				series.New([]string{"b", "a", "b", "c", "d"}, series.String, "COL.1"),
				series.New([]int{1, 2, 4, 5, 4}, series.Int, "COL.2"),
				series.New([]float64{3.0, 4.0, 5.3, 3.2, 1.2}, series.Float, "COL.3"),
			),
			New(
				series.New([]string{"b", "a", "b", "c", "d", "b", "a", "b", "c", "d"}, series.String, "COL.1"),
				series.New([]int{1, 2, 4, 5, 4, 1, 2, 4, 5, 4}, series.Int, "COL.2"),
				series.New([]float64{3.0, 4.0, 5.3, 3.2, 1.2, 3.0, 4.0, 5.3, 3.2, 1.2}, series.Float, "COL.3"),
			),
		},
		{
			a,
			New(
				series.New([]int{1, 2, 4, 5, 4}, series.Int, "COL.1"),
				series.New([]int{1, 2, 4, 5, 4}, series.Int, "COL.2"),
				series.New([]float64{3.0, 4.0, 5.3, 3.2, 1.2}, series.Float, "COL.3"),
			),
			New(
				series.New([]string{"b", "a", "b", "c", "d", "1", "2", "4", "5", "4"}, series.String, "COL.1"),
				series.New([]int{1, 2, 4, 5, 4, 1, 2, 4, 5, 4}, series.Int, "COL.2"),
				series.New([]float64{3.0, 4.0, 5.3, 3.2, 1.2, 3.0, 4.0, 5.3, 3.2, 1.2}, series.Float, "COL.3"),
			),
		},

		{
			a,
			New(
				series.New([]string{"b", "a", "b", "c", "d"}, series.String, "COL.1"),
				series.New([]float64{3.0, 4.0, 5.3, 3.2, 1.2}, series.Float, "COL.3"),
			),
			New(
				series.New([]string{"b", "a", "b", "c", "d", "b", "a", "b", "c", "d"}, series.String, "COL.1"),
				series.New([]int{1, 2, 4, 5, 4}, series.Int, "COL.2").Concat(series.New([]NA{NA{}, NA{}, NA{}, NA{}, NA{}}, series.Int, "")),
				series.New([]float64{3.0, 4.0, 5.3, 3.2, 1.2, 3.0, 4.0, 5.3, 3.2, 1.2}, series.Float, "COL.3"),
			),
		},
		{
			a,
			New(
				series.New([]int{1, 2, 4, 5, 4}, series.Int, "COL.1"),
				series.New([]int{1, 2, 4, 5, 4}, series.Int, "COL.2"),
				series.New([]float64{3.0, 4.0, 5.3, 3.2, 1.2}, series.Float, "COL.3"),
				series.New([]string{"a", "b", "c", "d", "e"}, series.String, "COL.4"),
			),
			New(
				series.New([]string{"b", "a", "b", "c", "d", "1", "2", "4", "5", "4"}, series.String, "COL.1"),
				series.New([]int{1, 2, 4, 5, 4, 1, 2, 4, 5, 4}, series.Int, "COL.2"),
				series.New([]float64{3.0, 4.0, 5.3, 3.2, 1.2, 3.0, 4.0, 5.3, 3.2, 1.2}, series.Float, "COL.3"),
				series.New([]NA{NA{}, NA{}, NA{}, NA{}, NA{}}, series.String, "COL.4").Concat(series.New([]string{"a", "b", "c", "d", "e"}, series.String, "COL.4")),
			),
		},
		{
			a,
			New(
				series.New([]string{"a", "b", "c", "d", "e"}, series.String, "COL.0"),
				series.New([]int{1, 2, 4, 5, 4}, series.Int, "COL.1"),
				series.New([]int{1, 2, 4, 5, 4}, series.Int, "COL.2"),
				series.New([]float64{3.0, 4.0, 5.3, 3.2, 1.2}, series.Float, "COL.3"),
			),
			New(
				series.New([]string{"b", "a", "b", "c", "d", "1", "2", "4", "5", "4"}, series.String, "COL.1"),
				series.New([]int{1, 2, 4, 5, 4, 1, 2, 4, 5, 4}, series.Int, "COL.2"),
				series.New([]float64{3.0, 4.0, 5.3, 3.2, 1.2, 3.0, 4.0, 5.3, 3.2, 1.2}, series.Float, "COL.3"),
				series.New([]NA{NA{}, NA{}, NA{}, NA{}, NA{}}, series.String, "COL.0").Concat(series.New([]string{"a", "b", "c", "d", "e"}, series.String, "COL.0")),
			),
		},
		{
			DataFrame{},
			a,
			a,
		},
	}
	for i, tc := range table {
		b := tc.dfa.Concat(tc.dfb)

		if b.Err != nil {
			t.Errorf("Test: %d\nError:%v", i, b.Err)
		}
		//if err := checkAddrDf(a, b); err != nil {
		//t.Error(err)
		//}
		// Check that the types are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Types(), b.Types()) {
			t.Errorf("Test: %d\nDifferent types:\nA:%v\nB:%v", i, tc.expDf.Types(), b.Types())
		}
		// Check that the colnames are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Names(), b.Names()) {
			t.Errorf("Test: %d\nDifferent colnames:\nA:%v\nB:%v", i, tc.expDf.Names(), b.Names())
		}
		// Check that the values are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Records(), b.Records()) {
			t.Errorf("Test: %d\nDifferent values:\nA:%v\nB:%v", i, tc.expDf.Records(), b.Records())
		}
	}
}
func TestDataFrame_Records(t *testing.T) {
	a := New(
		series.New([]string{"a", "b", "c"}, series.String, "COL.1"),
		series.New([]int{1, 2, 3}, series.Int, "COL.2"),
		series.New([]float64{3, 2, 1}, series.Float, "COL.3"))
	expected := [][]string{
		{"COL.1", "COL.2", "COL.3"},
		{"a", "1", "3.000000"},
		{"b", "2", "2.000000"},
		{"c", "3", "1.000000"},
	}
	received := a.Records()
	if !reflect.DeepEqual(expected, received) {
		t.Error(
			"Error when saving records.\n",
			"Expected: ", expected, "\n",
			"Received: ", received,
		)
	}
}

func TestDataFrame_Mutate(t *testing.T) {
	a := New(
		series.New([]string{"b", "a", "b", "c", "d"}, series.String, "COL.1"),
		series.New([]int{1, 2, 4, 5, 4}, series.Int, "COL.2"),
		series.New([]float64{3.0, 4.0, 5.3, 3.2, 1.2}, series.Float, "COL.3"),
	)
	table := []struct {
		s     series.Series
		expDf DataFrame
	}{
		{
			series.New([]string{"A", "B", "A", "A", "A"}, series.String, "COL.1"),
			New(
				series.New([]string{"A", "B", "A", "A", "A"}, series.String, "COL.1"),
				series.New([]int{1, 2, 4, 5, 4}, series.Int, "COL.2"),
				series.New([]float64{3.0, 4.0, 5.3, 3.2, 1.2}, series.Float, "COL.3"),
			),
		},
		{
			series.New([]string{"A", "B", "A", "A", "A"}, series.String, "COL.2"),
			New(
				series.New([]string{"b", "a", "b", "c", "d"}, series.String, "COL.1"),
				series.New([]string{"A", "B", "A", "A", "A"}, series.String, "COL.2"),
				series.New([]float64{3.0, 4.0, 5.3, 3.2, 1.2}, series.Float, "COL.3"),
			),
		},
		{
			series.New([]string{"A", "B", "A", "A", "A"}, series.String, "COL.4"),
			New(
				series.New([]string{"b", "a", "b", "c", "d"}, series.String, "COL.1"),
				series.New([]int{1, 2, 4, 5, 4}, series.Int, "COL.2"),
				series.New([]float64{3.0, 4.0, 5.3, 3.2, 1.2}, series.Float, "COL.3"),
				series.New([]string{"A", "B", "A", "A", "A"}, series.String, "COL.4"),
			),
		},
	}
	for i, tc := range table {
		b := a.Mutate(tc.s)

		if b.Err != nil {
			t.Errorf("Test: %d\nError:%v", i, b.Err)
		}
		//if err := checkAddrDf(a, b); err != nil {
		//t.Error(err)
		//}
		// Check that the types are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Types(), b.Types()) {
			t.Errorf("Test: %d\nDifferent types:\nA:%v\nB:%v", i, tc.expDf.Types(), b.Types())
		}
		// Check that the colnames are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Names(), b.Names()) {
			t.Errorf("Test: %d\nDifferent colnames:\nA:%v\nB:%v", i, tc.expDf.Names(), b.Names())
		}
		// Check that the values are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Records(), b.Records()) {
			t.Errorf("Test: %d\nDifferent values:\nA:%v\nB:%v", i, tc.expDf.Records(), b.Records())
		}
	}
}

func TestDataFrame_Filter_Or(t *testing.T) {
	a := New(
		series.New([]string{"b", "a", "b", "c", "d"}, series.String, "COL.1"),
		series.New([]int{1, 2, 4, 5, 4}, series.Int, "COL.2"),
		series.New([]float64{3.0, 4.0, 5.3, 3.2, 1.2}, series.Float, "COL.3"),
	)
	table := []struct {
		filters []F
		expDf   DataFrame
	}{
		{
			[]F{{0, "COL.2", series.GreaterEq, 4}},
			New(
				series.New([]string{"b", "c", "d"}, series.String, "COL.1"),
				series.New([]int{4, 5, 4}, series.Int, "COL.2"),
				series.New([]float64{5.3, 3.2, 1.2}, series.Float, "COL.3"),
			),
		},
		{
			[]F{
				{0, "COL.2", series.Greater, 4},
				{0, "COL.2", series.Eq, 1},
			},
			New(
				series.New([]string{"b", "c"}, series.String, "COL.1"),
				series.New([]int{1, 5}, series.Int, "COL.2"),
				series.New([]float64{3.0, 3.2}, series.Float, "COL.3"),
			),
		},
		{
			[]F{
				{0, "COL.2", series.Greater, 4},
				{0, "COL.2", series.Eq, 1},
				{0, "COL.1", series.Eq, "d"},
			},
			New(
				series.New([]string{"b", "c", "d"}, series.String, "COL.1"),
				series.New([]int{1, 5, 4}, series.Int, "COL.2"),
				series.New([]float64{3.0, 3.2, 1.2}, series.Float, "COL.3"),
			),
		},
		{
			[]F{
				{1, "", series.Greater, 4},
				{1, "", series.Eq, 1},
				{0, "", series.Eq, "d"},
			},
			New(
				series.New([]string{"b", "c", "d"}, series.String, "COL.1"),
				series.New([]int{1, 5, 4}, series.Int, "COL.2"),
				series.New([]float64{3.0, 3.2, 1.2}, series.Float, "COL.3"),
			),
		},
	}
	for i, tc := range table {
		b := a.Filter(tc.filters...)

		if b.Err != nil {
			t.Errorf("Test: %d\nError:%v", i, b.Err)
		}
		//if err := checkAddrDf(a, b); err != nil {
		//t.Error(err)
		//}
		// Check that the types are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Types(), b.Types()) {
			t.Errorf("Test: %d\nDifferent types:\nA:%v\nB:%v", i, tc.expDf.Types(), b.Types())
		}
		// Check that the colnames are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Names(), b.Names()) {
			t.Errorf("Test: %d\nDifferent colnames:\nA:%v\nB:%v", i, tc.expDf.Names(), b.Names())
		}
		// Check that the values are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Records(), b.Records()) {
			t.Errorf("Test: %d\nDifferent values:\nA:%v\nB:%v", i, tc.expDf.Records(), b.Records())
		}

		b2 := a.FilterAggregation(Or, tc.filters...)

		// Check that the types are the same between both DataFrames
		if !reflect.DeepEqual(b.Types(), b2.Types()) {
			t.Errorf("Test: %d\nDifferent types:\nB:%v\nB2:%v", i, b.Types(), b2.Types())
		}
		// Check that the colnames are the same between both DataFrames
		if !reflect.DeepEqual(b.Names(), b2.Names()) {
			t.Errorf("Test: %d\nDifferent colnames:\nB:%v\nB2:%v", i, b.Names(), b2.Names())
		}
		// Check that the values are the same between both DataFrames
		if !reflect.DeepEqual(b.Records(), b2.Records()) {
			t.Errorf("Test: %d\nDifferent values:\nB:%v\nB2:%v", i, b.Records(), b2.Records())
		}
	}
}

func TestDataFrame_Filter_And(t *testing.T) {
	a := New(
		series.New([]string{"b", "a", "b", "c", "d"}, series.String, "COL.1"),
		series.New([]int{1, 2, 4, 5, 4}, series.Int, "COL.2"),
		series.New([]float64{3.0, 4.0, 5.3, 3.2, 1.2}, series.Float, "COL.3"),
	)
	table := []struct {
		filters []F
		expDf   DataFrame
	}{
		{
			[]F{{"COL.2", series.GreaterEq, 4}},
			New(
				series.New([]string{"b", "c", "d"}, series.String, "COL.1"),
				series.New([]int{4, 5, 4}, series.Int, "COL.2"),
				series.New([]float64{5.3, 3.2, 1.2}, series.Float, "COL.3"),
			),
		},
		// should not have any rows
		{
			[]F{
				{"COL.2", series.Greater, 4},
				{"COL.2", series.Eq, 1},
			},
			New(
				series.New([]string{}, series.String, "COL.1"),
				series.New([]int{}, series.Int, "COL.2"),
				series.New([]float64{}, series.Float, "COL.3"),
			),
		},
		{
			[]F{
				{"COL.2", series.Less, 4},
				{"COL.1", series.Eq, "b"},
			},
			New(
				series.New([]string{"b"}, series.String, "COL.1"),
				series.New([]int{1}, series.Int, "COL.2"),
				series.New([]float64{3.0}, series.Float, "COL.3"),
			),
		},
	}
	for i, tc := range table {
		b := a.FilterAggregation(And, tc.filters...)

		if b.Err != nil {
			t.Errorf("Test: %d\nError:%v", i, b.Err)
		}
		//if err := checkAddrDf(a, b); err != nil {
		//t.Error(err)
		//}
		// Check that the types are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Types(), b.Types()) {
			t.Errorf("Test: %d\nDifferent types:\nA:%v\nB:%v", i, tc.expDf.Types(), b.Types())
		}
		// Check that the colnames are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Names(), b.Names()) {
			t.Errorf("Test: %d\nDifferent colnames:\nA:%v\nB:%v", i, tc.expDf.Names(), b.Names())
		}
		// Check that the values are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Records(), b.Records()) {
			t.Errorf("Test: %d\nDifferent values:\nA:%v\nB:%v", i, tc.expDf.Records(), b.Records())
		}
	}
}

func TestLoadRecords(t *testing.T) {
	table := []struct {
		df    DataFrame
		expDf DataFrame
		err   bool
	}{
		{ // Test: 0
			LoadRecords(
				[][]string{
					{"A", "B", "C", "D"},
					{"a", "1", "true", "0"},
					{"b", "2", "true", "0.5"},
				},
			),
			New(
				series.New([]string{"a", "b"}, series.String, "A"),
				series.New([]int{1, 2}, series.Int, "B"),
				series.New([]bool{true, true}, series.Bool, "C"),
				series.New([]float64{0, 0.5}, series.Float, "D"),
			),
			false,
		},
		{ // Test: 1
			LoadRecords(
				[][]string{
					{"A", "B", "C", "D"},
					{"a", "1", "true", "0"},
					{"b", "2", "true", "0.5"},
				},
				HasHeader(true),
				DetectTypes(false),
				DefaultType(series.String),
			),
			New(
				series.New([]string{"a", "b"}, series.String, "A"),
				series.New([]int{1, 2}, series.String, "B"),
				series.New([]bool{true, true}, series.String, "C"),
				series.New([]string{"0", "0.5"}, series.String, "D"),
			),
			false,
		},
		{ // Test: 2
			LoadRecords(
				[][]string{
					{"A", "B", "C", "D"},
					{"a", "1", "true", "0"},
					{"b", "2", "true", "0.5"},
				},
				HasHeader(false),
				DetectTypes(false),
				DefaultType(series.String),
			),
			New(
				series.New([]string{"A", "a", "b"}, series.String, "X0"),
				series.New([]string{"B", "1", "2"}, series.String, "X1"),
				series.New([]string{"C", "true", "true"}, series.String, "X2"),
				series.New([]string{"D", "0", "0.5"}, series.String, "X3"),
			),
			false,
		},
		{ // Test: 3
			LoadRecords(
				[][]string{
					{"A", "B", "C", "D"},
					{"a", "1", "true", "0"},
					{"b", "2", "true", "0.5"},
				},
				HasHeader(true),
				DetectTypes(false),
				DefaultType(series.String),
				WithTypes(map[string]series.Type{
					"B": series.Float,
					"C": series.String,
				}),
			),
			New(
				series.New([]string{"a", "b"}, series.String, "A"),
				series.New([]float64{1, 2}, series.Float, "B"),
				series.New([]bool{true, true}, series.String, "C"),
				series.New([]string{"0", "0.5"}, series.String, "D"),
			),
			false,
		},
		{ // Test: 4
			LoadRecords(
				[][]string{
					{"A", "B", "C", "D"},
					{"a", "1", "true", "0"},
					{"b", "2", "true", "0.5"},
				},
				HasHeader(true),
				DetectTypes(true),
				DefaultType(series.String),
				WithTypes(map[string]series.Type{
					"B": series.Float,
				}),
			),
			New(
				series.New([]string{"a", "b"}, series.String, "A"),
				series.New([]float64{1, 2}, series.Float, "B"),
				series.New([]bool{true, true}, series.Bool, "C"),
				series.New([]string{"0", "0.5"}, series.Float, "D"),
			),
			false,
		},
		{ // Test: 5
			LoadRecords(
				[][]string{
					{"A", "B", "C", "D"},
					{"a", "1", "true", "0"},
					{"b", "2", "true", "0.5"},
				},
				HasHeader(true),
				Names("MyA", "MyB", "MyC", "MyD"),
			),
			New(
				series.New([]string{"a", "b"}, series.String, "MyA"),
				series.New([]int{1, 2}, series.Int, "MyB"),
				series.New([]bool{true, true}, series.Bool, "MyC"),
				series.New([]string{"0", "0.5"}, series.Float, "MyD"),
			),
			false,
		},
		{ // Test: 6
			LoadRecords(
				[][]string{
					{"A", "B", "C", "D"},
					{"a", "1", "true", "0"},
					{"b", "2", "true", "0.5"},
				},
				HasHeader(false),
				Names("MyA", "MyB", "MyC", "MyD"),
			),
			New(
				series.New([]string{"A", "a", "b"}, series.String, "MyA"),
				series.New([]string{"B", "1", "2"}, series.String, "MyB"),
				series.New([]string{"C", "true", "true"}, series.String, "MyC"),
				series.New([]string{"D", "0", "0.5"}, series.String, "MyD"),
			),
			false,
		},
		{ // Test: 7
			LoadRecords(
				[][]string{
					{"A", "B", "C", "D"},
					{"a", "1", "true", "0"},
					{"b", "2", "true", "0.5"},
				},
				HasHeader(false),
				Names("MyA", "MyB", "MyC"),
			),
			DataFrame{},
			true,
		},
		{ // Test: 8
			LoadRecords(
				[][]string{
					{"A", "B", "C", "D"},
					{"a", "1", "true", "0"},
					{"b", "2", "true", "0.5"},
				},
				HasHeader(false),
				Names("MyA", "MyB", "MyC", "MyD", "MyE"),
			),
			DataFrame{},
			true,
		},
		{
			LoadRecords(
				[][]string{
					{"A", "B", "C", "D"},
					{"1", "1", "true", "0"},
					{"a", "2", "true", "0.5"},
				},
			),
			New(
				series.New([]string{"1", "a"}, series.String, "A"),
				series.New([]int{1, 2}, series.Int, "B"),
				series.New([]bool{true, true}, series.Bool, "C"),
				series.New([]float64{0, 0.5}, series.Float, "D"),
			),
			false,
		},
		{
			LoadRecords(
				[][]string{
					{"A", "B", "C", "D"},
					{"a", "1", "true", "0"},
					{"1", "2", "true", "0.5"},
				},
			),
			New(
				series.New([]string{"a", "1"}, series.String, "A"),
				series.New([]int{1, 2}, series.Int, "B"),
				series.New([]bool{true, true}, series.Bool, "C"),
				series.New([]float64{0, 0.5}, series.Float, "D"),
			),
			false,
		},
		{
			LoadRecords(
				[][]string{
					{"A", "B", "C", "D"},
					{"a", "1", "true", "0.5"},
					{"1", "2", "true", "1"},
				},
			),
			New(
				series.New([]string{"a", "1"}, series.String, "A"),
				series.New([]int{1, 2}, series.Int, "B"),
				series.New([]bool{true, true}, series.Bool, "C"),
				series.New([]float64{0.5, 1}, series.Float, "D"),
			),
			false,
		},
		{
			LoadRecords(
				[][]string{
					{"A", "B", "C", "D"},
					{"a", "1", "trueee", "0.5"},
					{"1", "2", "true", "1"},
				},
			),
			New(
				series.New([]string{"a", "1"}, series.String, "A"),
				series.New([]int{1, 2}, series.Int, "B"),
				series.New([]string{"trueee", "true"}, series.String, "C"),
				series.New([]float64{0.5, 1}, series.Float, "D"),
			),
			false,
		},
		{
			LoadRecords(
				[][]string{
					{"A", "B", "C", "D"},
					{"a", "1", "true", "0.5"},
					{"1", "2", "trueee", "1"},
				},
			),
			New(
				series.New([]string{"a", "1"}, series.String, "A"),
				series.New([]int{1, 2}, series.Int, "B"),
				series.New([]string{"true", "trueee"}, series.String, "C"),
				series.New([]float64{0.5, 1}, series.Float, "D"),
			),
			false,
		},
		{
			LoadRecords(
				[][]string{
					{"A", "B", "C", "D"},
					{"a", "1", "true", "0.5"},
					{"1", "2", "true", "a"},
				},
			),
			New(
				series.New([]string{"a", "1"}, series.String, "A"),
				series.New([]int{1, 2}, series.Int, "B"),
				series.New([]bool{true, true}, series.Bool, "C"),
				series.New([]string{"0.5", "a"}, series.String, "D"),
			),
			false,
		},
		{
			LoadRecords(
				[][]string{
					{"A", "B", "C", "D"},
					{"a", "1", "true", "0.5"},
					{"1", "2", "0.5", "a"},
				},
			),
			New(
				series.New([]string{"a", "1"}, series.String, "A"),
				series.New([]int{1, 2}, series.Int, "B"),
				series.New([]string{"true", "NaN"}, series.Bool, "C"),
				series.New([]string{"0.5", "a"}, series.String, "D"),
			),
			false,
		},
	}

	for i, tc := range table {
		if tc.err != (tc.df.Err != nil) {
			t.Errorf("Test: %d\nError: %v", i, tc.df.Err)
		}
		// Check that the types are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Types(), tc.df.Types()) {
			t.Errorf("Test: %d\nDifferent types:\nA:%v\nB:%v", i, tc.expDf.Types(), tc.df.Types())
		}
		// Check that the colnames are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Names(), tc.df.Names()) {
			t.Errorf("Test: %d\nDifferent colnames:\nA:%v\nB:%v", i, tc.expDf.Names(), tc.df.Names())
		}
		// Check that the values are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Records(), tc.df.Records()) {
			t.Errorf("Test: %d\nDifferent values:\nA:%v\nB:%v", i, tc.expDf.Records(), tc.df.Records())
		}
	}
}

func TestLoadMaps(t *testing.T) {
	table := []struct {
		df    DataFrame
		expDf DataFrame
	}{
		{ // Test: 0
			LoadMaps(
				[]map[string]interface{}{
					map[string]interface{}{
						"A": "a",
						"B": 1,
						"C": true,
						"D": 0,
					},
					map[string]interface{}{
						"A": "b",
						"B": 2,
						"C": true,
						"D": 0.5,
					},
				},
			),
			New(
				series.New([]string{"a", "b"}, series.String, "A"),
				series.New([]int{1, 2}, series.Int, "B"),
				series.New([]bool{true, true}, series.Bool, "C"),
				series.New([]float64{0, 0.5}, series.Float, "D"),
			),
		},
		{ // Test: 1
			LoadMaps(
				[]map[string]interface{}{
					map[string]interface{}{
						"A": "a",
						"B": 1,
						"C": true,
						"D": 0,
					},
					map[string]interface{}{
						"A": "b",
						"B": 2,
						"C": true,
						"D": 0.5,
					},
				},
				HasHeader(true),
				DetectTypes(false),
				DefaultType(series.String),
			),
			New(
				series.New([]string{"a", "b"}, series.String, "A"),
				series.New([]int{1, 2}, series.String, "B"),
				series.New([]bool{true, true}, series.String, "C"),
				series.New([]string{"0", "0.5"}, series.String, "D"),
			),
		},
		{ // Test: 2
			LoadMaps(
				[]map[string]interface{}{
					map[string]interface{}{
						"A": "a",
						"B": 1,
						"C": true,
						"D": 0,
					},
					map[string]interface{}{
						"A": "b",
						"B": 2,
						"C": true,
						"D": 0.5,
					},
				},
				HasHeader(false),
				DetectTypes(false),
				DefaultType(series.String),
			),
			New(
				series.New([]string{"A", "a", "b"}, series.String, "X0"),
				series.New([]string{"B", "1", "2"}, series.String, "X1"),
				series.New([]string{"C", "true", "true"}, series.String, "X2"),
				series.New([]string{"D", "0", "0.5"}, series.String, "X3"),
			),
		},
		{ // Test: 3
			LoadMaps(
				[]map[string]interface{}{
					map[string]interface{}{
						"A": "a",
						"B": 1,
						"C": true,
						"D": 0,
					},
					map[string]interface{}{
						"A": "b",
						"B": 2,
						"C": true,
						"D": 0.5,
					},
				},
				HasHeader(true),
				DetectTypes(false),
				DefaultType(series.String),
				WithTypes(map[string]series.Type{
					"B": series.Float,
					"C": series.String,
				}),
			),
			New(
				series.New([]string{"a", "b"}, series.String, "A"),
				series.New([]float64{1, 2}, series.Float, "B"),
				series.New([]bool{true, true}, series.String, "C"),
				series.New([]string{"0", "0.5"}, series.String, "D"),
			),
		},
		{ // Test: 4
			LoadMaps(
				[]map[string]interface{}{
					map[string]interface{}{
						"A": "a",
						"B": 1,
						"C": true,
						"D": 0,
					},
					map[string]interface{}{
						"A": "b",
						"B": 2,
						"C": true,
						"D": 0.5,
					},
				},
				HasHeader(true),
				DetectTypes(true),
				DefaultType(series.String),
				WithTypes(map[string]series.Type{
					"B": series.Float,
				}),
			),
			New(
				series.New([]string{"a", "b"}, series.String, "A"),
				series.New([]float64{1, 2}, series.Float, "B"),
				series.New([]bool{true, true}, series.Bool, "C"),
				series.New([]string{"0", "0.5"}, series.Float, "D"),
			),
		},
	}

	for i, tc := range table {
		if tc.df.Err != nil {
			t.Errorf("Test: %d\nError: %v", i, tc.df.Err)
		}
		// Check that the types are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Types(), tc.df.Types()) {
			t.Errorf("Test: %d\nDifferent types:\nA:%v\nB:%v", i, tc.expDf.Types(), tc.df.Types())
		}
		// Check that the colnames are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Names(), tc.df.Names()) {
			t.Errorf("Test: %d\nDifferent colnames:\nA:%v\nB:%v", i, tc.expDf.Names(), tc.df.Names())
		}
		// Check that the values are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Records(), tc.df.Records()) {
			t.Errorf("Test: %d\nDifferent values:\nA:%v\nB:%v", i, tc.expDf.Records(), tc.df.Records())
		}
	}
}

func TestReadCSV(t *testing.T) {
	// Load the data from a CSV string and try to infer the type of the
	// columns
	csvStr := `
Country,Date,Age,Amount,Id
"United States",2012-02-01,50,112.1,01234
"United States",2012-02-01,32,321.31,54320
"United Kingdom",2012-02-01,17,18.2,12345
"United States",2012-02-01,32,321.31,54320
"United Kingdom",2012-02-01,NA,18.2,12345
"United States",2012-02-01,32,321.31,54320
"United States",2012-02-01,32,321.31,54320
Spain,2012-02-01,66,555.42,00241
`
	a := ReadCSV(strings.NewReader(csvStr))
	if a.Err != nil {
		t.Errorf("Expected success, got error: %v", a.Err)
	}
}

func TestReadJSON(t *testing.T) {
	table := []struct {
		jsonStr string
		expDf   DataFrame
	}{
		{
			`[{"COL.1":null,"COL.2":1,"COL.3":3},{"COL.1":5,"COL.2":2,"COL.3":2},{"COL.1":6,"COL.2":3,"COL.3":20180428}]`,
			LoadRecords(
				[][]string{
					{"COL.1", "COL.2", "COL.3"},
					{"NaN", "1", "3"},
					{"5", "2", "2"},
					{"6", "3", "20180428"},
				},
				DetectTypes(false),
				DefaultType(series.Int),
			),
		},
		{
			`[{"COL.2":1,"COL.3":3},{"COL.1":5,"COL.2":2,"COL.3":2},{"COL.1":6,"COL.2":3,"COL.3":1}]`,
			LoadRecords(
				[][]string{
					{"COL.1", "COL.2", "COL.3"},
					{"NaN", "1", "3"},
					{"5", "2", "2"},
					{"6", "3", "1"},
				},
				DetectTypes(false),
				DefaultType(series.Int),
			),
		},
	}
	for i, tc := range table {
		c := ReadJSON(strings.NewReader(tc.jsonStr))

		if c.Err != nil {
			t.Errorf("Test: %d\nError:%v", i, c.Err)
		}
		// Check that the types are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Types(), c.Types()) {
			t.Errorf("Test: %d\nDifferent types:\nA:%v\nB:%v", i, tc.expDf.Types(), c.Types())
		}
		// Check that the colnames are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Names(), c.Names()) {
			t.Errorf("Test: %d\nDifferent colnames:\nA:%v\nB:%v", i, tc.expDf.Names(), c.Names())
		}
		// Check that the values are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Records(), c.Records()) {
			t.Errorf("Test: %d\nDifferent values:\nA:%v\nB:%v", i, tc.expDf.Records(), c.Records())
		}
	}
}

func TestReadHTML(t *testing.T) {
	table := []struct {
		htmlStr string
		expDf   []DataFrame
	}{
		{
			"",
			[]DataFrame{},
		},
		{
			`<html>
			<body>
			<table>
			<tr><td>COL.1</td></tr>
			<tr><td>100</td></tr>
			</table>
			</body>
			</html>`,
			[]DataFrame{
				LoadRecords(
					[][]string{
						{"COL.1"},
						{"100"},
					}),
			},
		},
		{
			`<html>
			<body>
			<table>
			<tr><td rowspan='2'>COL.1</td><td rowspan='2'>COL.2</td><td>COL.3</td></tr>
			<tr><td>100</td></tr>
			</table>
			</body>
			</html>`,
			[]DataFrame{
				LoadRecords(
					[][]string{
						{"COL.1", "COL.2", "COL.3"},
						{"COL.1", "COL.2", "100"},
					}),
			},
		},
	}

	for i, tc := range table {
		cs := ReadHTML(strings.NewReader(tc.htmlStr))
		if tc.htmlStr != "" && len(cs) == 0 {
			t.Errorf("Test: %d, got zero dataframes: %#v", i, cs)
		}
		for j, c := range cs {
			if len(cs) != len(tc.expDf) {
				t.Errorf("Test: %d\n got len(%d), want len(%d)", i, len(cs), len(tc.expDf))
			}
			if c.Err != nil {
				t.Errorf("Test: %d\nError:%v", i, c.Err)
			}
			// Check that the types are the same between both DataFrames
			if !reflect.DeepEqual(tc.expDf[j].Types(), c.Types()) {
				t.Errorf("Test: %d\nDifferent types:\nA:%v\nB:%v", i, tc.expDf[j].Types(), c.Types())
			}
			// Check that the colnames are the same between both DataFrames
			if !reflect.DeepEqual(tc.expDf[j].Names(), c.Names()) {
				t.Errorf("Test: %d\nDifferent colnames:\nA:%v\nB:%v", i, tc.expDf[j].Names(), c.Names())
			}
			// Check that the values are the same between both DataFrames
			if !reflect.DeepEqual(tc.expDf[j].Records(), c.Records()) {
				t.Errorf("Test: %d\nDifferent values:\nA:%v\nB:%v", i, tc.expDf[j].Records(), c.Records())
			}
		}
	}
}

func TestDataFrame_SetNames(t *testing.T) {
	a := New(
		series.New([]string{"a", "b", "c"}, series.String, "COL.1"),
		series.New([]int{1, 2, 3}, series.Int, "COL.2"),
		series.New([]float64{3, 2, 1}, series.Float, "COL.3"),
	)

	err := a.SetNames("wot", "tho", "tree")
	if err != nil {
		t.Error("Expected success, got error")
	}
	err = a.SetNames("yaaa")
	if err == nil {
		t.Error("Expected error, got success")
	}
}

func TestDataFrame_InnerJoin(t *testing.T) {
	a := LoadRecords(
		[][]string{
			{"A", "B", "C", "D"},
			{"1", "a", "5.1", "true"},
			{"2", "b", "6.0", "true"},
			{"3", "c", "6.0", "false"},
			{"1", "d", "7.1", "false"},
		},
	)
	b := LoadRecords(
		[][]string{
			{"A", "F", "D"},
			{"1", "1", "true"},
			{"4", "2", "false"},
			{"2", "8", "false"},
			{"5", "9", "false"},
		},
	)
	table := []struct {
		keys  []string
		expDf DataFrame
	}{
		{
			[]string{"A", "D"},
			LoadRecords(
				[][]string{
					{"A", "D", "B", "C", "F"},
					{"1", "true", "a", "5.1", "1"},
				},
			),
		},
		{
			[]string{"A"},
			LoadRecords(
				[][]string{
					{"A", "B", "C", "D_0", "F", "D_1"},
					{"1", "a", "5.1", "true", "1", "true"},
					{"2", "b", "6.0", "true", "8", "false"},
					{"1", "d", "7.1", "false", "1", "true"},
				},
			),
		},
		{
			[]string{"D"},
			LoadRecords(
				[][]string{
					{"D", "A_0", "B", "C", "A_1", "F"},
					{"true", "1", "a", "5.1", "1", "1"},
					{"true", "2", "b", "6.0", "1", "1"},
					{"false", "3", "c", "6.0", "4", "2"},
					{"false", "3", "c", "6.0", "2", "8"},
					{"false", "3", "c", "6.0", "5", "9"},
					{"false", "1", "d", "7.1", "4", "2"},
					{"false", "1", "d", "7.1", "2", "8"},
					{"false", "1", "d", "7.1", "5", "9"},
				},
			),
		},
	}
	for i, tc := range table {
		c := a.InnerJoin(b, tc.keys...)

		if err := c.Err; err != nil {
			t.Errorf("Test: %d\nError:%v", i, b.Err)
		}
		// Check that the types are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Types(), c.Types()) {
			t.Errorf("Test: %d\nDifferent types:\nA:%v\nB:%v", i, tc.expDf.Types(), c.Types())
		}
		// Check that the colnames are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Names(), c.Names()) {
			t.Errorf("Test: %d\nDifferent colnames:\nA:%v\nB:%v", i, tc.expDf.Names(), c.Names())
		}
		// Check that the values are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Records(), c.Records()) {
			t.Errorf("Test: %d\nDifferent values:\nA:%v\nB:%v", i, tc.expDf.Records(), c.Records())
		}
	}
}

func TestDataFrame_LeftJoin(t *testing.T) {
	a := LoadRecords(
		[][]string{
			{"A", "B", "C", "D"},
			{"1", "4", "5.1", "1"},
			{"2", "4", "6.0", "1"},
			{"3", "3", "6.0", "0"},
			{"1", "2", "7.1", "0"},
		},
		DetectTypes(false),
		DefaultType(series.Float),
	)
	b := LoadRecords(
		[][]string{
			{"A", "F", "D"},
			{"1", "1", "1"},
			{"4", "2", "0"},
			{"2", "8", "0"},
			{"5", "9", "0"},
		},
		DetectTypes(false),
		DefaultType(series.Float),
	)
	table := []struct {
		keys  []string
		expDf DataFrame
	}{
		{
			[]string{"A", "D"},
			LoadRecords(
				[][]string{
					{"A", "D", "B", "C", "F"},
					{"1", "1", "4", "5.1", "1"},
					{"2", "1", "4", "6.0", "NaN"},
					{"3", "0", "3", "6.0", "NaN"},
					{"1", "0", "2", "7.1", "NaN"},
				},
				DetectTypes(false),
				DefaultType(series.Float),
			),
		},
		{
			[]string{"A"},
			LoadRecords(
				[][]string{
					{"A", "B", "C", "D_0", "F", "D_1"},
					{"1", "4", "5.1", "1", "1", "1"},
					{"2", "4", "6.0", "1", "8", "0"},
					{"3", "3", "6.0", "0", "NaN", "NaN"},
					{"1", "2", "7.1", "0", "1", "1"},
				},
				DetectTypes(false),
				DefaultType(series.Float),
			),
		},
	}
	for i, tc := range table {
		c := a.LeftJoin(b, tc.keys...)

		if err := c.Err; err != nil {
			t.Errorf("Test: %d\nError:%v", i, b.Err)
		}
		// Check that the types are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Types(), c.Types()) {
			t.Errorf("Test: %d\nDifferent types:\nA:%v\nB:%v", i, tc.expDf.Types(), c.Types())
		}
		// Check that the colnames are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Names(), c.Names()) {
			t.Errorf("Test: %d\nDifferent colnames:\nA:%v\nB:%v", i, tc.expDf.Names(), c.Names())
		}
		// Check that the values are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Records(), c.Records()) {
			t.Errorf("Test: %d\nDifferent values:\nA:%v\nB:%v", i, tc.expDf.Records(), c.Records())
		}
	}
}

func TestDataFrame_RightJoin(t *testing.T) {
	a := LoadRecords(
		[][]string{
			{"A", "F", "D"},
			{"1", "1", "1"},
			{"4", "2", "0"},
			{"2", "8", "0"},
			{"5", "9", "0"},
		},
		DetectTypes(false),
		DefaultType(series.Float),
	)
	b := LoadRecords(
		[][]string{
			{"A", "B", "C", "D"},
			{"1", "4", "5.1", "1"},
			{"2", "4", "6.0", "1"},
			{"3", "3", "6.0", "0"},
			{"1", "2", "7.1", "0"},
		},
		DetectTypes(false),
		DefaultType(series.Float),
	)
	table := []struct {
		keys  []string
		expDf DataFrame
	}{
		{
			[]string{"A", "D"},
			LoadRecords(
				[][]string{
					{"A", "D", "F", "B", "C"},
					{"1", "1", "1", "4", "5.1"},
					{"2", "1", "NaN", "4", "6.0"},
					{"3", "0", "NaN", "3", "6.0"},
					{"1", "0", "NaN", "2", "7.1"},
				},
				DetectTypes(false),
				DefaultType(series.Float),
			),
		},
		{
			[]string{"A"},
			LoadRecords(
				[][]string{
					{"A", "F", "D_0", "B", "C", "D_1"},
					{"1", "1", "1", "4", "5.1", "1"},
					{"2", "8", "0", "4", "6.0", "1"},
					{"1", "1", "1", "2", "7.1", "0"},
					{"3", "NaN", "NaN", "3", "6.0", "0"},
				},
				DetectTypes(false),
				DefaultType(series.Float),
			),
		},
	}
	for i, tc := range table {
		c := a.RightJoin(b, tc.keys...)

		if err := c.Err; err != nil {
			t.Errorf("Test: %d\nError:%v", i, b.Err)
		}
		// Check that the types are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Types(), c.Types()) {
			t.Errorf("Test: %d\nDifferent types:\nA:%v\nB:%v", i, tc.expDf.Types(), c.Types())
		}
		// Check that the colnames are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Names(), c.Names()) {
			t.Errorf("Test: %d\nDifferent colnames:\nA:%v\nB:%v", i, tc.expDf.Names(), c.Names())
		}
		// Check that the values are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Records(), c.Records()) {
			t.Errorf("Test: %d\nDifferent values:\nA:%v\nB:%v", i, tc.expDf.Records(), c.Records())
		}
	}
}

func TestDataFrame_OuterJoin(t *testing.T) {
	a := LoadRecords(
		[][]string{
			{"A", "B", "C", "D"},
			{"1", "4", "5.1", "1"},
			{"2", "4", "6.0", "1"},
			{"3", "3", "6.0", "0"},
			{"1", "2", "7.1", "0"},
		},
		DetectTypes(false),
		DefaultType(series.Float),
	)
	b := LoadRecords(
		[][]string{
			{"A", "F", "D"},
			{"1", "1", "1"},
			{"4", "2", "0"},
			{"2", "8", "0"},
			{"5", "9", "0"},
		},
		DetectTypes(false),
		DefaultType(series.Float),
	)
	table := []struct {
		keys  []string
		expDf DataFrame
	}{
		{
			[]string{"A", "D"},
			LoadRecords(
				[][]string{
					{"A", "D", "B", "C", "F"},
					{"1", "1", "4", "5.1", "1"},
					{"2", "1", "4", "6.0", "NaN"},
					{"3", "0", "3", "6.0", "NaN"},
					{"1", "0", "2", "7.1", "NaN"},
					{"4", "0", "NaN", "NaN", "2"},
					{"2", "0", "NaN", "NaN", "8"},
					{"5", "0", "NaN", "NaN", "9"},
				},
				DetectTypes(false),
				DefaultType(series.Float),
			),
		},
		{
			[]string{"A"},
			LoadRecords(
				[][]string{
					{"A", "B", "C", "D_0", "F", "D_1"},
					{"1", "4", "5.1", "1", "1", "1"},
					{"2", "4", "6.0", "1", "8", "0"},
					{"3", "3", "6.0", "0", "NaN", "NaN"},
					{"1", "2", "7.1", "0", "1", "1"},
					{"4", "NaN", "NaN", "NaN", "2", "0"},
					{"5", "NaN", "NaN", "NaN", "9", "0"},
				},
				DetectTypes(false),
				DefaultType(series.Float),
			),
		},
	}
	for i, tc := range table {
		c := a.OuterJoin(b, tc.keys...)

		if err := c.Err; err != nil {
			t.Errorf("Test: %d\nError:%v", i, b.Err)
		}
		// Check that the types are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Types(), c.Types()) {
			t.Errorf("Test: %d\nDifferent types:\nA:%v\nB:%v", i, tc.expDf.Types(), c.Types())
		}
		// Check that the colnames are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Names(), c.Names()) {
			t.Errorf("Test: %d\nDifferent colnames:\nA:%v\nB:%v", i, tc.expDf.Names(), c.Names())
		}
		// Check that the values are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Records(), c.Records()) {
			t.Errorf("Test: %d\nDifferent values:\nA:%v\nB:%v", i, tc.expDf.Records(), c.Records())
		}
	}
}

func TestDataFrame_CrossJoin(t *testing.T) {
	a := LoadRecords(
		[][]string{
			{"A", "B", "C", "D"},
			{"1", "a", "5.1", "true"},
			{"2", "b", "6.0", "true"},
			{"3", "c", "6.0", "false"},
			{"1", "d", "7.1", "false"},
		},
	)
	b := LoadRecords(
		[][]string{
			{"A", "F", "D"},
			{"1", "1", "true"},
			{"4", "2", "false"},
			{"2", "8", "false"},
			{"5", "9", "false"},
		},
	)
	c := a.CrossJoin(b)
	expectedCSV := `
A_0,B,C,D_0,A_1,F,D_1
1,a,5.1,true,1,1,true
1,a,5.1,true,4,2,false
1,a,5.1,true,2,8,false
1,a,5.1,true,5,9,false
2,b,6.0,true,1,1,true
2,b,6.0,true,4,2,false
2,b,6.0,true,2,8,false
2,b,6.0,true,5,9,false
3,c,6.0,false,1,1,true
3,c,6.0,false,4,2,false
3,c,6.0,false,2,8,false
3,c,6.0,false,5,9,false
1,d,7.1,false,1,1,true
1,d,7.1,false,4,2,false
1,d,7.1,false,2,8,false
1,d,7.1,false,5,9,false
`
	expected := ReadCSV(
		strings.NewReader(expectedCSV),
		WithTypes(map[string]series.Type{
			"A.1": series.String,
		}))
	if err := c.Err; err != nil {
		t.Errorf("Error:%v", err)
	}
	// Check that the types are the same between both DataFrames
	if !reflect.DeepEqual(expected.Types(), c.Types()) {
		t.Errorf("Different types:\nA:%v\nB:%v", expected.Types(), c.Types())
	}
	// Check that the colnames are the same between both DataFrames
	if !reflect.DeepEqual(expected.Names(), c.Names()) {
		t.Errorf("Different colnames:\nA:%v\nB:%v", expected.Names(), c.Names())
	}
	// Check that the values are the same between both DataFrames
	if !reflect.DeepEqual(expected.Records(), c.Records()) {
		t.Errorf("Different values:\nA:%v\nB:%v", expected.Records(), c.Records())
	}
}

func TestDataFrame_Maps(t *testing.T) {
	a := New(
		series.New([]string{"a", "b", "c"}, series.String, "COL.1"),
		series.New([]string{"", "2", "3"}, series.Int, "COL.2"),
		series.New([]string{"", "", "3"}, series.Int, "COL.3"),
	)
	m := a.Maps()
	expected := []map[string]interface{}{
		map[string]interface{}{
			"COL.1": "a",
			"COL.2": nil,
			"COL.3": nil,
		},
		map[string]interface{}{
			"COL.1": "b",
			"COL.2": 2,
			"COL.3": nil,
		},
		map[string]interface{}{
			"COL.1": "c",
			"COL.2": 3,
			"COL.3": 3,
		},
	}
	if !reflect.DeepEqual(expected, m) {
		t.Errorf("Different values:\nA:%v\nB:%v", expected, m)
	}
}

func TestDataFrame_WriteCSV(t *testing.T) {
	table := []struct {
		df       DataFrame
		options  []WriteOption
		expected string
	}{
		{ // Test: 0
			LoadRecords(
				[][]string{
					{"COL.1", "COL.2", "COL.3"},
					{"NaN", "1", "3"},
					{"b", "2", "2"},
					{"c", "3", "1"},
				},
			),
			nil,
			`COL.1,COL.2,COL.3
NaN,1,3
b,2,2
c,3,1
`,
		},
		{ // Test: 1
			LoadRecords(
				[][]string{
					{"COL.1", "COL.2", "COL.3"},
					{"NaN", "1", "3"},
					{"b", "2", "2"},
					{"c", "3", "1"},
				},
			),
			nil,
			`COL.1,COL.2,COL.3
NaN,1,3
b,2,2
c,3,1
`,
		},
		{ // Test: 2
			LoadRecords(
				[][]string{
					{"COL.1", "COL.2", "COL.3"},
					{"NaN", "1", "3"},
					{"b", "2", "2"},
					{"c", "3", "1"},
				},
			),
			[]WriteOption{WriteHeader(false)},
			`NaN,1,3
b,2,2
c,3,1
`,
		},
	}

	for i, tc := range table {
		buf := new(bytes.Buffer)
		err := tc.df.WriteCSV(buf, tc.options...)
		if err != nil {
			t.Errorf("Test: %d\nError: %v", i, err)
		}
		if tc.expected != buf.String() {
			t.Errorf("Test: %d\nExpected: %v\nreceived: %v", i, tc.expected, buf.String())
		}
	}
}

func TestDataFrame_WriteJSON(t *testing.T) {
	a := LoadRecords(
		[][]string{
			{"COL.1", "COL.2", "COL.3"},
			{"NaN", "1", "3"},
			{"5", "2", "2"},
			{"6", "3", "1"},
		},
		DetectTypes(false),
		DefaultType(series.Int),
	)
	buf := new(bytes.Buffer)
	err := a.WriteJSON(buf)
	if err != nil {
		t.Errorf("Expected success, got error: %v", err)
	}
	expected := `[{"COL.1":null,"COL.2":1,"COL.3":3},{"COL.1":5,"COL.2":2,"COL.3":2},{"COL.1":6,"COL.2":3,"COL.3":1}]
`
	if expected != buf.String() {
		t.Errorf("\nexpected: %v\nreceived: %v", expected, buf.String())
	}
}

func TestDataFrame_Col(t *testing.T) {
	a := LoadRecords(
		[][]string{
			{"COL.1", "COL.2", "COL.3"},
			{"NaN", "1", "3"},
			{"5", "2", "2"},
			{"6", "3", "1"},
		},
		DetectTypes(false),
		DefaultType(series.Int),
	)
	b := a.Col("COL.2")
	expected := series.New([]int{1, 2, 3}, series.Int, "COL.2")
	if !reflect.DeepEqual(b.Records(), expected.Records()) {
		t.Errorf("\nexpected: %v\nreceived: %v", expected, b)
	}
}

func TestDataFrame_Set(t *testing.T) {
	a := LoadRecords(
		[][]string{
			{"A", "B", "C", "D"},
			{"a", "4", "5.1", "true"},
			{"b", "4", "6.0", "true"},
			{"c", "3", "6.0", "false"},
			{"a", "2", "7.1", "false"},
		},
	)
	table := []struct {
		indexes   series.Indexes
		newvalues DataFrame
		expDf     DataFrame
	}{
		{
			series.Ints([]int{0, 2}),
			LoadRecords(
				[][]string{
					{"A", "B", "C", "D"},
					{"k", "5", "7.0", "true"},
					{"k", "4", "6.0", "true"},
				},
			),
			LoadRecords(
				[][]string{
					{"A", "B", "C", "D"},
					{"k", "5", "7.0", "true"},
					{"b", "4", "6.0", "true"},
					{"k", "4", "6.0", "true"},
					{"a", "2", "7.1", "false"},
				},
			),
		},
		{
			series.Ints(0),
			LoadRecords(
				[][]string{
					{"A", "B", "C", "D"},
					{"k", "5", "7.0", "true"},
				},
			),
			LoadRecords(
				[][]string{
					{"A", "B", "C", "D"},
					{"k", "5", "7.0", "true"},
					{"b", "4", "6.0", "true"},
					{"c", "3", "6.0", "false"},
					{"a", "2", "7.1", "false"},
				},
			),
		},
		{
			series.Bools([]bool{true, false, false, false}),
			LoadRecords(
				[][]string{
					{"A", "B", "C", "D"},
					{"k", "5", "7.0", "true"},
				},
			),
			LoadRecords(
				[][]string{
					{"A", "B", "C", "D"},
					{"k", "5", "7.0", "true"},
					{"b", "4", "6.0", "true"},
					{"c", "3", "6.0", "false"},
					{"a", "2", "7.1", "false"},
				},
			),
		},
		{
			series.Bools([]bool{false, true, true, false}),
			LoadRecords(
				[][]string{
					{"A", "B", "C", "D"},
					{"k", "5", "7.0", "true"},
					{"k", "4", "6.0", "true"},
				},
			),
			LoadRecords(
				[][]string{
					{"A", "B", "C", "D"},
					{"a", "4", "5.1", "true"},
					{"k", "5", "7.0", "true"},
					{"k", "4", "6.0", "true"},
					{"a", "2", "7.1", "false"},
				},
			),
		},
		{
			[]int{0, 2},
			LoadRecords(
				[][]string{
					{"A", "B", "C", "D"},
					{"k", "5", "7.0", "true"},
					{"k", "4", "6.0", "true"},
				},
			),
			LoadRecords(
				[][]string{
					{"A", "B", "C", "D"},
					{"k", "5", "7.0", "true"},
					{"b", "4", "6.0", "true"},
					{"k", "4", "6.0", "true"},
					{"a", "2", "7.1", "false"},
				},
			),
		},
		{
			0,
			LoadRecords(
				[][]string{
					{"A", "B", "C", "D"},
					{"k", "5", "7.0", "true"},
				},
			),
			LoadRecords(
				[][]string{
					{"A", "B", "C", "D"},
					{"k", "5", "7.0", "true"},
					{"b", "4", "6.0", "true"},
					{"c", "3", "6.0", "false"},
					{"a", "2", "7.1", "false"},
				},
			),
		},
		{
			[]bool{true, false, false, false},
			LoadRecords(
				[][]string{
					{"A", "B", "C", "D"},
					{"k", "5", "7.0", "true"},
				},
			),
			LoadRecords(
				[][]string{
					{"A", "B", "C", "D"},
					{"k", "5", "7.0", "true"},
					{"b", "4", "6.0", "true"},
					{"c", "3", "6.0", "false"},
					{"a", "2", "7.1", "false"},
				},
			),
		},
		{
			[]bool{false, true, true, false},
			LoadRecords(
				[][]string{
					{"A", "B", "C", "D"},
					{"k", "5", "7.0", "true"},
					{"k", "4", "6.0", "true"},
				},
			),
			LoadRecords(
				[][]string{
					{"A", "B", "C", "D"},
					{"a", "4", "5.1", "true"},
					{"k", "5", "7.0", "true"},
					{"k", "4", "6.0", "true"},
					{"a", "2", "7.1", "false"},
				},
			),
		},
	}
	for i, tc := range table {
		a := a.Copy()
		b := a.Set(tc.indexes, tc.newvalues)

		if b.Err != nil {
			t.Errorf("Test: %d\nError:%v", i, b.Err)
		}
		// Check that the types are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Types(), b.Types()) {
			t.Errorf("Test: %d\nDifferent types:\nA:%v\nB:%v", i, tc.expDf.Types(), b.Types())
		}
		// Check that the colnames are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Names(), b.Names()) {
			t.Errorf("Test: %d\nDifferent colnames:\nA:%v\nB:%v", i, tc.expDf.Names(), b.Names())
		}
		// Check that the values are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Records(), b.Records()) {
			t.Errorf("Test: %d\nDifferent values:\nA:%v\nB:%v", i, tc.expDf.Records(), b.Records())
		}
	}
}

func TestDataFrame_Arrange(t *testing.T) {
	a := LoadRecords(
		[][]string{
			{"A", "B", "C", "D"},
			{"a", "4", "5.1", "true"},
			{"b", "4", "6.0", "true"},
			{"c", "3", "6.0", "false"},
			{"a", "2", "7.1", "false"},
		},
	)
	table := []struct {
		colnames []Order
		expDf    DataFrame
	}{
		{
			[]Order{Sort("A")},
			LoadRecords(
				[][]string{
					{"A", "B", "C", "D"},
					{"a", "4", "5.1", "true"},
					{"a", "2", "7.1", "false"},
					{"b", "4", "6.0", "true"},
					{"c", "3", "6.0", "false"},
				},
			),
		},
		{
			[]Order{Sort("B")},
			LoadRecords(
				[][]string{
					{"A", "B", "C", "D"},
					{"a", "2", "7.1", "false"},
					{"c", "3", "6.0", "false"},
					{"a", "4", "5.1", "true"},
					{"b", "4", "6.0", "true"},
				},
			),
		},
		{
			[]Order{Sort("A"), Sort("B")},
			LoadRecords(
				[][]string{
					{"A", "B", "C", "D"},
					{"a", "2", "7.1", "false"},
					{"a", "4", "5.1", "true"},
					{"b", "4", "6.0", "true"},
					{"c", "3", "6.0", "false"},
				},
			),
		},
		{
			[]Order{Sort("B"), Sort("A")},
			LoadRecords(
				[][]string{
					{"A", "B", "C", "D"},
					{"a", "2", "7.1", "false"},
					{"c", "3", "6.0", "false"},
					{"a", "4", "5.1", "true"},
					{"b", "4", "6.0", "true"},
				},
			),
		},
		{
			[]Order{RevSort("A")},
			LoadRecords(
				[][]string{
					{"A", "B", "C", "D"},
					{"c", "3", "6.0", "false"},
					{"b", "4", "6.0", "true"},
					{"a", "4", "5.1", "true"},
					{"a", "2", "7.1", "false"},
				},
			),
		},
		{
			[]Order{RevSort("B")},
			LoadRecords(
				[][]string{
					{"A", "B", "C", "D"},
					{"a", "4", "5.1", "true"},
					{"b", "4", "6.0", "true"},
					{"c", "3", "6.0", "false"},
					{"a", "2", "7.1", "false"},
				},
			),
		},
		{
			[]Order{Sort("A"), RevSort("B")},
			LoadRecords(
				[][]string{
					{"A", "B", "C", "D"},
					{"a", "4", "5.1", "true"},
					{"a", "2", "7.1", "false"},
					{"b", "4", "6.0", "true"},
					{"c", "3", "6.0", "false"},
				},
			),
		},
		{
			[]Order{Sort("B"), RevSort("A")},
			LoadRecords(
				[][]string{
					{"A", "B", "C", "D"},
					{"a", "2", "7.1", "false"},
					{"c", "3", "6.0", "false"},
					{"b", "4", "6.0", "true"},
					{"a", "4", "5.1", "true"},
				},
			),
		},
		{
			[]Order{RevSort("B"), RevSort("A")},
			LoadRecords(
				[][]string{
					{"A", "B", "C", "D"},
					{"b", "4", "6.0", "true"},
					{"a", "4", "5.1", "true"},
					{"c", "3", "6.0", "false"},
					{"a", "2", "7.1", "false"},
				},
			),
		},
		{
			[]Order{RevSort("A"), RevSort("B")},
			LoadRecords(
				[][]string{
					{"A", "B", "C", "D"},
					{"c", "3", "6.0", "false"},
					{"b", "4", "6.0", "true"},
					{"a", "4", "5.1", "true"},
					{"a", "2", "7.1", "false"},
				},
			),
		},
	}
	for i, tc := range table {
		b := a.Arrange(tc.colnames...)

		if b.Err != nil {
			t.Errorf("Test: %d\nError:%v", i, b.Err)
		}
		//if err := checkAddrDf(a, b); err != nil {
		//t.Error(err)
		//}
		// Check that the types are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Types(), b.Types()) {
			t.Errorf("Test: %d\nDifferent types:\nA:%v\nB:%v", i, tc.expDf.Types(), b.Types())
		}
		// Check that the colnames are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Names(), b.Names()) {
			t.Errorf("Test: %d\nDifferent colnames:\nA:%v\nB:%v", i, tc.expDf.Names(), b.Names())
		}
		// Check that the values are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Records(), b.Records()) {
			t.Errorf("Test: %d\nDifferent values:\nA:%v\nB:%v", i, tc.expDf.Records(), b.Records())
		}
	}
}

func TestDataFrame_Capply(t *testing.T) {
	a := LoadRecords(
		[][]string{
			{"A", "B", "C", "D"},
			{"a", "4", "5.1", "true"},
			{"b", "4", "6.0", "true"},
			{"c", "3", "6.0", "false"},
			{"a", "2", "7.1", "false"},
		},
	)
	mean := func(s series.Series) series.Series {
		floats := s.Float()
		sum := 0.0
		for _, f := range floats {
			sum += f
		}
		return series.Floats(sum / float64(len(floats)))
	}
	sum := func(s series.Series) series.Series {
		floats := s.Float()
		sum := 0.0
		for _, f := range floats {
			sum += f
		}
		return series.Floats(sum)
	}
	table := []struct {
		fun   func(series.Series) series.Series
		expDf DataFrame
	}{
		{
			mean,
			LoadRecords(
				[][]string{
					{"A", "B", "C", "D"},
					{"NaN", "3.25", "6.05", "0.5"},
				},
				DefaultType(series.Float),
				DetectTypes(false),
			),
		},
		{
			sum,
			LoadRecords(
				[][]string{
					{"A", "B", "C", "D"},
					{"NaN", "13", "24.2", "2"},
				},
				DefaultType(series.Float),
				DetectTypes(false),
			),
		},
	}
	for i, tc := range table {
		b := a.Capply(tc.fun)

		if b.Err != nil {
			t.Errorf("Test: %d\nError:%v", i, b.Err)
		}
		//if err := checkAddrDf(a, b); err != nil {
		//t.Error(err)
		//}
		// Check that the types are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Types(), b.Types()) {
			t.Errorf("Test: %d\nDifferent types:\nA:%v\nB:%v", i, tc.expDf.Types(), b.Types())
		}
		// Check that the colnames are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Names(), b.Names()) {
			t.Errorf("Test: %d\nDifferent colnames:\nA:%v\nB:%v", i, tc.expDf.Names(), b.Names())
		}
		// Check that the values are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Records(), b.Records()) {
			t.Errorf("Test: %d\nDifferent values:\nA:%v\nB:%v", i, tc.expDf.Records(), b.Records())
		}
	}
}

func TestDataFrame_String(t *testing.T) {
	a := LoadRecords(
		[][]string{
			{"A", "C", "D"},
			{"1", "5.1", "true"},
			{"NaN", "6.0", "true"},
			{"2", "6.0", "false"},
			{"2", "7.1", "false"},
		},
	)
	received := a.String()
	expected := `[4x3] DataFrame

    A     C        D     
 0: 1     5.100000 true  
 1: NaN   6.000000 true  
 2: 2     6.000000 false 
 3: 2     7.100000 false 
    <int> <float>  <bool>
`
	if expected != received {
		t.Errorf("Different values:\nExpected: \n%v\nReceived: \n%v\n", expected, received)
	}
}

func TestDataFrame_Rapply(t *testing.T) {
	a := LoadRecords(
		[][]string{
			{"A", "B", "C", "D"},
			{"1", "4", "5.1", "1"},
			{"1", "4", "6.0", "1"},
			{"2", "3", "6.0", "0"},
			{"2", "2", "7.1", "0"},
		},
	)
	mean := func(s series.Series) series.Series {
		floats := s.Float()
		sum := 0.0
		for _, f := range floats {
			sum += f
		}
		ret := series.Floats(sum / float64(len(floats)))
		return ret
	}
	sum := func(s series.Series) series.Series {
		floats := s.Float()
		sum := 0.0
		for _, f := range floats {
			sum += f
		}
		return series.Floats(sum)
	}
	table := []struct {
		fun   func(series.Series) series.Series
		expDf DataFrame
	}{
		{
			mean,
			LoadRecords(
				[][]string{
					{"X0"},
					{"2.775"},
					{"3"},
					{"2.75"},
					{"2.775"},
				},
				DefaultType(series.Float),
				DetectTypes(false),
			),
		},
		{
			sum,
			LoadRecords(
				[][]string{
					{"X0"},
					{"11.1"},
					{"12"},
					{"11"},
					{"11.1"},
				},
				DefaultType(series.Float),
				DetectTypes(false),
			),
		},
	}
	for i, tc := range table {
		b := a.Rapply(tc.fun)

		if b.Err != nil {
			t.Errorf("Test: %d\nError:%v", i, b.Err)
		}
		//if err := checkAddrDf(a, b); err != nil {
		//t.Error(err)
		//}
		// Check that the types are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Types(), b.Types()) {
			t.Errorf("Test: %d\nDifferent types:\nA:%v\nB:%v", i, tc.expDf.Types(), b.Types())
		}
		// Check that the colnames are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Names(), b.Names()) {
			t.Errorf("Test: %d\nDifferent colnames:\nA:%v\nB:%v", i, tc.expDf.Names(), b.Names())
		}
		// Check that the values are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Records(), b.Records()) {
			t.Errorf("Test: %d\nDifferent values:\nA:%v\nB:%v", i, tc.expDf.Records(), b.Records())
		}
	}
}

type mockMatrix struct {
	DataFrame
}

func (m mockMatrix) At(i, j int) float64 {
	return m.columns[j].Elem(i).Float()
}

func (m mockMatrix) T() Matrix {
	return m
}

func TestLoadMatrix(t *testing.T) {
	table := []struct {
		b     DataFrame
		expDf DataFrame
	}{
		{
			LoadRecords(
				[][]string{
					{"A", "B", "C", "D"},
					{"4", "1", "true", "0"},
					{"3", "2", "true", "0.5"},
				},
			),
			New(
				series.New([]string{"4", "3"}, series.Float, "X0"),
				series.New([]int{1, 2}, series.Float, "X1"),
				series.New([]bool{true, true}, series.Float, "X2"),
				series.New([]float64{0, 0.5}, series.Float, "X3"),
			),
		},
	}
	for i, tc := range table {
		b := LoadMatrix(mockMatrix{tc.b})

		if b.Err != nil {
			t.Errorf("Test: %d\nError:%v", i, b.Err)
		}
		// Check that the types are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Types(), b.Types()) {
			t.Errorf("Test: %d\nDifferent types:\nA:%v\nB:%v", i, tc.expDf.Types(), b.Types())
		}
		// Check that the colnames are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Names(), b.Names()) {
			t.Errorf("Test: %d\nDifferent colnames:\nA:%v\nB:%v", i, tc.expDf.Names(), b.Names())
		}
		// Check that the values are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Records(), b.Records()) {
			t.Errorf("Test: %d\nDifferent values:\nA:%v\nB:%v", i, tc.expDf.Records(), b.Records())
		}
	}
}

func TestLoadStructs(t *testing.T) {
	type testStruct struct {
		A string
		B int
		C bool
		D float64
	}
	type testStructTags struct {
		A string  `dataframe:"a,string"`
		B int     `dataframe:"b,string"`
		C bool    `dataframe:"c,string"`
		D float64 `dataframe:"d,string"`
		E int     `dataframe:"-"` // ignored
		f int     // ignored
	}
	data := []testStruct{
		{"a", 1, true, 0.0},
		{"b", 2, true, 0.5},
	}
	dataTags := []testStructTags{
		{"a", 1, true, 0.0, 0, 0},
		{"NA", 2, true, 0.5, 0, 0},
	}
	table := []struct {
		b     DataFrame
		expDf DataFrame
	}{
		{
			LoadStructs(dataTags),
			New(
				series.New([]string{"a", "NaN"}, series.String, "a"),
				series.New([]int{1, 2}, series.String, "b"),
				series.New([]bool{true, true}, series.String, "c"),
				series.New([]string{"0.000000", "0.500000"}, series.String, "d"),
			),
		},
		{
			LoadStructs(data),
			New(
				series.New([]string{"a", "b"}, series.String, "A"),
				series.New([]int{1, 2}, series.Int, "B"),
				series.New([]bool{true, true}, series.Bool, "C"),
				series.New([]float64{0, 0.5}, series.Float, "D"),
			),
		},
		{
			LoadStructs(
				data,
				HasHeader(true),
				DetectTypes(false),
				DefaultType(series.String),
			),
			New(
				series.New([]string{"a", "b"}, series.String, "A"),
				series.New([]int{1, 2}, series.String, "B"),
				series.New([]bool{true, true}, series.String, "C"),
				series.New([]string{"0.000000", "0.500000"}, series.String, "D"),
			),
		},
		{
			LoadStructs(
				data,
				HasHeader(false),
				DetectTypes(false),
				DefaultType(series.String),
			),
			New(
				series.New([]string{"A", "a", "b"}, series.String, "X0"),
				series.New([]string{"B", "1", "2"}, series.String, "X1"),
				series.New([]string{"C", "true", "true"}, series.String, "X2"),
				series.New([]string{"D", "0.000000", "0.500000"}, series.String, "X3"),
			),
		},
		{
			LoadStructs(
				data,
				HasHeader(true),
				DetectTypes(false),
				DefaultType(series.String),
				WithTypes(map[string]series.Type{
					"B": series.Float,
					"C": series.String,
				}),
			),
			New(
				series.New([]string{"a", "b"}, series.String, "A"),
				series.New([]float64{1, 2}, series.Float, "B"),
				series.New([]bool{true, true}, series.String, "C"),
				series.New([]string{"0.000000", "0.500000"}, series.String, "D"),
			),
		},
		{
			LoadStructs(
				data,
				HasHeader(true),
				DetectTypes(true),
				DefaultType(series.String),
				WithTypes(map[string]series.Type{
					"B": series.Float,
				}),
			),
			New(
				series.New([]string{"a", "b"}, series.String, "A"),
				series.New([]float64{1, 2}, series.Float, "B"),
				series.New([]bool{true, true}, series.Bool, "C"),
				series.New([]string{"0", "0.5"}, series.Float, "D"),
			),
		},
	}
	for i, tc := range table {
		if tc.b.Err != nil {
			t.Errorf("Test: %d\nError:%v", i, tc.b.Err)
		}
		// Check that the types are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Types(), tc.b.Types()) {
			t.Errorf("Test: %d\nDifferent types:\nA:%v\nB:%v", i, tc.expDf.Types(), tc.b.Types())
		}
		// Check that the colnames are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Names(), tc.b.Names()) {
			t.Errorf("Test: %d\nDifferent colnames:\nA:%v\nB:%v", i, tc.expDf.Names(), tc.b.Names())
		}
		// Check that the values are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Records(), tc.b.Records()) {
			t.Errorf("Test: %d: Different values:\nA:%v\nB:%v", i, tc.expDf, tc.b)
		}
	}
}

func TestDescribe(t *testing.T) {
	table := []struct {
		df       DataFrame
		expected DataFrame
	}{
		{
			LoadRecords(
				[][]string{
					[]string{"A", "B", "C", "D"},
					[]string{"a", "4", "5.1", "true"},
					[]string{"b", "4", "6.0", "true"},
					[]string{"c", "3", "6.0", "false"},
					[]string{"a", "2", "7.1", "false"},
				}),

			New(
				series.New(
					[]string{"mean", "median", "stddev", "min", "25%", "50%", "75%", "max"},
					series.String,
					"",
				),
				series.New(
					[]string{"-", "-", "-", "a", "-", "-", "-", "c"},
					series.String,
					"A",
				),
				series.New(
					[]float64{3.25, 3.5, 0.957427, 2.0, 2.0, 3.0, 4.0, 4.0},
					series.Float,
					"B",
				),
				series.New(
					[]float64{6.05, 6., 0.818535, 5.1, 5.1, 6.0, 6.0, 7.1},
					series.Float,
					"C",
				),
				series.New(
					[]float64{0.5, math.NaN(), 0.57735, 0.0, 0.0, 0.0, 1.0, 1.0},
					series.Float,
					"D",
				),
			),
		},
	}

	for testnum, test := range table {
		received := test.df.Describe()
		expected := test.expected

		equal := true
		for i, col := range received.columns {
			lcol := col.Records()
			rcol := expected.columns[i].Records()
			for j, value := range lcol {
				lvalue, lerr := strconv.ParseFloat(value, 64)
				rvalue, rerr := strconv.ParseFloat(rcol[j], 64)
				if lerr != nil || rerr != nil {
					equal = lvalue == rvalue
				} else {
					equal = compareFloats(lvalue, rvalue, 6)
				}
				if !equal {
					break
				}
			}
			if !equal {
				break
			}
		}

		if !equal {
			t.Errorf("Test:%v\nExpected:\n%v\nReceived:\n%v\n", testnum, expected, received)
		}
	}
}

func TestDataFrame_Insert(t *testing.T) {
	tests := []struct {
		desc     string
		df       DataFrame
		value    DataFrame
		pos      int
		expected DataFrame
	}{
		{
			"TestDataFrame_Insert:0: DataframeString.Insert(DataframeString) & pos=end of Series",
			LoadRecords(
				[][]string{
					{"A", "C", "D"},
					{"1", "5.1", "true"},
					{"NaN", "6.0", "true"},
					{"2", "6.0", "false"},
				},
			),
			LoadRecords(
				[][]string{
					{"A", "C", "D"},
					{"2", "7.1", "false"},
				},
			),
			-1,
			LoadRecords(
				[][]string{
					{"A", "C", "D"},
					{"1", "5.1", "true"},
					{"NaN", "6.0", "true"},
					{"2", "6.0", "false"},
					{"2", "7.1", "false"},
				},
			),
		},
		{
			"TestDataFrame_Insert:1: DataFrameString.Insert(DataFrameString) & pos=0",
			LoadRecords(
				[][]string{
					{"A", "C", "D"},
					{"1", "5.1", "true"},
					{"NaN", "6.0", "true"},
					{"2", "6.0", "false"},
				},
			),
			LoadRecords(
				[][]string{
					{"A", "C", "D"},
					{"2", "7.1", "false"},
				},
			),
			0,
			LoadRecords(
				[][]string{
					{"A", "C", "D"},
					{"2", "7.1", "false"},
					{"1", "5.1", "true"},
					{"NaN", "6.0", "true"},
					{"2", "6.0", "false"},
				},
			),
		},
	}

	for i, test := range tests {
		actual := test.df.Insert(test.value, test.pos)

		if test.df.Err != nil {
			t.Errorf("Test: %d\nError:%v", i, test.df.Err)
		}
		// Check that the types are the same between both DataFrames
		if !reflect.DeepEqual(test.expected.Types(), actual.Types()) {
			t.Errorf("Test: %d\nDifferent types:\nexpected:%v\nactual:%v", i, test.expected.Types(), actual.Types())
		}
		// Check that the colnames are the same between both DataFrames
		if !reflect.DeepEqual(test.expected.Names(), actual.Names()) {
			t.Errorf("Test: %d\nDifferent colnames:\nexpected:%v\nactual:%v", i, test.expected.Names(), actual.Names())
		}
		// Check that the values are the same between both DataFrames
		if !reflect.DeepEqual(test.expected.Records(), actual.Records()) {
			t.Errorf("Test: %d: Different values:\nexpected:%v\nactual:%v", i, test.expected, actual)
		}
	}
}
