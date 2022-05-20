package dataframe

import (
	"reflect"
	"testing"

	"github.com/mqy527/gota/series"
)

func TestDataFrame_Self_AppendColumns(t *testing.T) {
	a := New(
		series.New([]string{"b", "a", "b", "c", "d"}, series.String, "COL.1"),
		series.New([]int{1, 2, 4, 5, 4}, series.Int, "COL.2"),
		series.New([]float64{3.0, 4.0, 5.3, 3.2, 1.2}, series.Float, "COL.3"),
	)
	table := []struct {
		s     series.Series
		s1    series.Series
		expDf DataFrame
	}{
		{
			series.New([]string{"A", "B", "A", "A", "A"}, series.String, "COL.1"),
			series.New([]int{2, 3, 5, 6, 7}, series.String, "COL.3"),
			New(
				series.New([]string{"b", "a", "b", "c", "d"}, series.String, "COL.1_0"),
				series.New([]int{1, 2, 4, 5, 4}, series.Int, "COL.2"),
				series.New([]float64{3.0, 4.0, 5.3, 3.2, 1.2}, series.Float, "COL.3_0"),
				series.New([]string{"A", "B", "A", "A", "A"}, series.String, "COL.1_1"),
				series.New([]int{2, 3, 5, 6, 7}, series.String, "COL.3_1"),
			),
		},
		{
			series.New([]string{"A", "B", "A", "A", "A"}, series.String, "COL.2"),
			series.New([]string{"w", "e", "r", "t", "y"}, series.String, "COL.1"),
			New(
				series.New([]string{"b", "a", "b", "c", "d"}, series.String, "COL.1_0"),
				series.New([]int{1, 2, 4, 5, 4}, series.Int, "COL.2_0"),
				series.New([]float64{3.0, 4.0, 5.3, 3.2, 1.2}, series.Float, "COL.3"),
				series.New([]string{"A", "B", "A", "A", "A"}, series.String, "COL.2_1"),
				series.New([]string{"w", "e", "r", "t", "y"}, series.String, "COL.1_1"),
			),
		},
		{
			series.New([]string{"A", "B", "A", "A", "A"}, series.String, "COL.4"),
			series.New([]int{2, 3, 5, 6, 7}, series.String, "COL.5"),
			New(
				series.New([]string{"b", "a", "b", "c", "d"}, series.String, "COL.1"),
				series.New([]int{1, 2, 4, 5, 4}, series.Int, "COL.2"),
				series.New([]float64{3.0, 4.0, 5.3, 3.2, 1.2}, series.Float, "COL.3"),
				series.New([]string{"A", "B", "A", "A", "A"}, series.String, "COL.4"),
				series.New([]int{2, 3, 5, 6, 7}, series.String, "COL.5"),
			),
		},
		{
			series.New([]string{"A", "B", "A", "A", "A"}, series.String, "COL.4"),
			series.New([]float64{3.3, 4.3, 5.3, 5.5, 6.4}, series.Float, "COL.5"),
			New(
				series.New([]string{"b", "a", "b", "c", "d"}, series.String, "COL.1"),
				series.New([]int{1, 2, 4, 5, 4}, series.Int, "COL.2"),
				series.New([]float64{3.0, 4.0, 5.3, 3.2, 1.2}, series.Float, "COL.3"),
				series.New([]string{"A", "B", "A", "A", "A"}, series.String, "COL.4"),
				series.New([]float64{3.3, 4.3, 5.3, 5.5, 6.4}, series.Float, "COL.5"),
			),
		},
	}
	for i, tc := range table {
		b := a.Copy()
		
		b.Self().AppendColumns(tc.s, tc.s1)

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

func TestDataFrame_Self_Capply(t *testing.T) {
	a := LoadRecords(
		[][]string{
			{"A", "B", "C", "D"},
			{"a", "4", "5.1", series.NaN},
			{"b", series.NaN, "6.0", "true"},
			{"c", "3", "6.0", series.NaN},
			{series.NaN, "2", "7.1", "false"},
		},
	)
	fillNaNForward := func(s series.Series) {
		s.FillNaNForward()
	}
	table := []struct {
		fun   func(series.Series)
		expDf DataFrame
	}{
		{
			fillNaNForward,
			LoadRecords(
				[][]string{
					{"A", "B", "C", "D"},
					{"a", "4", "5.1", series.NaN},
					{"b", "4", "6.0", "true"},
					{"c", "3", "6.0", "true"},
					{"c", "2", "7.1", "false"},
				},
			),
		},
	}
	for i, tc := range table {
		b := a.Copy()
		b.Self().Capply(tc.fun)

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