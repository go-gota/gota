package dataframe

import (
	"reflect"
	"testing"

	"github.com/isuruceanu/gota/series"
)

func TestDataFrame_Summarize(t *testing.T) {
	a := LoadRecords(
		[][]string{
			[]string{"G", "D", "C", "W"},
			[]string{"M", "A", "Z", "21.1"},
			[]string{"F", "A", "Z", "15.1"},
			[]string{"F", "B", "X", "18.2"},
			[]string{"M", "C", "Z", "12.1"},
			[]string{"M", "C", "X", "8.1"},

			[]string{"M", "A", "Z", "21.1"},
			[]string{"F", "A", "Z", "15.1"},
			[]string{"F", "B", "X", "18.2"},
			[]string{"M", "C", "Z", "12.1"},
			[]string{"M", "C", "X", "8.1"},

			[]string{"M", "B", "Z", "21.1"},
			[]string{"F", "C", "Z", "15.1"},
			[]string{"F", "B", "X", "18.2"},
			[]string{"M", "C", "Z", "12.1"},
			[]string{"M", "C", "X", "8.1"},
		},
	)

	expectedDf := LoadRecords(
		[][]string{
			[]string{"G", "D", "X0", "X1"},
			[]string{"F", "A", "2.", "15.1"},
			[]string{"F", "B", "3.", "18.2"},
			[]string{"F", "C", "1.", "15.1"},
			[]string{"M", "A", "2.", "21.1"},
			[]string{"M", "B", "1.", "21.1"},
			[]string{"M", "C", "6.", "10.1"},
		})

	g := a.Group("G", "D")

	r := g.Summarize(summary)

	if err := r.Err; err != nil {
		t.Errorf("Test Error:%v", err)
	}
	if err := checkAddrDf(a, r); err != nil {
		t.Error(err)
	}
	// Check that the types are the same between both DataFrames
	if !reflect.DeepEqual(expectedDf.Types(), r.Types()) {
		t.Errorf("Different types:\nA:%v\nB:%v", expectedDf.Types(), r.Types())
	}
	// Check that the colnames are the same between both DataFrames
	if !reflect.DeepEqual(expectedDf.Names(), r.Names()) {
		t.Errorf("Different colnames:\nA:%v\nB:%v", expectedDf.Names(), r.Names())
	}
	// Check that the values are the same between both DataFrames
	if !reflect.DeepEqual(expectedDf.Records(), r.Records()) {
		t.Errorf("Different values:\nA:%v\nB:%v", expectedDf.Records(), r.Records())
	}

}

func summary(ds DataFrame) series.Series {
	count := float64(ds.Nrow())
	mean := ds.Col("W").Mean()

	return series.Floats([]float64{count, mean})
}
