package dataframe

import (
	"fmt"
	"testing"

	"github.com/isuruceanu/gota/series"
)

func TestOuterJoinMergeWithCombine(t *testing.T) {
	result := LoadRecords(
		[][]string{
			[]string{"UID", "Gender", "HEXO", "Bonus"},
			[]string{"1", "M", "17.0", "20.0"},
			[]string{"2", "F", "18.0", "28.0"},
			[]string{"3", "M", "16.2", "NA"},
			[]string{"4", "F", "23.0", "NA"},
			[]string{"5", "F", "NA", "18.0"},
		},
	)

	third := first.Merge(second, "UID").WithCombine(compareFn).OuterJoin()
	if third.Err != nil {
		t.Error(third.Err)
	}

	fmt.Println(third.String())

	if third.String() != result.String() {
		t.Error("Result dataset differs from expected")
	}
}

func TestRightJoinMergeWithCombine(t *testing.T) {
	result := LoadRecords(
		[][]string{
			[]string{"UID", "Gender", "HEXO", "Bonus"},
			[]string{"1", "M", "17.0", "20.0"},
			[]string{"2", "F", "18.0", "28.0"},
			[]string{"5", "F", "NA", "18.0"},
		},
	)

	third := first.Merge(second, "UID").WithCombine(compareFn).RightJoin()
	if third.Err != nil {
		t.Error(third.Err)
	}

	fmt.Println(third.String())

	if third.String() != result.String() {
		t.Error("Result dataset differs from expected")
	}
}

func TestInnerJoinMergeWithCombine(t *testing.T) {
	result := LoadRecords(
		[][]string{
			[]string{"UID", "Gender", "HEXO", "Bonus"},
			[]string{"1", "M", "17.0", "20.0"},
			[]string{"2", "F", "18.0", "28.0"},
		},
	)

	third := first.Merge(second, "UID").WithCombine(compareFn).InnerJoin()
	if third.Err != nil {
		t.Error(third.Err)
	}

	fmt.Println(third.String())

	if third.String() != result.String() {
		t.Error("Result dataset differs from expected")
	}
}

func TestLeftMergeWithCombine(t *testing.T) {

	result := LoadRecords(
		[][]string{
			[]string{"UID", "Gender", "HEXO", "Bonus"},
			[]string{"1", "M", "17.0", "20.0"},
			[]string{"2", "F", "18.0", "28.0"},
			[]string{"3", "M", "16.2", "NA"},
			[]string{"4", "F", "23.0", "NA"},
		},
	)

	third := first.Merge(second, "UID").WithCombine(compareFn).LeftJoin()
	if third.Err != nil {
		t.Error(third.Err)
	}

	if third.String() != result.String() {
		t.Error("Result dataset differs from expected")
	}
}

var compareFn = func(a, b series.Series) bool {
	return a.Name == b.Name
}

var (
	first = LoadRecords(
		[][]string{
			[]string{"Gender", "HEXO", "UID"},
			[]string{"M", "17.0", "1"},
			[]string{"NA", "18.0", "2"},
			[]string{"M", "16.2", "3"},
			[]string{"F", "23.0", "4"},
		},
	)

	second = LoadRecords(
		[][]string{
			[]string{"Gender", "Bonus", "UID"},
			[]string{"NA", "20.0", "1"},
			[]string{"F", "28.0", "2"},
			[]string{"F", "18.0", "5"},
		},
	)
)
