package dataframe

import (
	"fmt"
	"testing"

	"github.com/isuruceanu/gota/series"
)

func TestOuterJoinMergeWithCombine(t *testing.T) {
	result := LoadRecords(
		[][]string{
			[]string{"UID", "Gender", "HEXO", "Age", "WWH", "Bonus"},
			[]string{"1", "M", "100.0", "18", "40", "25.0"},
			[]string{"2", "F", "100.0", "20", "40", "25.0"},
			[]string{"3", "M", "100.0", "21", "40", "25.0"},
			[]string{"4", "F", "100.0", "22", "40", "NA"},
			[]string{"5", "F", "NA", "15", "NA", "25.0"},
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
			[]string{"UID", "Gender", "HEXO", "Age", "WWH", "Bonus"},
			[]string{"1", "M", "100.0", "18", "40", "25.0"},
			[]string{"2", "F", "100.0", "20", "40", "25.0"},
			[]string{"3", "M", "100.0", "21", "40", "25.0"},
			[]string{"5", "F", "NA", "15", "NA", "25.0"},
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
			[]string{"UID", "Gender", "HEXO", "Age", "WWH", "Bonus"},
			[]string{"1", "M", "100.0", "18", "40", "25.0"},
			[]string{"2", "F", "100.0", "20", "40", "25.0"},
			[]string{"3", "M", "100.0", "21", "40", "25.0"},
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
			[]string{"UID", "Gender", "HEXO", "Age", "WWH", "Bonus"},
			[]string{"1", "M", "100.0", "18", "40", "25.0"},
			[]string{"2", "F", "100.0", "20", "40", "25.0"},
			[]string{"3", "M", "100.0", "21", "40", "25.0"},
			[]string{"4", "F", "100.0", "22", "40", "NA"},
		},
	)

	third := first.Merge(second, "UID").WithCombine(compareFn).LeftJoin()
	if third.Err != nil {
		t.Error(third.Err)
	}

	fmt.Println(third.String())
	if third.String() != result.String() {
		t.Error("Result dataset differs from expected")
	}
}

func TestMergeWithCombineWithChangeHeader(t *testing.T) {
	result := LoadRecords(
		[][]string{
			[]string{"UID", "OtherGenderName", "HEXO", "Age", "WWH", "Bonus"},
			[]string{"1", "M", "100.0", "18", "40", "25.0"},
			[]string{"2", "F", "100.0", "20", "40", "25.0"},
			[]string{"3", "M", "100.0", "21", "40", "25.0"},
			[]string{"4", "F", "100.0", "22", "40", "NA"},
		},
	)

	headerFn := func(a, b series.Series) (string, interface{}, bool) {
		if a.Name == "Gender" {
			return "OtherGenderName", "some data here", false
		}
		return "", nil, true
	}

	third := first.Merge(second, "UID").WithCombine(compareFn).WithResultHeader(headerFn).LeftJoin()
	if third.Err != nil {
		t.Error(third.Err)
	}

	fmt.Println(third.String())
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
			[]string{"UID", "Gender", "HEXO", "Age", "WWH"},
			[]string{"1", "M", "100.0", "18", "40"},
			[]string{"2", "F", "100.0", "20", "40"},
			[]string{"3", "M", "100.0", "21", "40"},
			[]string{"4", "F", "100.0", "22", "40"},
		},
	)

	second = LoadRecords(
		[][]string{
			[]string{"UID", "Gender", "Age", "Bonus"},
			[]string{"1", "M", "18", "25.0"},
			[]string{"2", "F", "17", "25.0"},
			[]string{"3", "M", "16", "25.0"},
			[]string{"5", "F", "15", "25.0"},
		},
	)
)
