package df

import (
	"fmt"
	"testing"
)

func TestNew(t *testing.T) {
	df, err := New(
		C{"A", Strings("aa", "b")},
		C{"B", Strings("a", "bbb")},
	)
	if err != nil {
		t.Error("Error when creating DataFrame:", err)
	}
	expected := "   A   B    \n\n0: aa  a    \n1: b   bbb  \n"
	received := fmt.Sprint(df)
	if expected != received {
		t.Error(
			"DataFrame created by New() is not correct",
			"Expected:\n",
			expected, "\n",
			"Received:\n",
			received,
		)
	}

	df, err = New()
	if err == nil {
		t.Error("Error when creating DataFrame not being thrown")
	}

	df, err = New(
		C{"A", Strings("a", "b")},
		C{"B", Strings("a", "b", "c")},
	)
	if err == nil {
		t.Error("Error when creating DataFrame not being thrown")
	}

	df, err = New(
		C{"A", Strings()},
		C{"B", Strings("a", "b", "c")},
	)
	if err == nil {
		t.Error("Error when creating DataFrame not being thrown")
	}
}

func TestDataFrame_LoadData(t *testing.T) {
	data := [][]string{
		[]string{"A", "B", "C", "D"},
		[]string{"1", "2", "3", "4"},
		[]string{"5", "6", "7", "8"},
	}

	// Test correct data loading
	df := DataFrame{}
	df.LoadData(data)
	expected := "   A  B  C  D  \n\n0: 1  2  3  4  \n1: 5  6  7  8  \n"
	received := fmt.Sprint(df)
	if expected != received {
		t.Error(
			"DataFrame loaded data incorrectly",
			"Expected:\n",
			expected, "\n",
			"Received:\n",
			received,
		)
	}

	// Test nil data loading
	err := df.LoadData(nil)
	if err == nil {
		t.Error("DataFrame should have failed")
	}

	// Test empty headers
	data = [][]string{
		[]string{"", "", "", ""},
		[]string{"1", "2", "3", "4"},
		[]string{"5", "6", "7", "8"},
	}
	df.LoadData(data)
	expectedColnames := fmt.Sprint([]string{"V0", "V1", "V2", "V3"})
	receivedColnames := fmt.Sprint(df.colNames)
	if expectedColnames != receivedColnames {
		t.Error(
			"Colnames not being generated properly",
			"Expected:\n",
			expectedColnames, "\n",
			"Received:\n",
			receivedColnames,
		)
	}

	// Test duplicated headers
	data = [][]string{
		[]string{"A", "B", "A", "C"},
		[]string{"1", "2", "3", "4"},
		[]string{"5", "6", "7", "8"},
	}
	err = df.LoadData(data)
	if err == nil {
		t.Error("Duplicated headers but no error")
	}
}

func TestDataFrame_LoadAndParse(t *testing.T) {
	data := [][]string{
		[]string{"A", "B", "C", "D"},
		[]string{"1", "2", "3", "4"},
		[]string{"5", "6", "7", "8"},
	}

	// Test parsing two columns as integers
	df := DataFrame{}
	df.LoadAndParse(data, T{"A": "int", "C": "int"})
	if fmt.Sprint(df.colTypes) != "[df.Int df.String df.Int df.String]" {
		t.Error("Incorrect type parsing")
	}
}

func TestDataFrame_SaveRecords(t *testing.T) {
	data := [][]string{
		[]string{"A", "B", "C", "D"},
		[]string{"1", "2", "3", "4"},
		[]string{"5", "6", "7", "8"},
	}

	// Test parsing two columns as integers
	df := DataFrame{}
	df.LoadData(data)
	datab := df.SaveRecords()
	if fmt.Sprint(data) != fmt.Sprint(datab) {
		t.Error("Recovered records differ from original")
	}
}

func TestDataFrame_SubsetColumns(t *testing.T) {
	data := [][]string{
		[]string{"A", "B", "C", "D"},
		[]string{"1", "2", "3", "4"},
		[]string{"5", "6", "7", "8"},
	}

	// Test parsing two columns as integers
	df := DataFrame{}
	df.LoadData(data)

	// Subset by column and rearrange the columns by name on the given order
	_, err := df.SubsetColumns([]string{"A", "B"})
	if err != nil {
		t.Error(err)
	}

	// Subset by column using a range element
	_, err = df.SubsetColumns(R{0, 3})
	if err != nil {
		t.Error(err)
	}

	// Subset by column using an array of column numbers
	_, err = df.SubsetColumns([]int{0, 3, 1})
	if err != nil {
		t.Error(err)
	}
}

func TestDataFrame_SubsetRows(t *testing.T) {
	data := [][]string{
		[]string{"A", "B", "C", "D"},
		[]string{"1", "2", "3", "4"},
		[]string{"5", "6", "7", "8"},
		[]string{"9", "10", "11", "12"},
	}

	// Test parsing two columns as integers
	df := DataFrame{}
	df.LoadData(data)

	// Subset by column using a range element
	_, err := df.SubsetRows(R{1, 2})
	if err != nil {
		t.Error(err)
	}

	// Subset by column using an array of column numbers
	_, err = df.SubsetRows([]int{0, 2, 1})
	if err != nil {
		t.Error(err)
	}
}

func TestDataFrame_Rbind(t *testing.T) {
	dataA := [][]string{
		[]string{"A", "B", "C", "D"},
		[]string{"1", "2", "3", "4"},
		[]string{"5", "6", "7", "8"},
		[]string{"9", "10", "11", "12"},
	}
	dataB := [][]string{
		[]string{"B", "A", "D", "C"},
		[]string{"1", "2", "3", "4"},
		[]string{"5", "6", "7", "8"},
		[]string{"9", "10", "11", "12"},
	}

	// Test parsing two columns as integers
	dfA := DataFrame{}
	dfA.LoadData(dataA)
	dfB := DataFrame{}
	dfB.LoadData(dataB)

	_, err := Rbind(dfA, dfB)
	if err != nil {
		t.Error(err)
	}
}

func TestDataFrame_Cbind(t *testing.T) {
	dataA := [][]string{
		[]string{"A", "B"},
		[]string{"1", "2"},
		[]string{"5", "6"},
		[]string{"9", "10"},
	}
	dataB := [][]string{
		[]string{"C", "D"},
		[]string{"3", "4"},
		[]string{"7", "8"},
		[]string{"11", "12"},
	}

	// Test parsing two columns as integers
	dfA := DataFrame{}
	dfA.LoadData(dataA)
	dfB := DataFrame{}
	dfB.LoadData(dataB)

	_, err := Cbind(dfA, dfB)
	if err != nil {
		t.Error(err)
	}
}

func TestDataFrame_Unique(t *testing.T) {
	data := [][]string{
		[]string{"A", "B", "C", "D"},
		[]string{"1", "2", "3", "4"},
		[]string{"5", "6", "7", "8"},
		[]string{"1", "2", "3", "4"},
		[]string{"9", "10", "11", "12"},
		[]string{"9", "10", "11", "12"},
		[]string{"9", "10", "11", "12"},
		[]string{"5", "7", "7", "8"},
	}

	// Test parsing two columns as integers
	df := DataFrame{}
	df.LoadData(data)

	_, err := df.Unique()
	if err != nil {
		t.Error(err)
	}
}

func TestDataFrame_Duplicated(t *testing.T) {
	data := [][]string{
		[]string{"A", "B", "C", "D"},
		[]string{"1", "2", "3", "4"},
		[]string{"5", "6", "7", "8"},
		[]string{"1", "2", "3", "4"},
		[]string{"9", "10", "11", "12"},
		[]string{"9", "10", "11", "12"},
		[]string{"9", "10", "11", "12"},
		[]string{"5", "7", "7", "8"},
	}

	// Test parsing two columns as integers
	df := DataFrame{}
	df.LoadData(data)

	_, err := df.Duplicated()
	if err != nil {
		t.Error(err)
	}
}

func TestDataFrame_RemoveDuplicated(t *testing.T) {
	data := [][]string{
		[]string{"A", "B", "C", "D"},
		[]string{"1", "2", "3", "4"},
		[]string{"5", "6", "7", "8"},
		[]string{"1", "2", "3", "4"},
		[]string{"9", "10", "11", "12"},
		[]string{"9", "10", "11", "12"},
		[]string{"9", "10", "11", "12"},
		[]string{"5", "7", "7", "8"},
	}

	// Test parsing two columns as integers
	df := DataFrame{}
	df.LoadData(data)

	_, err := df.RemoveDuplicated()
	if err != nil {
		t.Error(err)
	}
}

func TestDataFrame_RemoveUnique(t *testing.T) {
	data := [][]string{
		[]string{"A", "B", "C", "D"},
		[]string{"1", "2", "3", "4"},
		[]string{"5", "6", "7", "8"},
		[]string{"1", "2", "3", "4"},
		[]string{"9", "10", "11", "12"},
		[]string{"9", "10", "11", "12"},
		[]string{"9", "10", "11", "12"},
		[]string{"5", "7", "7", "8"},
	}

	// Test parsing two columns as integers
	df := DataFrame{}
	df.LoadData(data)

	_, err := df.RemoveUnique()
	if err != nil {
		t.Error(err)
	}
}
