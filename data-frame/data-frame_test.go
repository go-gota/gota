package df

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestDataFrame_New(t *testing.T) {
	a := New(Strings("b"), Ints(1, 2))
	if a.Err() == nil {
		t.Error("Expected error, got success")
	}
	a = New(Strings("b", "a"), NamedInts("Y", 1, 2), Floats(3.0, 4.0))
	if a.Err() != nil {
		t.Error("Expected success, got error")
	}
	expectedNames := []string{"X0", "Y", "X1"}
	receivedNames := a.Names()
	if !reflect.DeepEqual(expectedNames, receivedNames) {
		t.Error(
			"Expected Names:",
			expectedNames,
			"Received Names:",
			receivedNames,
		)
	}
	expectedTypes := []string{"string", "int", "float"}
	receivedTypes := a.Types()
	if !reflect.DeepEqual(expectedTypes, receivedTypes) {
		t.Error(
			"Expected Types:",
			expectedTypes,
			"Received Types:",
			receivedTypes,
		)
	}
	// TODO: Check that the address of the columns are different that of the original series
	// TODO: Check that dimensions match
	a = New()
	if a.Err() == nil {
		t.Error("Expected error, got success")
	}
}

func TestDataFrame_Copy(t *testing.T) {
	a := New(NamedStrings("COL.1", "b", "a"), NamedInts("COL.2", 1, 2), NamedFloats("COL.3", 3.0, 4.0))
	b := a.Copy()
	if a.columns[0].elements.(stringElements)[0] == b.columns[0].elements.(stringElements)[0] {
		t.Error("Copy error: The memory address should be different even if the content is the same")
	}
	// TODO: More error checking, this is not exhaustive enough
}

func TestDataFrame_Subset(t *testing.T) {
	a := New(NamedStrings("COL.1", "b", "a", "c", "d"), NamedInts("COL.2", 1, 2, 3, 4), NamedFloats("COL.3", 3.0, 4.0, 2.1, 1))
	b := a.Subset([]int{2, 3})
	if b.Err() != nil {
		t.Error("Expected success, got error")
	}
	b = a.Subset([]bool{true, false, false, true})
	if b.Err() != nil {
		t.Error("Expected success, got error")
	}
	b = a.Subset(Ints(1, 2, 3))
	if b.Err() != nil {
		t.Error("Expected success, got error")
	}
	b = a.Subset(Bools(1, 0, 0, 0))
	if b.Err() != nil {
		t.Error("Expected success, got error")
	}
	b = a.Subset(Ints(1, 2, 3)).Subset([]int{0})
	if b.Err() != nil {
		t.Error("Expected success, got error")
	}
	// TODO: More error checking, this is not exhaustive enough
}

func TestDataFrame_Select(t *testing.T) {
	a := New(NamedStrings("COL.1", "b", "a", "c", "d"), NamedInts("COL.2", 1, 2, 3, 4), NamedFloats("COL.3", 3.0, 4.0, 2.1, 1))
	b := a.Select([]string{"COL.1", "COL.3", "COL.1"}...)
	if b.Err() == nil {
		t.Error("Expected error, got success")
	}
	b = a.Select([]string{"COL.3", "COL.1"}...)
	if b.Err() != nil {
		t.Error("Expected success, got error")
	}
	b = a.Subset([]int{0, 1}).Select([]string{"COL.3", "COL.1"}...)
	if b.Err() != nil {
		t.Error("Expected success, got error")
	}
	// TODO: More error checking, this is not exhaustive enough
}

func TestDataFrame_Rename(t *testing.T) {
	a := New(NamedStrings("COL.1", "b", "a", "c", "d"), NamedInts("COL.2", 1, 2, 3, 4), NamedFloats("COL.3", 3.0, 4.0, 2.1, 1))
	b := a.Rename("NewCol!", "YOOOO")
	if b.Err() == nil {
		t.Error("Expected error, got success")
	}
	b = a.Rename("NewCol!", "COL.2")
	if b.Err() != nil {
		t.Error("Expected success, got error")
	}
	// TODO: More error checking, this is not exhaustive enough
}

func TestDataFrame_CBind(t *testing.T) {
	a := New(NamedStrings("COL.1", "b", "a", "c", "d"), NamedInts("COL.2", 1, 2, 3, 4), NamedFloats("COL.3", 3.0, 4.0, 2.1, 1))
	b := New(NamedStrings("COL.1", "a", "c", "d"), NamedInts("COL.2", 1, 2, 3, 4), NamedFloats("COL.3", 3.0, 4.0, 2.1, 1))
	c := a.CBind(b)
	if c.Err() == nil {
		t.Error("Expected error, got success")
	}
	b = New(NamedStrings("COL.1", "d", "a", "d", "e"), NamedInts("COL.2", 1, 2, 3, 4), NamedFloats("COL.3", 3.0, 4.0, 2.1, 1))
	c = a.CBind(b)
	if c.Err() != nil {
		t.Error("Expected success, got error")
	}
	// TODO: More error checking, this is not exhaustive enough
}

func TestDataFrame_RBind(t *testing.T) {
	a := New(NamedStrings("COL.1", "b", "a", "c", "d"), NamedInts("COL.2", 1, 2, 3, 4), NamedFloats("COL.3", 3.0, 4.0, 2.1, 1))
	b := New(NamedStrings("COL.1", "a", "c", "d"), NamedInts("COL.2", 1, 2, 3, 4), NamedFloats("COL.3", 3.0, 4.0, 2.1, 1))
	c := a.RBind(b)
	if c.Err() == nil {
		t.Error("Expected error, got success")
	}
	b = New(NamedStrings("COL.1", "d", "a", "d", "e"), NamedInts("COL.2", 1, 2, 3, 4), NamedFloats("COL.3", 3.0, 4.0, 2.1, 1))
	c = a.RBind(b).RBind(b)
	if c.Err() != nil {
		t.Error("Expected success, got error")
	}
	// TODO: More error checking, this is not exhaustive enough
}

func TestDataFrame_Records(t *testing.T) {
	a := New(NamedStrings("COL.1", "a", "b", "c"), NamedInts("COL.2", 1, 2, 3), NamedFloats("COL.3", 3, 2, 1))
	expected := [][]string{
		[]string{"COL.1", "COL.2", "COL.3"},
		[]string{"a", "1", "3"},
		[]string{"b", "2", "2"},
		[]string{"c", "3", "1"},
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

func TestDataFrame_LoadRecords(t *testing.T) {
	records := [][]string{
		[]string{"COL.1", "COL.2", "COL.3"},
		[]string{"a", "true", "3"},
		[]string{"b", "false", "2"},
		[]string{"1", "", "1.1"},
	}
	a := LoadRecords(records)
	if a.Err() != nil {
		t.Error("Expected success, got error")
	}
	a = LoadRecords(records, "int")
	if a.Err() != nil {
		t.Error("Expected success, got error")
	}
	a = LoadRecords(records, "string")
	if a.Err() != nil {
		t.Error("Expected success, got error")
	}
	a = LoadRecords(records, "float")
	if a.Err() != nil {
		t.Error("Expected success, got error")
	}
	a = LoadRecords(records, "bool")
	if a.Err() != nil {
		t.Error("Expected success, got error")
	}
	a = LoadRecords(records, "blaaah")
	if a.Err() == nil {
		t.Error("Expected error, got success")
	}
	a = LoadRecords(records, []string{"string", "int"}...)
	if a.Err() == nil {
		t.Error("Expected error, got success")
	}
	a = LoadRecords(records, []string{"string", "int", "float"}...)
	if a.Err() != nil {
		t.Error("Expected success, got error")
	}
	a = LoadRecords(records, []string{"string", "bool", "int"}...)
	if a.Err() != nil {
		t.Error("Expected success, got error")
	}
}

func TestDataFrame_ReadCSV(t *testing.T) {
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
	if a.Err() != nil {
		t.Errorf("Expected success, got error: %v", a.Err())
	}
	a = ReadCSV(strings.NewReader(csvStr), "int")
	if a.Err() != nil {
		t.Errorf("Expected success, got error: %v", a.Err())
	}
	a = ReadCSV(strings.NewReader(csvStr), "string")
	if a.Err() != nil {
		t.Errorf("Expected success, got error: %v", a.Err())
	}
	a = ReadCSV(strings.NewReader(csvStr), "float")
	if a.Err() != nil {
		t.Errorf("Expected success, got error: %v", a.Err())
	}
	a = ReadCSV(strings.NewReader(csvStr), "bool")
	if a.Err() != nil {
		t.Errorf("Expected success, got error: %v", a.Err())
	}
	a = ReadCSV(strings.NewReader(csvStr), "blaaah")
	if a.Err() == nil {
		t.Error("Expected error, got success")
	}
	a = ReadCSV(strings.NewReader(csvStr), []string{"string", "int"}...)
	if a.Err() == nil {
		t.Error("Expected error, got success")
	}
	a = ReadCSV(strings.NewReader(csvStr), []string{"string", "int", "float", "float", "int"}...)
	if a.Err() != nil {
		t.Error("Expected success, got error")
		t.Errorf("Expected success, got error: %v", a.Err())
	}
}

func TestDataFrame_SetNames(t *testing.T) {
	a := New(NamedStrings("COL.1", "a", "b", "c"), NamedInts("COL.2", 1, 2, 3), NamedFloats("COL.3", 3, 2, 1))
	n := []string{"wot", "tho", "tree"}
	err := a.SetNames(n)
	if err != nil {
		t.Error("Expected success, got error")
	}
	err = a.SetNames([]string{"yaaa"})
	if err == nil {
		t.Error("Expected error, got success")
	}
}

func TestDataFrame_Maps(t *testing.T) {
	a := New(
		NamedStrings("COL.1", nil, "b", "c"),
		NamedInts("COL.2", 1, 2, 3),
		NamedFloats("COL.3", 3, nil, 1))
	m := a.Maps()
	_, err := json.Marshal(m)
	if err != nil {
		t.Errorf("Expected success, got error: %v", err)
	}
}

func TestDataFrame_WriteCSV(t *testing.T) {
	a := New(
		NamedStrings("COL.1", nil, "b", "c"),
		NamedInts("COL.2", 1, 2, 3),
		NamedFloats("COL.3", 3, nil, 1))
	buf := new(bytes.Buffer)
	err := a.WriteCSV(buf)
	if err != nil {
		t.Errorf("Expected success, got error: %v", err)
	}
	expected := `COL.1,COL.2,COL.3
NA,1,3
b,2,NA
c,3,1
`
	if expected != buf.String() {
		t.Errorf("\nexpected: %v\nreceived: %v", expected, buf.String())
	}
}

func TestDataFrame_WriteJSON(t *testing.T) {
	a := New(
		NamedStrings("COL.1", nil, "b", "c"),
		NamedInts("COL.2", 1, 2, 3),
		NamedFloats("COL.3", 3, nil, 1))
	buf := new(bytes.Buffer)
	err := a.WriteJSON(buf)
	if err != nil {
		t.Errorf("Expected success, got error: %v", err)
	}
	expected := `[{"COL.1":null,"COL.2":1,"COL.3":3},{"COL.1":"b","COL.2":2,"COL.3":null},{"COL.1":"c","COL.2":3,"COL.3":1}]
`
	if expected != buf.String() {
		t.Errorf("\nexpected: %v\nreceived: %v", expected, buf.String())
	}
}

func TestDataFrame_Column(t *testing.T) {
	a := New(NamedStrings("COL.1", nil, "b", "c"), NamedInts("COL.2", 1, 2, 3), NamedFloats("COL.3", 3, nil, 1))
	b := a.Col("COL.2")
	if b.Err() != nil {
		t.Error("Expected success, got error")
	}
}

func TestDataFrame_Mutate(t *testing.T) {
	a := New(
		NamedStrings("COL.1", nil, "b", "c"),
		NamedInts("COL.2", 1, 2, 3),
		NamedFloats("COL.3", 3, nil, 1),
	)
	b := a.Mutate("COL.2", NamedStrings("ColumnChanged!", "x", 1, "z"))
	if b.Err() != nil {
		t.Error("Expected success, got error")
	}
	b = b.Mutate("NewColumn!", Strings("x", 1, "z"))
	if b.Err() != nil {
		t.Error("Expected success, got error")
	}
}

func TestDataFrame_Filter(t *testing.T) {
	a := New(
		NamedInts("Age", 23, 32, 41),
		NamedStrings("Names", "Alice", "Bob", "Daniel"),
		NamedFloats("Credit", 12.10, 15.1, 16.2),
	)
	b := a.Filter(
		F{"Age", "<", 30},
		F{"Age", ">", 40},
	).Filter(F{"Names", "==", "Alice"})
	if b.Err() != nil {
		t.Error("Expected success, got error")
	}
	if b.Nrow() != 1 {
		t.Error("Expected Nrow=1, got ", b.Nrow())
	}
}

func TestDataFrame_LoadMaps(t *testing.T) {
	m := []map[string]interface{}{
		map[string]interface{}{
			"Age":    23,
			"Name":   "Alice",
			"Credit": 12.10,
		},
		map[string]interface{}{
			"Age":    32,
			"Name":   "Bob",
			"Credit": 15.1,
		},
		map[string]interface{}{
			"Age":    41,
			"Name":   "Daniel",
			"Credit": 16.2,
		},
	}
	b := LoadMaps(m)
	if b.Err() != nil {
		t.Error("Expected success, got error: ", b.Err())
	}
}

func TestDataFrame_InnerJoin(t *testing.T) {
	a := New(
		NamedInts("A", 1, 2, 3, 1),
		NamedStrings("B", "a", "b", "c", "d"),
		NamedFloats("C", 5.1, 6.0, 6.0, 7.1),
		NamedBools("D", true, true, false, false),
	)
	b := New(
		NamedStrings("A", "1", "4", "2", "5"),
		NamedInts("F", 1, 2, 8, 9),
		NamedBools("D", true, false, false, false),
	)
	testTable := []struct {
		keys     []string
		expected DataFrame
	}{
		{
			[]string{"A"},
			New(
				NamedInts("A", 1, 2, 1),
				NamedStrings("B", "a", "b", "d"),
				NamedFloats("C", 5.1, 6.0, 7.1),
				NamedBools("D.0", true, true, false),
				NamedInts("F", 1, 8, 1),
				NamedBools("D.1", true, false, true),
			),
		},
		{
			[]string{"D"},
			New(
				NamedBools("D", true, true, false, false, false, false, false, false),
				NamedInts("A.0", 1, 2, 3, 3, 3, 1, 1, 1),
				NamedStrings("B", "a", "b", "c", "c", "c", "d", "d", "d"),
				NamedFloats("C", 5.1, 6.0, 6.0, 6.0, 6.0, 7.1, 7.1, 7.1),
				NamedStrings("A.1", "1", "1", "4", "2", "5", "4", "2", "5"),
				NamedInts("F", 1, 1, 2, 8, 9, 2, 8, 9),
			),
		},
		{
			[]string{"A", "D"},
			New(
				NamedInts("A", 1),
				NamedBools("D", true),
				NamedStrings("B", "a"),
				NamedFloats("C", 5.1),
				NamedInts("F", 1),
			),
		},
		{
			[]string{"D", "A"},
			New(
				NamedBools("D", true),
				NamedInts("A", 1),
				NamedStrings("B", "a"),
				NamedFloats("C", 5.1),
				NamedInts("F", 1),
			),
		},
	}
	for k, v := range testTable {
		c := a.InnerJoin(b, v.keys...)
		if !joinTestEq(c, v.expected) {
			t.Errorf(
				"Error on test %v:\nExpected:\n%v\nReceived:\n%v",
				k, v.expected, c)
		}
	}
}

func TestDataFrame_LeftJoin(t *testing.T) {
	a := New(
		NamedInts("A", 1, 2, 3, 1),
		NamedStrings("B", "a", "b", "c", "d"),
		NamedFloats("C", 5.1, 6.0, 6.0, 7.1),
		NamedBools("D", true, true, false, false),
	)
	b := New(
		NamedStrings("A", "1", "4", "2", "5"),
		NamedInts("F", 1, 2, 8, 9),
		NamedBools("D", true, false, false, false),
	)
	testTable := []struct {
		keys     []string
		expected DataFrame
	}{
		{
			[]string{"A"},
			New(
				NamedInts("A", 1, 2, 3, 1),
				NamedStrings("B", "a", "b", "c", "d"),
				NamedFloats("C", 5.1, 6.0, 6.0, 7.1),
				NamedBools("D.0", true, true, false, false),
				NamedInts("F", 1, 8, nil, 1),
				NamedBools("D.1", true, false, nil, true),
			),
		},
		{
			[]string{"D"},
			New(
				NamedBools("D", true, true, false, false, false, false, false, false),
				NamedInts("A.0", 1, 2, 3, 3, 3, 1, 1, 1),
				NamedStrings("B", "a", "b", "c", "c", "c", "d", "d", "d"),
				NamedFloats("C", 5.1, 6.0, 6.0, 6.0, 6.0, 7.1, 7.1, 7.1),
				NamedStrings("A.1", "1", "1", "4", "2", "5", "4", "2", "5"),
				NamedInts("F", 1, 1, 2, 8, 9, 2, 8, 9),
			),
		},
		{
			[]string{"A", "D"},
			New(
				NamedInts("A", 1, 2, 3, 1),
				NamedBools("D", true, true, false, false),
				NamedStrings("B", "a", "b", "c", "d"),
				NamedFloats("C", 5.1, 6.0, 6.0, 7.1),
				NamedInts("F", 1, nil, nil, nil),
			),
		},
		{
			[]string{"D", "A"},
			New(
				NamedBools("D", true, true, false, false),
				NamedInts("A", 1, 2, 3, 1),
				NamedStrings("B", "a", "b", "c", "d"),
				NamedFloats("C", 5.1, 6.0, 6.0, 7.1),
				NamedInts("F", 1, nil, nil, nil),
			),
		},
	}
	for k, v := range testTable {
		c := a.LeftJoin(b, v.keys...)
		if !joinTestEq(c, v.expected) {
			t.Errorf(
				"Error on test %v:\nExpected:\n%v\nReceived:\n%v",
				k, v.expected, c)
		}
	}
}

func TestDataFrame_RightJoin(t *testing.T) {
	a := New(
		NamedInts("A", 1, 2, 3, 1),
		NamedStrings("B", "a", "b", "c", "d"),
		NamedFloats("C", 5.1, 6.0, 6.0, 7.1),
		NamedBools("D", true, true, false, false),
	)
	b := New(
		NamedStrings("A", "1", "4", "2", "5"),
		NamedInts("F", 1, 2, 8, 9),
		NamedBools("D", true, false, false, false),
	)
	testTable := []struct {
		keys     []string
		expected DataFrame
	}{
		{
			[]string{"A"},
			New(
				NamedInts("A", 1, 1, 2, 4, 5),
				NamedStrings("B", "a", "d", "b", nil, nil),
				NamedFloats("C", 5.1, 7.1, 6.0, nil, nil),
				NamedBools("D.0", true, false, true, nil, nil),
				NamedInts("F", 1, 1, 8, 2, 9),
				NamedBools("D.1", true, true, false, false, false),
			),
		},
		{
			[]string{"D"},
			New(
				NamedBools("D", true, true, false, false, false, false, false, false),
				NamedInts("A.0", 1, 2, 3, 1, 3, 1, 3, 1),
				NamedStrings("B", "a", "b", "c", "d", "c", "d", "c", "d"),
				NamedFloats("C", 5.1, 6.0, 6.0, 7.1, 6.0, 7.1, 6.0, 7.1),
				NamedStrings("A.1", "1", "1", "4", "4", "2", "2", "5", "5"),
				NamedInts("F", 1, 1, 2, 2, 8, 8, 9, 9),
			),
		},
		{
			[]string{"A", "D"},
			New(
				NamedInts("A", 1, 4, 2, 5),
				NamedBools("D", true, false, false, false),
				NamedStrings("B", "a", nil, nil, nil),
				NamedFloats("C", 5.1, nil, nil, nil),
				NamedInts("F", 1, 2, 8, 9),
			),
		},
		{
			[]string{"D", "A"},
			New(
				NamedBools("D", true, false, false, false),
				NamedInts("A", 1, 4, 2, 5),
				NamedStrings("B", "a", nil, nil, nil),
				NamedFloats("C", 5.1, nil, nil, nil),
				NamedInts("F", 1, 2, 8, 9),
			),
		},
	}
	for k, v := range testTable {
		c := a.RightJoin(b, v.keys...)
		if !joinTestEq(c, v.expected) {
			t.Errorf(
				"Error on test %v:\nExpected:\n%v\nReceived:\n%v",
				k, v.expected, c)
		}
	}
}

func TestDataFrame_OuterJoin(t *testing.T) {
	a := New(
		NamedInts("A", 1, 2, 3, 1),
		NamedStrings("B", "a", "b", "c", "d"),
		NamedFloats("C", 5.1, 6.0, 6.0, 7.1),
		NamedBools("D", true, true, false, false),
	)
	b := New(
		NamedStrings("A", "1", "4", "2", "5"),
		NamedInts("F", 1, 2, 8, 9),
		NamedBools("D", true, false, false, false),
	)
	testTable := []struct {
		keys     []string
		expected DataFrame
	}{
		{
			[]string{"A"},
			New(
				NamedInts("A", 1, 2, 3, 1),
				NamedStrings("B", "a", "b", "c", "d"),
				NamedFloats("C", 5.1, 6.0, 6.0, 7.1),
				NamedBools("D.0", true, true, false, false),
				NamedInts("F", 1, 8, nil, 1),
				NamedBools("D.1", true, false, nil, true),
			).RBind(
				New(
					NamedInts("A", 4, 5),
					NamedStrings("B", nil, nil),
					NamedFloats("C", nil, nil),
					NamedBools("D.0", nil, nil),
					NamedInts("F", 2, 9),
					NamedBools("D.1", false, false),
				),
			),
		},
		{
			[]string{"D"},
			New(
				NamedBools("D", true, true, false, false, false, false, false, false),
				NamedInts("A.0", 1, 2, 3, 3, 3, 1, 1, 1),
				NamedStrings("B", "a", "b", "c", "c", "c", "d", "d", "d"),
				NamedFloats("C", 5.1, 6.0, 6.0, 6.0, 6.0, 7.1, 7.1, 7.1),
				NamedStrings("A.1", "1", "1", "4", "2", "5", "4", "2", "5"),
				NamedInts("F", 1, 1, 2, 8, 9, 2, 8, 9),
			),
		},
		{
			[]string{"A", "D"},
			New(
				NamedInts("A", 1, 2, 3, 1),
				NamedBools("D", true, true, false, false),
				NamedStrings("B", "a", "b", "c", "d"),
				NamedFloats("C", 5.1, 6.0, 6.0, 7.1),
				NamedInts("F", 1, nil, nil, nil),
			).RBind(
				New(
					NamedInts("A", 4, 2, 5),
					NamedBools("D", false, false, false),
					NamedStrings("B", nil, nil, nil),
					NamedFloats("C", nil, nil, nil),
					NamedInts("F", 2, 8, 9),
				),
			),
		},
		{
			[]string{"D", "A"},
			New(
				NamedBools("D", true, true, false, false),
				NamedInts("A", 1, 2, 3, 1),
				NamedStrings("B", "a", "b", "c", "d"),
				NamedFloats("C", 5.1, 6.0, 6.0, 7.1),
				NamedInts("F", 1, nil, nil, nil),
			).RBind(
				New(
					NamedBools("D", false, false, false),
					NamedInts("A", 4, 2, 5),
					NamedStrings("B", nil, nil, nil),
					NamedFloats("C", nil, nil, nil),
					NamedInts("F", 2, 8, 9),
				),
			),
		},
	}
	for k, v := range testTable {
		c := a.OuterJoin(b, v.keys...)
		if !joinTestEq(c, v.expected) {
			t.Errorf(
				"Error on test %v:\nExpected:\n%v\nReceived:\n%v",
				k, v.expected, c)
		}
	}
}

func TestDataFrame_CrossJoin(t *testing.T) {
	a := New(
		NamedInts("A", 1, 2, 3, 1),
		NamedStrings("B", "a", "b", "c", "d"),
		NamedFloats("C", 5.1, 6.0, 6.0, 7.1),
		NamedBools("D", true, true, false, false),
	)
	b := New(
		NamedStrings("A", "1", "4", "2", "5"),
		NamedInts("F", 1, 2, 8, 9),
		NamedBools("D", true, false, false, false),
	)
	c := a.CrossJoin(b)
	expectedCSV := `
A.0,B,C,D.0,A.1,F,D.1
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
	expected := ReadCSV(strings.NewReader(expectedCSV),
		[]string{"int", "string", "float", "bool", "string", "int", "bool"}...)
	if c.Err() != nil {
		t.Error("Expected success, got error: ", c.Err())
	}
	if !joinTestEq(c, expected) {
		t.Errorf(
			"Error:\nExpected:\n%v\nReceived:\n%v",
			expected, c)
	}
}

// Helper function to compare DataFrames even if the value to compare is NA
func joinTestEq(a, b DataFrame) bool {
	if a.nrows != b.nrows || a.ncols != b.ncols {
		return false
	}
	if !reflect.DeepEqual(a.Names(), b.Names()) {
		return false
	}
	if !reflect.DeepEqual(a.Types(), b.Types()) {
		return false
	}
	for i := 0; i < a.nrows; i++ {
		for j := 0; j < a.ncols; j++ {
			aElem := a.columns[j].elem(i)
			bElem := b.columns[j].elem(i)

			if !(aElem.IsNA() && bElem.IsNA()) &&
				!aElem.Eq(bElem) {
				return false
			}
		}
	}
	return true
}
