package df

import (
	"encoding/json"
	"reflect"
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
	if a.columns[0].elements.(StringElements)[0] == b.columns[0].elements.(StringElements)[0] {
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

func TestDataFrame_SaveRecords(t *testing.T) {
	a := New(NamedStrings("COL.1", "a", "b", "c"), NamedInts("COL.2", 1, 2, 3), NamedFloats("COL.3", 3, 2, 1))
	expected := [][]string{
		[]string{"COL.1", "COL.2", "COL.3"},
		[]string{"a", "1", "3"},
		[]string{"b", "2", "2"},
		[]string{"c", "3", "1"},
	}
	received := a.SaveRecords()
	if !reflect.DeepEqual(expected, received) {
		t.Error(
			"Error when saving records.\n",
			"Expected: ", expected, "\n",
			"Received: ", received,
		)
	}
}

func TestDataFrame_ReadRecords(t *testing.T) {
	records := [][]string{
		[]string{"COL.1", "COL.2", "COL.3"},
		[]string{"a", "true", "3"},
		[]string{"b", "false", "2"},
		[]string{"1", "", "1.1"},
	}
	a := ReadRecords(records)
	if a.Err() != nil {
		t.Error("Expected success, got error")
	}
	a = ReadRecords(records, "int")
	if a.Err() != nil {
		t.Error("Expected success, got error")
	}
	a = ReadRecords(records, "string")
	if a.Err() != nil {
		t.Error("Expected success, got error")
	}
	a = ReadRecords(records, "float")
	if a.Err() != nil {
		t.Error("Expected success, got error")
	}
	a = ReadRecords(records, "bool")
	if a.Err() != nil {
		t.Error("Expected success, got error")
	}
	a = ReadRecords(records, "blaaah")
	if a.Err() == nil {
		t.Error("Expected error, got success")
	}
	a = ReadRecords(records, []string{"string", "int"}...)
	if a.Err() == nil {
		t.Error("Expected error, got success")
	}
	a = ReadRecords(records, []string{"string", "int", "float"}...)
	if a.Err() != nil {
		t.Error("Expected success, got error")
	}
	a = ReadRecords(records, []string{"string", "bool", "int"}...)
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
	a := ReadCSV(csvStr)
	if a.Err() != nil {
		t.Error("Expected success, got error")
	}
	a = ReadCSV(csvStr, "int")
	if a.Err() != nil {
		t.Error("Expected success, got error")
	}
	a = ReadCSV(csvStr, "string")
	if a.Err() != nil {
		t.Error("Expected success, got error")
	}
	a = ReadCSV(csvStr, "float")
	if a.Err() != nil {
		t.Error("Expected success, got error")
	}
	a = ReadCSV(csvStr, "bool")
	if a.Err() != nil {
		t.Error("Expected success, got error")
	}
	a = ReadCSV(csvStr, "blaaah")
	if a.Err() == nil {
		t.Error("Expected error, got success")
	}
	a = ReadCSV(csvStr, []string{"string", "int"}...)
	if a.Err() == nil {
		t.Error("Expected error, got success")
	}
	a = ReadCSV(csvStr, []string{"string", "int", "float", "float", "int"}...)
	if a.Err() != nil {
		t.Error("Expected success, got error")
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

func TestDataFrame_SaveMaps(t *testing.T) {
	a := New(NamedStrings("COL.1", nil, "b", "c"), NamedInts("COL.2", 1, 2, 3), NamedFloats("COL.3", 3, nil, 1))
	m := a.SaveMaps()
	_, err := json.Marshal(m)
	if err != nil {
		t.Error("Expected success, got error")
	}
}

func TestDataFrame_SaveCSV(t *testing.T) {
	a := New(NamedStrings("COL.1", nil, "b", "c"), NamedInts("COL.2", 1, 2, 3), NamedFloats("COL.3", 3, nil, 1))
	_, err := a.SaveCSV()
	if err != nil {
		t.Error("Expected success, got error")
	}
}

func TestDataFrame_SaveJSON(t *testing.T) {
	a := New(NamedStrings("COL.1", nil, "b", "c"), NamedInts("COL.2", 1, 2, 3), NamedFloats("COL.3", 3, nil, 1))
	_, err := a.SaveJSON()
	if err != nil {
		t.Error("Expected success, got error")
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

func TestDataFrame_ReadMaps(t *testing.T) {
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
	b := ReadMaps(m)
	if b.Err() != nil {
		t.Error("Expected success, got error: ", b.Err())
	}
}

func TestDataFrame_InnerJoin(t *testing.T) {
	a := New(
		NamedInts("Age", 23, 32, 41),
		NamedStrings("Names", "Alice", "Bob", "Daniel"),
		NamedFloats("Credit", 12.10, 15.1, 16.2),
	)
	b := New(
		NamedInts("Age", 23, 32, 23),
		NamedStrings("Names", "Alice", "Bob", "Daniel"),
		NamedFloats("Credit", 1.10, 0.1, 16.2),
	)
	c := a.InnerJoin(b, "Names")
	if c.Err() != nil {
		t.Error("Expected success, got error: ", c.Err())
	}
	c = a.InnerJoin(b, "Credit")
	if c.Err() != nil {
		t.Error("Expected success, got error: ", c.Err())
	}
	c = a.InnerJoin(b, "Age")
	if c.Err() != nil {
		t.Error("Expected success, got error: ", c.Err())
	}
	c = a.InnerJoin(b, "Names", "Age")
	if c.Err() != nil {
		t.Error("Expected success, got error: ", c.Err())
	}
	c = a.InnerJoin(b, "Names", "Credit")
	if c.Err() != nil {
		t.Error("Expected success, got error: ", c.Err())
	}
	c = a.InnerJoin(b, "Age", "Names")
	if c.Err() != nil {
		t.Error("Expected success, got error: ", c.Err())
	}
	c = b.Rename("Credit.B", "Credit").InnerJoin(a, "Age", "Names")
	if c.Err() != nil {
		t.Error("Expected success, got error: ", c.Err())
	}
}

func TestDataFrame_LeftJoin(t *testing.T) {
	a := New(
		NamedInts("Age", 23, 32, 41),
		NamedStrings("Names", "Alice", "Bob", "Daniel"),
		NamedFloats("Credit", 12.10, 15.1, 16.2),
	)
	b := New(
		NamedInts("Age", 23, 32, 23),
		NamedStrings("Names", "Alice", "Bob", "Daniel"),
		NamedFloats("Credit", 1.10, 0.1, 16.2),
	)
	c := a.LeftJoin(b, "Age")
	if c.Err() != nil {
		t.Error("Expected success, got error: ", c.Err())
	}
}

func TestDataFrame_RightJoin(t *testing.T) {
	a := New(
		NamedInts("Age", 23, 32, 41),
		NamedStrings("Names", "Alice", "Bob", "Daniel"),
		NamedFloats("Credit", 12.10, 15.1, 16.2),
	)
	b := New(
		NamedInts("Age", 23, 32, 23),
		NamedStrings("Names", "Alice", "Bob", "Daniel"),
		NamedFloats("Credit", 1.10, 0.1, 16.2),
	)
	c := b.RightJoin(a, "Age")
	if c.Err() != nil {
		t.Error("Expected success, got error: ", c.Err())
	}
}

func TestDataFrame_OuterJoin(t *testing.T) {
	a := New(
		NamedInts("Age", 23, 32, 41),
		NamedStrings("Names", "Alice", "Bob", "Daniel"),
		NamedFloats("Credit", 12.10, 15.1, 16.2),
	)
	b := New(
		NamedInts("Age", 23, 32, 31),
		NamedStrings("Names", "Alice", "Bob", "Daniel"),
		NamedFloats("Credit", 1.10, 0.1, 16.2),
	)
	c := a.OuterJoin(b, "Age")
	if c.Err() != nil {
		t.Error("Expected success, got error: ", c.Err())
	}
}

func TestDataFrame_CrossJoin(t *testing.T) {
	a := New(
		NamedInts("Age", 23, 32, 41),
		NamedStrings("Names", "Alice", "Bob", "Daniel"),
		NamedFloats("Credit", 12.10, 15.1, 16.2),
	)
	b := New(
		NamedInts("Age", 23, 32, 31),
		NamedStrings("Names", "Alice", "Bob", "Daniel"),
		NamedFloats("Credit", 1.10, 0.1, 16.2),
	)
	c := a.CrossJoin(b)
	if c.Err() != nil {
		t.Error("Expected success, got error: ", c.Err())
	}
}

//func TestExample(t *testing.T) {
//var a, b DataFrame
//r, err := http.Get("https://jsonplaceholder.typicode.com/albums")
//if err != nil {
//log.Fatal(err)
//} else {
//defer r.Body.Close()
//var target []map[string]interface{}
//json.NewDecoder(r.Body).Decode(&target)
//a = ReadMaps(target)
//}
//r, err = http.Get("https://jsonplaceholder.typicode.com/photos")
//if err != nil {
//log.Fatal(err)
//} else {
//defer r.Body.Close()
//var target []map[string]interface{}
//json.NewDecoder(r.Body).Decode(&target)
//b = ReadMaps(target)
//}
//c := a.LeftJoin(b, "id")
//c = a.InnerJoin(b, "id")
//c = a.RightJoin(b, "id")
////fmt.Println(c)
////fmt.Println(a.Names())
////fmt.Println(b.Names())
////fmt.Println(c.Names())
//fmt.Println(c.Dim())
//}
