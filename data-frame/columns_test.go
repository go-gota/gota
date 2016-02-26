package df

import (
	"fmt"
	"reflect"
	"testing"
)

func TestAppend(t *testing.T) {
	colname := "T"
	col := column{
		colName: colname,
	}
	var tests = []struct {
		data        Cells
		expectedLen int
	}{
		{Strings("A", "B"), 2},
		{Strings("1", "2"), 2},
		{Ints(3, 4, nil), 3},
		{Strings("CDE", "FGH"), 2},
		{nil, 0},
	}
	for k, v := range tests {
		colb, err := col.append(v.data...)
		if err != nil {
			t.Error("Error on test", k, ":", err)
		}
		expectedLen := v.expectedLen
		receivedLen := len(colb.cells)
		if expectedLen != receivedLen {
			t.Error("Error on test", k, ":\n",
				"Expected Len:", expectedLen,
				"Received Length:", receivedLen)
		}
	}
	s := "A"
	_, err := col.append(Int{nil}, String{&s})
	if err == nil {
		t.Error("Should throw an error: Conflicting types")
	}
}

func TestnewCol(t *testing.T) {
	col, err := newCol("TestCol", Strings("A", "B"))
	if err != nil || col == nil {
		t.Error("newCol has failed unexpectedly:", err)
	}
	expected := "[A B]"
	received := fmt.Sprint(col.cells)
	if expected != received {
		t.Error(
			"Single element not being introduced properly",
			"Expected:\n",
			expected, "\n",
			"Received:\n",
			received,
		)
	}
}

func TestColumn_ParseColumn(t *testing.T) {
	// String to Int
	cola, _ := newCol("TestCol", Strings("1", "2"))
	colb := cola.copy()
	err := colb.ParseColumn("int")
	if err != nil {
		t.Error("Error parsing a df.String column into df.Int:", err)
	}

	if len(colb.cells) != len(cola.cells) ||
		colb.colName != cola.colName ||
		colb.colType != "df.Int" ||
		fmt.Sprint(colb.cells) != "[1 2]" {
		t.Error("Error parsing a df.String column into df.Int",
			"\nlen(cola.cells):", len(cola.cells),
			"\nlen(colb.cells):", len(colb.cells),
			"\ncola.colName:", cola.colName,
			"\ncolb.colName:", colb.colName,
			"\ncolb.colType:", colb.colType,
		)
	}

	// String to String
	cola, _ = newCol("TestCol", Strings("1", "2"))
	colb = cola.copy()
	err = colb.ParseColumn("string")
	if err != nil {
		t.Error("Error parsing a df.String column into df.String:", err)
	}
	if len(colb.cells) != len(cola.cells) ||
		colb.colName != cola.colName ||
		colb.colType != "df.String" ||
		fmt.Sprint(colb.cells) != "[1 2]" {
		t.Error("Error parsing a df.String column into df.Int",
			"\nlen(cola.cells):", len(cola.cells),
			"\nlen(colb.cells):", len(colb.cells),
			"\ncola.colName:", cola.colName,
			"\ncolb.colName:", colb.colName,
			"\ncolb.colType:", colb.colType,
		)
	}

	// Float to String
	cola, _ = newCol("TestCol", Floats(1, 2))
	colb = cola.copy()
	err = colb.ParseColumn("string")
	if err != nil {
		t.Error("Error parsing a df.Float column into df.String:", err)
	}
	if len(colb.cells) != len(cola.cells) ||
		colb.colName != cola.colName ||
		colb.colType != "df.String" ||
		fmt.Sprint(colb.cells) != "[1 2]" {
		t.Error("Error parsing a df.Float column into df.Int",
			"\nlen(cola.cells):", len(cola.cells),
			"\nlen(colb.cells):", len(colb.cells),
			"\ncola.colName:", cola.colName,
			"\ncolb.colName:", colb.colName,
			"\ncolb.colType:", colb.colType,
		)
	}

	// Float to Int
	cola, _ = newCol("TestCol", Floats(1, 2))
	colb = cola.copy()
	err = colb.ParseColumn("int")
	if err != nil {
		t.Error("Error parsing a df.Float column into df.Int:", err)
	}
	if len(colb.cells) != len(cola.cells) ||
		colb.colName != cola.colName ||
		colb.colType != "df.Int" ||
		fmt.Sprint(colb.cells) != "[1 2]" {
		t.Error("Error parsing a df.Float column into df.Int",
			"\nlen(cola.cells):", len(cola.cells),
			"\nlen(colb.cells):", len(colb.cells),
			"\ncola.colName:", cola.colName,
			"\ncolb.colName:", colb.colName,
			"\ncolb.colType:", colb.colType,
		)
	}

	// Int to String
	cola, _ = newCol("TestCol", Ints(1, 2))
	colb = cola.copy()
	err = colb.ParseColumn("string")
	if err != nil {
		t.Error("Error parsing a df.Int column into df.String:", err)
	}
	if len(colb.cells) != len(cola.cells) ||
		colb.colName != cola.colName ||
		colb.colType != "df.String" ||
		fmt.Sprint(colb.cells) != "[1 2]" {
		t.Error("Error parsing a df.Int column into df.String",
			"\nlen(cola.cells):", len(cola.cells),
			"\nlen(colb.cells):", len(colb.cells),
			"\ncola.colName:", cola.colName,
			"\ncolb.colName:", colb.colName,
			"\ncolb.colType:", colb.colType,
		)
	}

	// String to Float
	cola, _ = newCol("TestCol", Strings("1", "2"))
	colb = cola.copy()
	err = colb.ParseColumn("float")
	if err != nil {
		t.Error("Error parsing a df.String column into df.Float:", err)
	}
	if len(colb.cells) != len(cola.cells) ||
		colb.colName != cola.colName ||
		colb.colType != "df.Float" ||
		fmt.Sprint(colb.cells) != "[1 2]" {
		t.Error("Error parsing a df.String column into df.Float",
			"\nlen(cola.cells):", len(cola.cells),
			"\nlen(colb.cells):", len(colb.cells),
			"\ncola.colName:", cola.colName,
			"\ncolb.colName:", colb.colName,
			"\ncolb.colType:", colb.colType,
		)
	}

	// Int to Float
	cola, _ = newCol("TestCol", Ints(1, 2))
	colb = cola.copy()
	err = colb.ParseColumn("float")
	if err != nil {
		t.Error("Error parsing a df.Int column into df.Float:", err)
	}
	if len(colb.cells) != len(cola.cells) ||
		colb.colName != cola.colName ||
		colb.colType != "df.Float" ||
		fmt.Sprint(colb.cells) != "[1 2]" {
		t.Error("Error parsing a df.Int column into df.Float",
			"\nlen(cola.cells):", len(cola.cells),
			"\nlen(colb.cells):", len(colb.cells),
			"\ncola.colName:", cola.colName,
			"\ncolb.colName:", colb.colName,
			"\ncolb.colType:", colb.colType,
		)
	}

	// Float to Float
	cola, _ = newCol("TestCol", Floats(1, 2))
	colb = cola.copy()
	err = colb.ParseColumn("float")
	if err != nil {
		t.Error("Error parsing a df.Float column into df.Float:", err)
	}
	if len(colb.cells) != len(cola.cells) ||
		colb.colName != cola.colName ||
		colb.colType != "df.Float" ||
		fmt.Sprint(colb.cells) != "[1 2]" {
		t.Error("Error parsing a df.Float column into df.Float",
			"\nlen(cola.cells):", len(cola.cells),
			"\nlen(colb.cells):", len(colb.cells),
			"\ncola.colName:", cola.colName,
			"\ncolb.colName:", colb.colName,
			"\ncolb.colType:", colb.colType,
		)
	}

	// Float to Bool
	cola, _ = newCol("TestCol", Floats(1, 0))
	colb = cola.copy()
	err = colb.ParseColumn("bool")
	if err != nil {
		t.Error("Error parsing a df.Float column into df.Float:", err)
	}
	if len(colb.cells) != len(cola.cells) ||
		colb.colName != cola.colName ||
		colb.colType != "df.Bool" ||
		fmt.Sprint(colb.cells) != "[true false]" {
		t.Error("Error parsing a df.Float column into df.Float",
			"\nlen(cola.cells):", len(cola.cells),
			"\nlen(colb.cells):", len(colb.cells),
			"\ncola.colName:", cola.colName,
			"\ncolb.colName:", colb.colName,
			"\ncolb.colType:", colb.colType,
			"\ncolb.cells:", colb.cells,
		)
	}

	// Unknown type
	cola, _ = newCol("TestCol", Ints(1, 2))
	colb = cola.copy()
	err = colb.ParseColumn("asdfasdfsa")
	if err == nil {
		t.Error("Error parsing an unknown type, error not thrown.")
	}
}

func TestColumn_na(t *testing.T) {
	var tests = []struct {
		data     column
		expNa    []bool
		expHasNa bool
	}{
		{data: column{
			cells:    Strings("A", "B"),
			colType:  "df.String",
			colName:  "A",
			numChars: 1,
		},
			expNa:    []bool{false, false},
			expHasNa: false,
		},
		{data: column{
			cells:    Ints(1, 2, 3, 4),
			colType:  "df.Int",
			colName:  "B",
			numChars: 1,
		},
			expNa:    []bool{false, false, false, false},
			expHasNa: false,
		},
		{data: column{
			cells:    Floats(1.0, 2.0, nil, 3.0),
			colType:  "df.Float",
			colName:  "C",
			numChars: 1,
		},
			expNa:    []bool{false, false, true, false},
			expHasNa: true,
		},
		{data: column{
			cells:    Bools(true, nil, false),
			colType:  "df.Bool",
			colName:  "A",
			numChars: 1,
		},
			expNa:    []bool{false, true, false},
			expHasNa: true,
		},
	}
	for k, v := range tests {
		hasna := v.data.HasNA()
		na := v.data.NA()
		exphasna := v.expHasNa
		expna := v.expNa
		if hasna != exphasna ||
			!reflect.DeepEqual(na, expna) {
			t.Error(
				"Test:", k,
				"\nExpected col.na:", expna,
				"\nReceived col.na:", na,
				"\nExpected col.hasNa:", exphasna,
				"\nReceived col.hasNa:", hasna,
			)
		}
	}
	col, _ := newCol("TestCol", Ints(1, 2, nil, 3, nil))
	if !col.HasNA() {
		t.Error("Should return true")
	}
}
