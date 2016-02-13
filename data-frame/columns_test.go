package df

import (
	"fmt"
	"testing"
)

func TestAppend(t *testing.T) {
	colname := "T"
	col := column{
		colName: colname,
	}
	var tests = []struct {
		data        cells
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
	_, err := col.append(Int{nil}, String{"A"})
	if err == nil {
		t.Error("Should throw an error: Conflicting types")
	}
}

func TestNewCol(t *testing.T) {
	col, err := newCol("TestCol", Strings("A", "B"))
	if err != nil || col == nil {
		t.Error("NewCol has failed unexpectedly:", err)
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

func TestColumn_parseColumn(t *testing.T) {
	// String to Int
	//cola, _ := NewCol("TestCol", Strings("1", "2"))
	//colb, err := parseColumn(*cola, "int")
	//if err != nil {
	//t.Error("Error parsing a df.String column into df.Int:", err)
	//}
	//if colb.Len() != cola.Len() ||
	//colb.colName != cola.colName ||
	//colb.colType != "df.Int" ||
	//fmt.Sprint(colb.row) != "[1 2]" {
	//t.Error("Error parsing a df.String column into df.Int",
	//"\ncola.Len():", cola.Len(),
	//"\ncolb.Len():", colb.Len(),
	//"\ncola.colName:", cola.colName,
	//"\ncolb.colName:", colb.colName,
	//"\ncolb.colType:", colb.colType,
	//)
	//}

	// String to String
	//cola, _ = NewCol("TestCol", Strings("1", "2"))
	//colb, err = parseColumn(*cola, "string")
	//if err != nil {
	//t.Error("Error parsing a df.String column into df.String:", err)
	//}
	//if colb.Len() != cola.Len() ||
	//colb.colName != cola.colName ||
	//colb.colType != "df.String" ||
	//fmt.Sprint(colb.row) != "[1 2]" {
	//t.Error("Error parsing a df.String column into df.Int",
	//"\ncola.Len():", cola.Len(),
	//"\ncolb.Len():", colb.Len(),
	//"\ncola.colName:", cola.colName,
	//"\ncolb.colName:", colb.colName,
	//"\ncolb.colType:", colb.colType,
	//)
	//}

	//// Int to String
	//cola, _ = NewCol("TestCol", Ints(1, 2))
	//colb, err = parseColumn(*cola, "string")
	//if err != nil {
	//t.Error("Error parsing a df.Int column into df.String:", err)
	//}
	//if colb.Len() != cola.Len() ||
	//colb.colName != cola.colName ||
	//colb.colType != "df.String" ||
	//fmt.Sprint(colb.row) != "[1 2]" {
	//t.Error("Error parsing a df.String column into df.Int",
	//"\ncola.Len():", cola.Len(),
	//"\ncolb.Len():", colb.Len(),
	//"\ncola.colName:", cola.colName,
	//"\ncolb.colName:", colb.colName,
	//"\ncolb.colType:", colb.colType,
	//)
	//}

	//// Unknown type
	//cola, _ = NewCol("TestCol", Ints(1, 2))
	//colb, err = parseColumn(*cola, "asdfg")
	//if err == nil {
	//t.Error("Error parsing an unknown type, error not thrown.")
	//}

}
