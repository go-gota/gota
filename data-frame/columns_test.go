package df

import (
	"fmt"
	"testing"
)

func TestColumn_FillColum(t *testing.T) {
	colname := "TestColumn"
	col := Column{
		colName:  colname,
		numChars: len(colname),
	}
	a := []string{"C", "D"}
	aa := Strings("A", "B", a, 1, 2, []int{3, 4, 5}, 6.0, []float64{7.0, 8.0})
	col.FillColumn(aa)
	expected := "[A B C D 1 2 3 4 5 6.000000 7.000000 8.000000]"
	received := fmt.Sprint(col.row)
	if received != expected {
		t.Error(
			"[]String value not being properly inserted\n",
			"Expected:\n",
			expected, "\n",
			"Received:\n",
			received,
		)
	}

	// Make sure that a modification on the original slice don't affect the column
	// values.
	a[0] = "D"
	expected = "[A B C D 1 2 3 4 5 6.000000 7.000000 8.000000]"
	received = fmt.Sprint(col.row)
	if expected != received {
		t.Error(
			"Changes on the source elements should not affect loaded values",
			"Expected:\n",
			expected, "\n",
			"Received:\n",
			received,
		)
	}

	// Empty column errors
	err := col.FillColumn(Strings())
	if err == nil {
		t.Error("Trying to fill an empty column should fail")
	}

	err = col.FillColumn(nil)
	if err == nil {
		t.Error("Trying to fill an empty column should fail")
	}

	// Not complying with the interface
	err = col.FillColumn([]string{"A", "B"})
	if err == nil {
		t.Error("Values passed to FillColumn should comply with the necessary interface")
	}
	err = col.FillColumn("A")
	if err == nil {
		t.Error("Values passed to FillColumn should comply with the necessary interface")
	}

	err = col.FillColumn(String{"ABCDEFGHIJKLMNOPQRSTU"})
	expected = "[ABCDEFGHIJKLMNOPQRSTU]"
	received = fmt.Sprint(col.row)
	if expected != received {
		t.Error(
			"Single element not being introduced properly",
			"Expected:\n",
			expected, "\n",
			"Received:\n",
			received,
		)
	}

	err = col.FillColumn(Strings("ABCDEFGHIJKLMNOPQRSTU"))
}

func TestColumn_Len(t *testing.T) {
	col, _ := NewCol("TestCol", Strings("A", "B"))
	expected := 2
	received := col.Len()
	if expected != received {
		t.Error(
			"Column.Len() doesn't give the right value",
			"Expected:\n",
			expected, "\n",
			"Received:\n",
			received,
		)
	}

	col = &Column{}
	expected = 0
	received = col.Len()
	if expected != received {
		t.Error(
			"Column.Len() on empty column should return 0",
			"Expected:\n",
			expected, "\n",
			"Received:\n",
			received,
		)
	}
}

func TestColumn_elementAtIndex(t *testing.T) {
	col := Column{}
	_, err := col.elementAtIndex(3)
	if err == nil {
		t.Error("Error when retrieving an element for an empty column not being thrown")
	}

	col.FillColumn(Strings("a", "b"))
	_, err = col.elementAtIndex(8)
	if err == nil {
		t.Error("Error when retrieving an element out of bounds not being thrown")
	}
}

func TestNewCol(t *testing.T) {
	col, err := NewCol("TestCol", Strings("A", "B"))
	if err != nil || col == nil {
		t.Error("NewCol has failed unexpectedly:", err)
	}
	expected := "[A B]"
	received := fmt.Sprint(col.row)
	if expected != received {
		t.Error(
			"Single element not being introduced properly",
			"Expected:\n",
			expected, "\n",
			"Received:\n",
			received,
		)
	}

	col, err = NewCol("TestCol", Strings())
	if col != nil || err == nil {
		t.Error("NewCol hasn't failed when it should")
	}
}

func TestColumn_parseColumn(t *testing.T) {
	// String to Int
	cola, _ := NewCol("TestCol", Strings("1", "2"))
	colb, err := parseColumn(*cola, "int", nil)
	if err != nil {
		t.Error("Error parsing a df.String column into df.Int:", err)
	}
	if colb.Len() != cola.Len() ||
		colb.colName != cola.colName ||
		colb.colType != "df.Int" ||
		fmt.Sprint(colb.row) != "[1 2]" {
		t.Error("Error parsing a df.String column into df.Int",
			"\ncola.Len():", cola.Len(),
			"\ncolb.Len():", colb.Len(),
			"\ncola.colName:", cola.colName,
			"\ncolb.colName:", colb.colName,
			"\ncolb.colType:", colb.colType,
		)
	}

	// String to String
	cola, _ = NewCol("TestCol", Strings("1", "2"))
	colb, err = parseColumn(*cola, "string", nil)
	if err != nil {
		t.Error("Error parsing a df.String column into df.String:", err)
	}
	if colb.Len() != cola.Len() ||
		colb.colName != cola.colName ||
		colb.colType != "df.String" ||
		fmt.Sprint(colb.row) != "[1 2]" {
		t.Error("Error parsing a df.String column into df.Int",
			"\ncola.Len():", cola.Len(),
			"\ncolb.Len():", colb.Len(),
			"\ncola.colName:", cola.colName,
			"\ncolb.colName:", colb.colName,
			"\ncolb.colType:", colb.colType,
		)
	}

	// Int to String
	cola, _ = NewCol("TestCol", Ints(1, 2))
	colb, err = parseColumn(*cola, "string", nil)
	if err != nil {
		t.Error("Error parsing a df.Int column into df.String:", err)
	}
	if colb.Len() != cola.Len() ||
		colb.colName != cola.colName ||
		colb.colType != "df.String" ||
		fmt.Sprint(colb.row) != "[1 2]" {
		t.Error("Error parsing a df.String column into df.Int",
			"\ncola.Len():", cola.Len(),
			"\ncolb.Len():", colb.Len(),
			"\ncola.colName:", cola.colName,
			"\ncolb.colName:", colb.colName,
			"\ncolb.colType:", colb.colType,
		)
	}
}
