package df

import (
	"fmt"
	"testing"
)

func TestStrings(t *testing.T) {
	a := []string{"C", "D"}
	aa := Strings("A", "B", a)
	expected := "[A B C D]"
	received := fmt.Sprint(aa)
	if expected != received {
		t.Error(
			"string and/or []string not being propery inserted\n",
			"Expected:\n",
			expected, "\n",
			"Received:\n",
			received,
		)
	}

	b := []int{1, 2}
	aa = Strings(b, 3, 4)
	expected = "[1 2 3 4]"
	received = fmt.Sprint(aa)
	if expected != received {
		t.Error(
			"int and/or []int not being propery inserted\n",
			"Expected:\n",
			expected, "\n",
			"Received:\n",
			received,
		)
	}

	c := []float64{3.0, 4.0}
	aa = Strings(1.0, 2.0, c)
	expected = "[1.000000 2.000000 3.000000 4.000000]"
	received = fmt.Sprint(aa)
	if expected != received {
		t.Error(
			"float64 and/or []float64 not being propery inserted\n",
			"Expected:\n",
			expected, "\n",
			"Received:\n",
			received,
		)
	}

	type T struct {
		x int
		y int
	}
	d := T{
		1,
		2,
	}
	dd := []T{d, d}
	aa = Strings(dd, aa, d, String{"B"}, nil)
	expected = "[NA NA 1.000000 2.000000 3.000000 4.000000 NA B ]"
	received = fmt.Sprint(aa)
	if received != expected {
		t.Error(
			"otherStructs and/or []otherStructs not being propery inserted\n",
			"Expected:\n",
			expected, "\n",
			"Received:\n",
			received,
		)
	}
}

func TestInts(t *testing.T) {
	a := []string{"C", "D", "1"}
	aa := Ints("A", "B", a, "2")
	expected := "[NA NA NA NA 1 2]"
	received := fmt.Sprint(aa)
	if expected != received {
		t.Error(
			"string and/or []string not being propery inserted\n",
			"Expected:\n",
			expected, "\n",
			"Received:\n",
			received,
		)
	}

	b := []int{1, 2}
	aa = Ints(b, 3, 4)
	expected = "[1 2 3 4]"
	received = fmt.Sprint(aa)
	if expected != received {
		t.Error(
			"int and/or []int not being propery inserted\n",
			"Expected:\n",
			expected, "\n",
			"Received:\n",
			received,
		)
	}

	c := []float64{3.6, 4.7}
	aa = Ints(1.1, 2.2, c)
	expected = "[1 2 3 4]"
	received = fmt.Sprint(aa)
	if expected != received {
		t.Error(
			"float64 and/or []float64 not being propery inserted\n",
			"Expected:\n",
			expected, "\n",
			"Received:\n",
			received,
		)
	}

	type T struct {
		x int
		y int
	}
	d := T{
		1,
		2,
	}
	dd := []T{d, d}
	bb := Strings(1, "B")
	aa = Ints(dd, aa, d, bb, nil)
	expected = "[NA NA 1 2 3 4 NA 1 NA NA]"
	received = fmt.Sprint(aa)
	if received != expected {
		t.Error(
			"otherStructs and/or []otherStructs not being propery inserted\n",
			"Expected:\n",
			expected, "\n",
			"Received:\n",
			received,
		)
	}

	_, err := aa[0].ToInteger()
	if err == nil {
		t.Error("ToInteger() Should fail for nil elements")
	}
}

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
