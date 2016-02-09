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
			"string and/or []string not being propery introduced\n",
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
			"int and/or []int not being propery introduced\n",
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
			"float64 and/or []float64 not being propery introduced\n",
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
	aa = Strings(dd, aa, d)
	expected = "[NA NA 1.000000 2.000000 3.000000 4.000000 NA]"
	received = fmt.Sprint(aa)
	if received != expected {
		t.Error(
			"otherStructs and/or []otherStructs not being propery introduced\n",
			"Expected:\n",
			expected, "\n",
			"Received:\n",
			received,
		)
	}
}

func TestInts(t *testing.T) {
	a := []string{"C", "D"}
	aa := Ints("A", "B", a)
	expected := "[NA NA NA NA]"
	received := fmt.Sprint(aa)
	if expected != received {
		t.Error(
			"string and/or []string not being propery introduced\n",
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
			"int and/or []int not being propery introduced\n",
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
			"float64 and/or []float64 not being propery introduced\n",
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
	aa = Ints(dd, aa, d)
	expected = "[NA NA 1 2 3 4 NA]"
	received = fmt.Sprint(aa)
	if received != expected {
		t.Error(
			"otherStructs and/or []otherStructs not being propery introduced\n",
			"Expected:\n",
			expected, "\n",
			"Received:\n",
			received,
		)
	}
}

func TestColumn_FillColum(t *testing.T) {
	//colname := "TestColumn"
	//col := Column{
	//colName:  colname,
	//numChars: len(colname),
	//}
	//a := []string{"C", "D"}
	//aa := Strings("A", "B", a, 1, 2, []int{3, 4, 5}, 6.0, []float64{7.0, 8.0}, time.Now())
	//col.FillColumn(aa)
	//fmt.Println(col)

	//// Make sure that a modification on the original slice don't affect the column
	//// values.
	//str1 := fmt.Sprint(col)
	//a[0] = "D"
	//str2 := fmt.Sprint(col)
	//if str1 != str2 {
	//t.Error("Changes on the source elements should not affect loaded values")
	//}

	//k := []int{1, 2, 3}
	//kk := []float64{4, 2, 3}
	//kkk := []string{"1", "2"}
	//b := Ints(1, 2.0, "3", k, nil, kk, nil, kkk)
	//col.FillColumn(b)
	//fmt.Println(col)
}
