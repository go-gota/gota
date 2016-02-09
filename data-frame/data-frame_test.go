package df

import (
	"fmt"
	"testing"
	"time"
)

func TestColumn_FillColum(t *testing.T) {
	colname := "TestColumn"
	col := Column{
		colName:  colname,
		numChars: len(colname),
	}
	a := []string{"C", "D"}
	aa := Strings("A", "B", a, 1, 2, []int{3, 4, 5}, 6.0, []float64{7.0, 8.0}, time.Now())
	col.FillColumn(aa)
	fmt.Println(col)

	// Make sure that a modification on the original slice don't affect the column
	// values.
	str1 := fmt.Sprint(col)
	a[0] = "D"
	str2 := fmt.Sprint(col)
	if str1 != str2 {
		t.Error("Changes on the source elements should not affect loaded values")
	}

	k := []int{1, 2, 3}
	kk := []float64{4, 2, 3}
	kkk := []string{"1", "2"}
	b := Ints(1, 2.0, "3", k, nil, kk, nil, kkk)
	col.FillColumn(b)
	fmt.Println(col)
}
