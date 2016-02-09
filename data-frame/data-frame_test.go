package df

import (
	"fmt"
	"testing"
)

func TestColumn_FillColum(t *testing.T) {
	c := Column{}
	a := []String{"A", "B", "C"}
	c.FillColumn(a)

	// Make sure that a modification on the original slice don't affect the column
	// values.
	str1 := fmt.Sprint(c)
	a[0] = "D"
	str2 := fmt.Sprint(c)
	if str1 != str2 {
		t.Error("Changes on the source elements should not affect loaded values")
	}
}
