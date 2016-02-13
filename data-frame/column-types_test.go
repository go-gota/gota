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

	// TODO: FAILS
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
