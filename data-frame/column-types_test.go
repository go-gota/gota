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
	s := "B"
	aa = Strings(dd, aa, d, String{&s}, nil)
	expected = "[NA NA 1.000000 2.000000 3.000000 4.000000 NA B NA]"
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

	_, err := aa[0].Int()
	if err == nil {
		t.Error("Int() Should fail for nil elements")
	}
}

func TestFloats(t *testing.T) {
	a := []string{"C", "D", "1.1"}
	aa := Floats("A", "B", a, "2.2")
	expected := "[NA NA NA NA 1.1 2.2]"
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
	aa = Floats(b, 3, 4)
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
	aa = Floats(1.1, 2.2, c)
	expected = "[1.1 2.2 3.6 4.7]"
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
	aa = Floats(dd, aa, d, bb, nil)
	expected = "[NA NA 1.1 2.2 3.6 4.7 NA 1 NA NA]"
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

	_, err := aa[0].Float()
	if err == nil {
		t.Error("Float() Should fail for nil elements")
	}
}

func TestBools(t *testing.T) {
	a := []string{"C", "D", "true"}
	aa := Bools("A", "B", a, "false")
	expected := "[NA NA NA NA true false]"
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
	aa = Bools(b, 1, 0)
	expected = "[true NA true false]"
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

	c := []float64{0.0, 0.01}
	aa = Bools(1.0, 2.2, c)
	expected = "[true NA false NA]"
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
	bb := Strings("true", "false")
	aa = Bools(dd, aa, d, bb, nil)
	expected = "[NA NA true NA NA NA NA true false NA]"
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

	_, err := aa[0].Float()
	if err == nil {
		t.Error("Float() Should fail for nil elements")
	}
}
