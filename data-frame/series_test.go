package df

import (
	"fmt"
	"reflect"
	"testing"
)

func TestSeries_Compare(t *testing.T) {
	a := Strings("A", "B", "C", "B", "D", "BADA")
	testData := []struct {
		comparator string
		comparando string
		expected   []bool
	}{
		{"==", "B", []bool{false, true, false, true, false, false}},
		{"in", "BADA", []bool{false, false, false, false, false, true}},
		{"!=", "C", []bool{true, true, false, true, true, true}},
		{"<", "B", []bool{true, false, false, false, false, false}},
		{"<=", "B", []bool{true, true, false, true, false, false}},
		{">", "C", []bool{false, false, false, false, true, false}},
		{">=", "C", []bool{false, false, true, false, true, false}},
	}
	for k, v := range testData {
		received, _ := a.Compare(v.comparator, v.comparando)
		if !reflect.DeepEqual(v.expected, received) {
			t.Error(
				"\nTest: ", k+1, "\n",
				"Expected:\n",
				v.expected, "\n",
				"Received:\n",
				received,
			)
		}
	}
	b := Strings("A", "B", "A")
	testData2 := []struct {
		comparator string
		comparando []string
		expected   []bool
	}{
		{"==", []string{"B", "A", "A"}, []bool{false, false, true}},
		{"!=", []string{"B", "B", "A"}, []bool{true, false, false}},
		{"in", []string{"C", "A"}, []bool{true, false, true}},
		{"in", []string{"B"}, []bool{false, true, false}},
		{"in", []string{"A", "B"}, []bool{true, true, true}},
		{"<", []string{"B", "B", "A"}, []bool{true, false, false}},
		{"<=", []string{"B", "B", "A"}, []bool{true, true, true}},
		{">", []string{"B", "B", "A"}, []bool{false, false, false}},
		{">=", []string{"B", "B", "A"}, []bool{false, true, true}},
	}
	for k, v := range testData2 {
		received, _ := b.Compare(v.comparator, v.comparando)
		if !reflect.DeepEqual(v.expected, received) {
			t.Error(
				"\nTest: ", k+1, "\n",
				"Expected:\n",
				v.expected, "\n",
				"Received:\n",
				received,
			)
		}
	}

	c := Ints(1, 2, 3, 2, 1)
	testData3 := []struct {
		comparator string
		comparando []int
		expected   []bool
	}{
		{"==", []int{1}, []bool{true, false, false, false, true}},
		{"==", []int{1, 3, 3, 1, 1}, []bool{true, false, true, false, true}},
		{"!=", []int{3}, []bool{true, true, false, true, true}},
		{"!=", []int{1, 3, 3, 1, 1}, []bool{false, true, false, true, false}},
		{"in", []int{5, 6, 7}, []bool{false, false, false, false, false}},
		{"in", []int{2, 3}, []bool{false, true, true, true, false}},
		{"<", []int{2}, []bool{true, false, false, false, true}},
		{"<", []int{3}, []bool{true, true, false, true, true}},
		{"<", []int{2, 2, 2, 1, 1}, []bool{true, false, false, false, false}},
		{"<=", []int{2}, []bool{true, true, false, true, true}},
		{"<=", []int{2, 2, 2, 1, 1}, []bool{true, true, false, false, true}},
		{">", []int{2}, []bool{false, false, true, false, false}},
		{">", []int{2, 1, 2, 1, 1}, []bool{false, true, true, true, false}},
		{">=", []int{2}, []bool{false, true, true, true, false}},
		{">=", []int{2, 1, 2, 1, 1}, []bool{false, true, true, true, true}},
	}
	for k, v := range testData3 {
		received, _ := c.Compare(v.comparator, v.comparando)
		if !reflect.DeepEqual(v.expected, received) {
			t.Error(
				"\nTest: ", k+1, "\n",
				"Expected:\n",
				v.expected, "\n",
				"Received:\n",
				received,
			)
		}
	}

	d := Floats(1, 2, 3, 2, 1)
	testData4 := []struct {
		comparator string
		comparando []int
		expected   []bool
	}{
		{"==", []int{1}, []bool{true, false, false, false, true}},
		{"==", []int{1, 3, 3, 1, 1}, []bool{true, false, true, false, true}},
		{"!=", []int{3}, []bool{true, true, false, true, true}},
		{"!=", []int{1, 3, 3, 1, 1}, []bool{false, true, false, true, false}},
		{"in", []int{5, 6, 7}, []bool{false, false, false, false, false}},
		{"in", []int{2, 3}, []bool{false, true, true, true, false}},
		{"<", []int{2}, []bool{true, false, false, false, true}},
		{"<", []int{3}, []bool{true, true, false, true, true}},
		{"<", []int{2, 2, 2, 1, 1}, []bool{true, false, false, false, false}},
		{"<=", []int{2}, []bool{true, true, false, true, true}},
		{"<=", []int{2, 2, 2, 1, 1}, []bool{true, true, false, false, true}},
		{">", []int{2}, []bool{false, false, true, false, false}},
		{">", []int{2, 1, 2, 1, 1}, []bool{false, true, true, true, false}},
		{">=", []int{2}, []bool{false, true, true, true, false}},
		{">=", []int{2, 1, 2, 1, 1}, []bool{false, true, true, true, true}},
	}
	for k, v := range testData4 {
		received, _ := d.Compare(v.comparator, v.comparando)
		if !reflect.DeepEqual(v.expected, received) {
			t.Error(
				"\nTest: ", k+1, "\n",
				"Expected:\n",
				v.expected, "\n",
				"Received:\n",
				received,
			)
		}
	}

	e := Bools(1, 1, 0, 0)
	testData5 := []struct {
		comparator string
		comparando []bool
		expected   []bool
	}{
		{"==", []bool{true}, []bool{true, true, false, false}},
		{"==", []bool{true, false, false, true}, []bool{true, false, true, false}},
		{"!=", []bool{false}, []bool{true, true, false, false}},
		{"!=", []bool{false, true, true, false}, []bool{true, false, true, false}},
		{"in", []bool{false}, []bool{false, false, true, true}},
		{"in", []bool{false, true}, []bool{true, true, true, true}},
		{"<", []bool{true}, []bool{false, false, true, true}},
		{"<=", []bool{true}, []bool{true, true, true, true}},
		{">", []bool{false}, []bool{true, true, false, false}},
		{">=", []bool{false}, []bool{true, true, true, true}},
	}
	for k, v := range testData5 {
		received, _ := e.Compare(v.comparator, v.comparando)
		if !reflect.DeepEqual(v.expected, received) {
			t.Error(
				"\nTest: ", k+1, "\n",
				"Expected:\n",
				v.expected, "\n",
				"Received:\n",
				received,
			)
		}
	}
}

func TestSeries_Index(t *testing.T) {
	a := Strings("A", "B", "C", "B", "D")
	a2 := Ints(1, 2, 3, nil, 5)
	a3 := Floats(1, 2, 3, nil, 5)
	a4 := Bools(1, 0, 3, nil, 5)
	b, _ := a.Subset([]int{2, 3, 4, 4, 4, 1})
	expected := "C B D D D B"
	received := fmt.Sprint(b)
	if expected != received {
		t.Error(
			"Expected:\n",
			expected, "\n",
			"Received:\n",
			received,
		)
	}
	b, _ = a.Subset([]bool{true, false, false, false, true})
	expected = "A D"
	received = fmt.Sprint(b)
	if expected != received {
		t.Error(
			"Expected:\n",
			expected, "\n",
			"Received:\n",
			received,
		)
	}
	b, _ = a.Subset(Bools([]bool{true, false, false, false, true}))
	expected = "A D"
	received = fmt.Sprint(b)
	if expected != received {
		t.Error(
			"Expected:\n",
			expected, "\n",
			"Received:\n",
			received,
		)
	}
	b, _ = a.Subset(Floats([]float64{2, 3, 4, 4, 4.1, 1}))
	expected = "C B D D D B"
	received = fmt.Sprint(b)
	if expected != received {
		t.Error(
			"Expected:\n",
			expected, "\n",
			"Received:\n",
			received,
		)
	}
	b, _ = a.Subset(Ints([]int{2, 3, 4, 4, 4, 1}))
	expected = "C B D D D B"
	received = fmt.Sprint(b)
	if expected != received {
		t.Error(
			"Expected:\n",
			expected, "\n",
			"Received:\n",
			received,
		)
	}
	b2, _ := a2.Subset([]int{2, 3, 4, 4, 4, 1})
	expected = "3 NA 5 5 5 2"
	received = fmt.Sprint(b2)
	if expected != received {
		t.Error(
			"Expected:\n",
			expected, "\n",
			"Received:\n",
			received,
		)
	}
	b2, _ = a2.Subset([]bool{true, false, false, true, true})
	expected = "1 NA 5"
	received = fmt.Sprint(b2)
	if expected != received {
		t.Error(
			"Expected:\n",
			expected, "\n",
			"Received:\n",
			received,
		)
	}
	b2, _ = a2.Subset(Bools([]bool{true, false, false, true, true}))
	expected = "1 NA 5"
	received = fmt.Sprint(b2)
	if expected != received {
		t.Error(
			"Expected:\n",
			expected, "\n",
			"Received:\n",
			received,
		)
	}
	b2, _ = a2.Subset(Ints([]int{2, 3, 4, 4, 4, 1}))
	expected = "3 NA 5 5 5 2"
	received = fmt.Sprint(b2)
	if expected != received {
		t.Error(
			"Expected:\n",
			expected, "\n",
			"Received:\n",
			received,
		)
	}
	b2, _ = a2.Subset(Floats([]int{2, 3, 4, 4, 4, 1}))
	expected = "3 NA 5 5 5 2"
	received = fmt.Sprint(b2)
	if expected != received {
		t.Error(
			"Expected:\n",
			expected, "\n",
			"Received:\n",
			received,
		)
	}
	b3, _ := a3.Subset([]int{2, 3, 4, 4, 4, 1})
	expected = "3 NA 5 5 5 2"
	received = fmt.Sprint(b3)
	if expected != received {
		t.Error(
			"Expected:\n",
			expected, "\n",
			"Received:\n",
			received,
		)
	}
	b3, _ = a3.Subset([]bool{true, false, false, true, true})
	expected = "1 NA 5"
	received = fmt.Sprint(b3)
	if expected != received {
		t.Error(
			"Expected:\n",
			expected, "\n",
			"Received:\n",
			received,
		)
	}
	b3, _ = a3.Subset(Bools([]bool{true, false, false, true, true}))
	expected = "1 NA 5"
	received = fmt.Sprint(b3)
	if expected != received {
		t.Error(
			"Expected:\n",
			expected, "\n",
			"Received:\n",
			received,
		)
	}
	b3, _ = a3.Subset(Ints([]int{2, 3, 4, 4, 4, 1}))
	expected = "3 NA 5 5 5 2"
	received = fmt.Sprint(b3)
	if expected != received {
		t.Error(
			"Expected:\n",
			expected, "\n",
			"Received:\n",
			received,
		)
	}
	b3, _ = a3.Subset(Floats([]int{2, 3, 4, 4, 4, 1}))
	expected = "3 NA 5 5 5 2"
	received = fmt.Sprint(b3)
	if expected != received {
		t.Error(
			"Expected:\n",
			expected, "\n",
			"Received:\n",
			received,
		)
	}
	b4, _ := a4.Subset([]int{2, 3, 4, 4, 4, 1})
	expected = "true NA true true true false"
	received = fmt.Sprint(b4)
	if expected != received {
		t.Error(
			"Expected:\n",
			expected, "\n",
			"Received:\n",
			received,
		)
	}
	b4, _ = a4.Subset([]bool{true, false, false, true, true})
	expected = "true NA true"
	received = fmt.Sprint(b4)
	if expected != received {
		t.Error(
			"Expected:\n",
			expected, "\n",
			"Received:\n",
			received,
		)
	}
	b4, _ = a4.Subset(Bools([]bool{true, false, false, true, true}))
	expected = "true NA true"
	received = fmt.Sprint(b4)
	if expected != received {
		t.Error(
			"Expected:\n",
			expected, "\n",
			"Received:\n",
			received,
		)
	}
	b4, _ = a4.Subset(Ints([]int{2, 3, 4, 4, 4, 1}))
	expected = "true NA true true true false"
	received = fmt.Sprint(b4)
	if expected != received {
		t.Error(
			"Expected:\n",
			expected, "\n",
			"Received:\n",
			received,
		)
	}
	b4, _ = a4.Subset(Floats([]int{2, 3, 4, 4, 4, 1}))
	expected = "true NA true true true false"
	received = fmt.Sprint(b4)
	if expected != received {
		t.Error(
			"Expected:\n",
			expected, "\n",
			"Received:\n",
			received,
		)
	}
}

func TestStrings(t *testing.T) {
	a := []string{"C", "D"}
	aa := Strings("A", "B", a)
	expected := "A B C D"
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
	expected = "1 2 3 4"
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
	expected = "1.000000 2.000000 3.000000 4.000000"
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
	expected = "NA NA 1.000000 2.000000 3.000000 4.000000 NA B NA"
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
	aa = Strings("a", "b", "c", Ints(4, 5, 6), Floats(6, nil, 0.1), Bools(1, 0, 0))
	expected = "a b c 4 5 6 6 NA 0.1 true false false"
	received = fmt.Sprint(aa)
	if received != expected {
		t.Error(
			"Series not being propery inserted\n",
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
	expected := "NA NA NA NA 1 2"
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
	expected = "1 2 3 4"
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
	expected = "1 2 3 4"
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
	expected = "NA NA 1 2 3 4 NA 1 NA NA"
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

	//_, err := aa.Elements[0].Int()
	//if err == nil {
	//t.Error("Int() Should fail for nil elements")
	//}
}

func TestFloats(t *testing.T) {
	a := []string{"C", "D", "1.1"}
	aa := Floats("A", "B", a, "2.2")
	expected := "NA NA NA NA 1.1 2.2"
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
	expected = "1 2 3 4"
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
	expected = "1.1 2.2 3.6 4.7"
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
	expected = "NA NA 1.1 2.2 3.6 4.7 NA 1 NA NA"
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

	//_, err := aa[0].Float()
	//if err == nil {
	//t.Error("Float() Should fail for nil elements")
	//}
}

func TestBools(t *testing.T) {
	a := []string{"C", "D", "true"}
	aa := Bools("A", "B", a, "false")
	expected := "NA NA NA NA true false"
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
	expected = "true true true false"
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
	expected = "true true false true"
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
	expected = "NA NA true true false true NA true false NA"
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

	//_, err := aa[0].Float()
	//if err == nil {
	//t.Error("Float() Should fail for nil elements")
	//}
}

func TestCopy(t *testing.T) {
	a := Strings(1, 2, 3, "a", "b", "c")
	b := a
	c := Copy(a)
	if fmt.Sprint(a) != fmt.Sprint(b) ||
		fmt.Sprint(a) != fmt.Sprint(c) {
		t.Error(
			"Different values when copying String elements",
		)
	}
	if !reflect.DeepEqual(Addr(a), Addr(b)) {
		t.Error(
			"Different memory address when assigning String elements",
		)
	}
	if reflect.DeepEqual(Addr(a), Addr(c)) {
		t.Error(
			"Same memory address when copying String elements",
		)
	}

	a = Ints(1, 2, 3, "a", "b", "c")
	b = a
	c = Copy(a)
	if fmt.Sprint(a) != fmt.Sprint(b) ||
		fmt.Sprint(a) != fmt.Sprint(c) {
		t.Error(
			"Different values when copying Int elements",
		)
	}
	if !reflect.DeepEqual(Addr(a), Addr(b)) {
		t.Error(
			"Different memory address when assigning Int elements",
		)
	}
	if reflect.DeepEqual(Addr(a), Addr(c)) {
		t.Error(
			"Same memory address when copying Int elements",
		)
	}

	a = Floats(1, 2, 3, 0.1, 0.2)
	b = a
	c = Copy(a)
	if fmt.Sprint(a) != fmt.Sprint(b) ||
		fmt.Sprint(a) != fmt.Sprint(c) {
		t.Error(
			"Different values when copying Float elements",
		)
	}
	if !reflect.DeepEqual(Addr(a), Addr(b)) {
		t.Error(
			"Different memory address when assigning Float elements",
		)
	}
	if reflect.DeepEqual(Addr(a), Addr(c)) {
		t.Error(
			"Same memory address when copying Float elements",
		)
	}

	a = Bools(true, false, 1, 0)
	b = a
	c = Copy(a)
	if fmt.Sprint(a) != fmt.Sprint(b) ||
		fmt.Sprint(a) != fmt.Sprint(c) {
		t.Error(
			"Different values when copying Bool elements",
		)
	}
	if !reflect.DeepEqual(Addr(a), Addr(b)) {
		t.Error(
			"Different memory address when assigning Bool elements",
		)
	}
	if reflect.DeepEqual(Addr(a), Addr(c)) {
		t.Error(
			"Same memory address when copying Bool elements",
		)
	}
}

//func TestInt_Compare(t *testing.T) {
//var tests = []struct {
//a        Cell
//b        Cell
//op       comparator
//expected bool
//}{
//{Ints(1)[0], Ints(1)[0], eq, true},
//{Ints(1)[0], Ints(1)[0], neq, false},
//{Ints(1)[0], Ints(1)[0], gt, false},
//{Ints(1)[0], Ints(1)[0], lt, false},
//{Ints(1)[0], Ints(1)[0], get, true},
//{Ints(1)[0], Ints(1)[0], let, true},

//{Ints(1)[0], Ints(2)[0], eq, false},
//{Ints(1)[0], Ints(2)[0], neq, true},
//{Ints(1)[0], Ints(2)[0], gt, false},
//{Ints(1)[0], Ints(2)[0], lt, true},
//{Ints(1)[0], Ints(2)[0], get, false},
//{Ints(1)[0], Ints(2)[0], let, true},

//{Ints(2)[0], Ints(1)[0], eq, false},
//{Ints(2)[0], Ints(1)[0], neq, true},
//{Ints(2)[0], Ints(1)[0], gt, true},
//{Ints(2)[0], Ints(1)[0], lt, false},
//{Ints(2)[0], Ints(1)[0], get, true},
//{Ints(2)[0], Ints(1)[0], let, false},

//{Ints(1)[0], Strings("1")[0], eq, true},
//{Ints(1)[0], Strings("1")[0], neq, false},
//{Ints(1)[0], Strings("1")[0], gt, false},
//{Ints(1)[0], Strings("1")[0], lt, false},
//{Ints(1)[0], Strings("1")[0], get, true},
//{Ints(1)[0], Strings("1")[0], let, true},

//{Ints(1)[0], Strings("2")[0], eq, false},
//{Ints(1)[0], Strings("2")[0], neq, true},
//{Ints(1)[0], Strings("2")[0], gt, false},
//{Ints(1)[0], Strings("2")[0], lt, true},
//{Ints(1)[0], Strings("2")[0], get, false},
//{Ints(1)[0], Strings("2")[0], let, true},

//{Ints(1)[0], Floats(1)[0], eq, true},
//{Ints(1)[0], Floats(1)[0], neq, false},
//{Ints(1)[0], Floats(1)[0], gt, false},
//{Ints(1)[0], Floats(1)[0], lt, false},
//{Ints(1)[0], Floats(1)[0], get, true},
//{Ints(1)[0], Floats(1)[0], let, true},

//{Ints(1)[0], Floats(2)[0], eq, false},
//{Ints(1)[0], Floats(2)[0], neq, true},
//{Ints(1)[0], Floats(2)[0], gt, false},
//{Ints(1)[0], Floats(2)[0], lt, true},
//{Ints(1)[0], Floats(2)[0], get, false},
//{Ints(1)[0], Floats(2)[0], let, true},

//{Ints(1)[0], Bools(1)[0], eq, true},
//{Ints(1)[0], Bools(1)[0], neq, false},
//{Ints(1)[0], Bools(1)[0], gt, false},
//{Ints(1)[0], Bools(1)[0], lt, false},
//{Ints(1)[0], Bools(1)[0], get, true},
//{Ints(1)[0], Bools(1)[0], let, true},

//{Ints(-1)[0], Bools(0)[0], eq, false},
//{Ints(-1)[0], Bools(0)[0], neq, true},
//{Ints(-1)[0], Bools(0)[0], gt, false},
//{Ints(-1)[0], Bools(0)[0], lt, true},
//{Ints(-1)[0], Bools(0)[0], get, false},
//{Ints(-1)[0], Bools(0)[0], let, true},
//}
//for k, v := range tests {
//res, err := v.a.Compare(v.b, v.op)
//if err != nil {
//t.Error("Error on test", k, ":", err)
//} else {
//if *res != v.expected {
//t.Error("Error on test", k,
//"\nExpected:", v.expected,
//"\nReceived:", *res,
//)
//}
//}
//}
//}

//func TestFloat_Compare(t *testing.T) {
//var tests = []struct {
//a        Cell
//b        Cell
//op       comparator
//expected bool
//}{
//{Floats(1)[0], Ints(1)[0], eq, true},
//{Floats(1)[0], Ints(1)[0], neq, false},
//{Floats(1)[0], Ints(1)[0], gt, false},
//{Floats(1)[0], Ints(1)[0], lt, false},
//{Floats(1)[0], Ints(1)[0], get, true},
//{Floats(1)[0], Ints(1)[0], let, true},

//{Floats(1)[0], Ints(2)[0], eq, false},
//{Floats(1)[0], Ints(2)[0], neq, true},
//{Floats(1)[0], Ints(2)[0], gt, false},
//{Floats(1)[0], Ints(2)[0], lt, true},
//{Floats(1)[0], Ints(2)[0], get, false},
//{Floats(1)[0], Ints(2)[0], let, true},

//{Floats(2)[0], Ints(1)[0], eq, false},
//{Floats(2)[0], Ints(1)[0], neq, true},
//{Floats(2)[0], Ints(1)[0], gt, true},
//{Floats(2)[0], Ints(1)[0], lt, false},
//{Floats(2)[0], Ints(1)[0], get, true},
//{Floats(2)[0], Ints(1)[0], let, false},

//{Floats(1)[0], Strings("1")[0], eq, true},
//{Floats(1)[0], Strings("1")[0], neq, false},
//{Floats(1)[0], Strings("1")[0], gt, false},
//{Floats(1)[0], Strings("1")[0], lt, false},
//{Floats(1)[0], Strings("1")[0], get, true},
//{Floats(1)[0], Strings("1")[0], let, true},

//{Floats(1)[0], Strings("2")[0], eq, false},
//{Floats(1)[0], Strings("2")[0], neq, true},
//{Floats(1)[0], Strings("2")[0], gt, false},
//{Floats(1)[0], Strings("2")[0], lt, true},
//{Floats(1)[0], Strings("2")[0], get, false},
//{Floats(1)[0], Strings("2")[0], let, true},

//{Floats(1)[0], Floats(1)[0], eq, true},
//{Floats(1)[0], Floats(1)[0], neq, false},
//{Floats(1)[0], Floats(1)[0], gt, false},
//{Floats(1)[0], Floats(1)[0], lt, false},
//{Floats(1)[0], Floats(1)[0], get, true},
//{Floats(1)[0], Floats(1)[0], let, true},

//{Floats(1)[0], Floats(2)[0], eq, false},
//{Floats(1)[0], Floats(2)[0], neq, true},
//{Floats(1)[0], Floats(2)[0], gt, false},
//{Floats(1)[0], Floats(2)[0], lt, true},
//{Floats(1)[0], Floats(2)[0], get, false},
//{Floats(1)[0], Floats(2)[0], let, true},

//{Floats(1)[0], Bools(1)[0], eq, true},
//{Floats(1)[0], Bools(1)[0], neq, false},
//{Floats(1)[0], Bools(1)[0], gt, false},
//{Floats(1)[0], Bools(1)[0], lt, false},
//{Floats(1)[0], Bools(1)[0], get, true},
//{Floats(1)[0], Bools(1)[0], let, true},

//{Floats(-1)[0], Bools(0)[0], eq, false},
//{Floats(-1)[0], Bools(0)[0], neq, true},
//{Floats(-1)[0], Bools(0)[0], gt, false},
//{Floats(-1)[0], Bools(0)[0], lt, true},
//{Floats(-1)[0], Bools(0)[0], get, false},
//{Floats(-1)[0], Bools(0)[0], let, true},
//}
//for k, v := range tests {
//res, err := v.a.Compare(v.b, v.op)
//if err != nil {
//t.Error("Error on test", k, ":", err)
//} else {
//if *res != v.expected {
//t.Error("Error on test", k,
//"\nExpected:", v.expected,
//"\nReceived:", *res,
//)
//}
//}
//}
//}

//func TestString_Compare(t *testing.T) {
//var tests = []struct {
//a        Cell
//b        Cell
//op       comparator
//expected bool
//}{
//{Strings(1)[0], Ints(1)[0], eq, true},
//{Strings(1)[0], Ints(1)[0], neq, false},
//{Strings(1)[0], Ints(1)[0], gt, false},
//{Strings(1)[0], Ints(1)[0], lt, false},
//{Strings(1)[0], Ints(1)[0], get, true},
//{Strings(1)[0], Ints(1)[0], let, true},

//{Strings(1)[0], Ints(2)[0], eq, false},
//{Strings(1)[0], Ints(2)[0], neq, true},
//{Strings(1)[0], Ints(2)[0], gt, false},
//{Strings(1)[0], Ints(2)[0], lt, true},
//{Strings(1)[0], Ints(2)[0], get, false},
//{Strings(1)[0], Ints(2)[0], let, true},

//{Strings(2)[0], Ints(1)[0], eq, false},
//{Strings(2)[0], Ints(1)[0], neq, true},
//{Strings(2)[0], Ints(1)[0], gt, true},
//{Strings(2)[0], Ints(1)[0], lt, false},
//{Strings(2)[0], Ints(1)[0], get, true},
//{Strings(2)[0], Ints(1)[0], let, false},

//{Strings(1)[0], Strings("1")[0], eq, true},
//{Strings(1)[0], Strings("1")[0], neq, false},
//{Strings(1)[0], Strings("1")[0], gt, false},
//{Strings(1)[0], Strings("1")[0], lt, false},
//{Strings(1)[0], Strings("1")[0], get, true},
//{Strings(1)[0], Strings("1")[0], let, true},

//{Strings(1)[0], Strings("2")[0], eq, false},
//{Strings(1)[0], Strings("2")[0], neq, true},
//{Strings(1)[0], Strings("2")[0], gt, false},
//{Strings(1)[0], Strings("2")[0], lt, true},
//{Strings(1)[0], Strings("2")[0], get, false},
//{Strings(1)[0], Strings("2")[0], let, true},

//{Strings(1)[0], Floats(1)[0], eq, true},
//{Strings(1)[0], Floats(1)[0], neq, false},
//{Strings(1)[0], Floats(1)[0], gt, false},
//{Strings(1)[0], Floats(1)[0], lt, false},
//{Strings(1)[0], Floats(1)[0], get, true},
//{Strings(1)[0], Floats(1)[0], let, true},

//{Strings(1)[0], Floats(2)[0], eq, false},
//{Strings(1)[0], Floats(2)[0], neq, true},
//{Strings(1)[0], Floats(2)[0], gt, false},
//{Strings(1)[0], Floats(2)[0], lt, true},
//{Strings(1)[0], Floats(2)[0], get, false},
//{Strings(1)[0], Floats(2)[0], let, true},

//{Strings("true")[0], Bools(1)[0], eq, true},
//{Strings("true")[0], Bools(1)[0], neq, false},
//{Strings("true")[0], Bools(1)[0], gt, false},
//{Strings("true")[0], Bools(1)[0], lt, false},
//{Strings("true")[0], Bools(1)[0], get, true},
//{Strings("true")[0], Bools(1)[0], let, true},

//{Strings("true")[0], Bools(0)[0], eq, false},
//{Strings("true")[0], Bools(0)[0], neq, true},
//{Strings("true")[0], Bools(0)[0], gt, true},
//{Strings("true")[0], Bools(0)[0], lt, false},
//{Strings("true")[0], Bools(0)[0], get, true},
//{Strings("true")[0], Bools(0)[0], let, false},

//{Strings("abc")[0], Strings("def")[0], eq, false},
//{Strings("abc")[0], Strings("def")[0], neq, true},
//{Strings("abc")[0], Strings("def")[0], gt, false},
//{Strings("abc")[0], Strings("def")[0], lt, true},
//{Strings("abc")[0], Strings("def")[0], get, false},
//{Strings("abc")[0], Strings("def")[0], let, true},

//{Strings("abc")[0], Strings("ab")[0], eq, false},
//{Strings("abc")[0], Strings("ab")[0], neq, true},
//{Strings("abc")[0], Strings("ab")[0], gt, true},
//{Strings("abc")[0], Strings("ab")[0], lt, false},
//{Strings("abc")[0], Strings("ab")[0], get, true},
//{Strings("abc")[0], Strings("ab")[0], let, false},
//}
//for k, v := range tests {
//res, err := v.a.Compare(v.b, v.op)
//if err != nil {
//t.Error("Error on test", k, ":", err)
//} else {
//if *res != v.expected {
//t.Error("Error on test", k,
//"\nExpected:", v.expected,
//"\nReceived:", *res,
//)
//}
//}
//}
//}

//func TestBool_Compare(t *testing.T) {
//var tests = []struct {
//a        Cell
//b        Cell
//op       comparator
//expected bool
//}{
//{Bools(1)[0], Ints(1)[0], eq, true},
//{Bools(1)[0], Ints(1)[0], neq, false},

//{Bools(1)[0], Ints(0)[0], eq, false},
//{Bools(1)[0], Ints(0)[0], neq, true},

//{Bools(1)[0], Strings("true")[0], eq, true},
//{Bools(1)[0], Strings("true")[0], neq, false},

//{Bools(1)[0], Strings("false")[0], eq, false},
//{Bools(1)[0], Strings("false")[0], neq, true},

//{Bools(1)[0], Floats(1)[0], eq, true},
//{Bools(1)[0], Floats(1)[0], neq, false},

//{Bools(1)[0], Floats(0)[0], eq, false},
//{Bools(1)[0], Floats(0)[0], neq, true},

//{Bools(1)[0], Bools(1)[0], eq, true},
//{Bools(1)[0], Bools(1)[0], neq, false},

//{Bools(0)[0], Ints(0)[0], eq, true},
//{Bools(0)[0], Ints(0)[0], neq, false},

//{Bools(0)[0], Ints(1)[0], eq, false},
//{Bools(0)[0], Ints(1)[0], neq, true},

//{Bools(0)[0], Strings("false")[0], eq, true},
//{Bools(0)[0], Strings("false")[0], neq, false},

//{Bools(0)[0], Strings("true")[0], eq, false},
//{Bools(0)[0], Strings("true")[0], neq, true},

//{Bools(0)[0], Floats(0)[0], eq, true},
//{Bools(0)[0], Floats(0)[0], neq, false},

//{Bools(0)[0], Floats(1)[0], eq, false},
//{Bools(0)[0], Floats(1)[0], neq, true},

//{Bools(0)[0], Bools(0)[0], eq, true},
//{Bools(0)[0], Bools(0)[0], neq, false},
//}
//for k, v := range tests {
//res, err := v.a.Compare(v.b, v.op)
//if err != nil {
//t.Error("Error on test", k, ":", err)
//} else {
//if *res != v.expected {
//t.Error("Error on test", k,
//"\nExpected:", v.expected,
//"\nReceived:", *res,
//)
//}
//}
//}
//}
