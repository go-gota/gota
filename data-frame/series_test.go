package df

import (
	"fmt"
	"math"
	"reflect"
	"testing"
)

//import (
//"fmt"
//"reflect"
//"testing"
//)

//func TestSeries_Compare(t *testing.T) {
//a := Strings("A", "B", "C", "B", "D", "BADA")
//testData := []struct {
//comparator Comparator
//comparando string
//expected   []bool
//}{
//{Eq, "B", []bool{false, true, false, true, false, false}},
//{In, "BADA", []bool{false, false, false, false, false, true}},
//{Neq, "C", []bool{true, true, false, true, true, true}},
//{Less, "B", []bool{true, false, false, false, false, false}},
//{LessEq, "B", []bool{true, true, false, true, false, false}},
//{Greater, "C", []bool{false, false, false, false, true, false}},
//{GreaterEq, "C", []bool{false, false, true, false, true, false}},
//}
//for k, v := range testData {
//received, _ := a.Compare(v.comparator, v.comparando)
//if !reflect.DeepEqual(v.expected, received) {
//t.Error(
//"\nTest: ", k+1, "\n",
//"Expected:\n",
//v.expected, "\n",
//"Received:\n",
//received,
//)
//}
//}
//b := Strings("A", "B", "A")
//testData2 := []struct {
//comparator Comparator
//comparando []string
//expected   []bool
//}{
//{Eq, []string{"B", "A", "A"}, []bool{false, false, true}},
//{Neq, []string{"B", "B", "A"}, []bool{true, false, false}},
//{In, []string{"C", "A"}, []bool{true, false, true}},
//{In, []string{"B"}, []bool{false, true, false}},
//{In, []string{"A", "B"}, []bool{true, true, true}},
//{Less, []string{"B", "B", "A"}, []bool{true, false, false}},
//{LessEq, []string{"B", "B", "A"}, []bool{true, true, true}},
//{Greater, []string{"B", "B", "A"}, []bool{false, false, false}},
//{GreaterEq, []string{"B", "B", "A"}, []bool{false, true, true}},
//}
//for k, v := range testData2 {
//received, _ := b.Compare(v.comparator, v.comparando)
//if !reflect.DeepEqual(v.expected, received) {
//t.Error(
//"\nTest: ", k+1, "\n",
//"Expected:\n",
//v.expected, "\n",
//"Received:\n",
//received,
//)
//}
//}

//c := Ints(1, 2, 3, 2, 1)
//testData3 := []struct {
//comparator Comparator
//comparando []int
//expected   []bool
//}{
//{Eq, []int{1}, []bool{true, false, false, false, true}},
//{Eq, []int{1, 3, 3, 1, 1}, []bool{true, false, true, false, true}},
//{Neq, []int{3}, []bool{true, true, false, true, true}},
//{Neq, []int{1, 3, 3, 1, 1}, []bool{false, true, false, true, false}},
//{In, []int{5, 6, 7}, []bool{false, false, false, false, false}},
//{In, []int{2, 3}, []bool{false, true, true, true, false}},
//{Less, []int{2}, []bool{true, false, false, false, true}},
//{Less, []int{3}, []bool{true, true, false, true, true}},
//{Less, []int{2, 2, 2, 1, 1}, []bool{true, false, false, false, false}},
//{LessEq, []int{2}, []bool{true, true, false, true, true}},
//{LessEq, []int{2, 2, 2, 1, 1}, []bool{true, true, false, false, true}},
//{Greater, []int{2}, []bool{false, false, true, false, false}},
//{Greater, []int{2, 1, 2, 1, 1}, []bool{false, true, true, true, false}},
//{GreaterEq, []int{2}, []bool{false, true, true, true, false}},
//{GreaterEq, []int{2, 1, 2, 1, 1}, []bool{false, true, true, true, true}},
//}
//for k, v := range testData3 {
//received, _ := c.Compare(v.comparator, v.comparando)
//if !reflect.DeepEqual(v.expected, received) {
//t.Error(
//"\nTest: ", k+1, "\n",
//"Expected:\n",
//v.expected, "\n",
//"Received:\n",
//received,
//)
//}
//}

//d := Floats(1, 2, 3, 2, 1)
//testData4 := []struct {
//comparator Comparator
//comparando []int
//expected   []bool
//}{
//{Eq, []int{1}, []bool{true, false, false, false, true}},
//{Eq, []int{1, 3, 3, 1, 1}, []bool{true, false, true, false, true}},
//{Neq, []int{3}, []bool{true, true, false, true, true}},
//{Neq, []int{1, 3, 3, 1, 1}, []bool{false, true, false, true, false}},
//{In, []int{5, 6, 7}, []bool{false, false, false, false, false}},
//{In, []int{2, 3}, []bool{false, true, true, true, false}},
//{Less, []int{2}, []bool{true, false, false, false, true}},
//{Less, []int{3}, []bool{true, true, false, true, true}},
//{Less, []int{2, 2, 2, 1, 1}, []bool{true, false, false, false, false}},
//{LessEq, []int{2}, []bool{true, true, false, true, true}},
//{LessEq, []int{2, 2, 2, 1, 1}, []bool{true, true, false, false, true}},
//{Greater, []int{2}, []bool{false, false, true, false, false}},
//{Greater, []int{2, 1, 2, 1, 1}, []bool{false, true, true, true, false}},
//{GreaterEq, []int{2}, []bool{false, true, true, true, false}},
//{GreaterEq, []int{2, 1, 2, 1, 1}, []bool{false, true, true, true, true}},
//}
//for k, v := range testData4 {
//received, _ := d.Compare(v.comparator, v.comparando)
//if !reflect.DeepEqual(v.expected, received) {
//t.Error(
//"\nTest: ", k+1, "\n",
//"Expected:\n",
//v.expected, "\n",
//"Received:\n",
//received,
//)
//}
//}

//e := Bools(1, 1, 0, 0)
//testData5 := []struct {
//comparator Comparator
//comparando []bool
//expected   []bool
//}{
//{Eq, []bool{true}, []bool{true, true, false, false}},
//{Eq, []bool{true, false, false, true}, []bool{true, false, true, false}},
//{Neq, []bool{false}, []bool{true, true, false, false}},
//{Neq, []bool{false, true, true, false}, []bool{true, false, true, false}},
//{In, []bool{false}, []bool{false, false, true, true}},
//{In, []bool{false, true}, []bool{true, true, true, true}},
//{Less, []bool{true}, []bool{false, false, true, true}},
//{LessEq, []bool{true}, []bool{true, true, true, true}},
//{Greater, []bool{false}, []bool{true, true, false, false}},
//{GreaterEq, []bool{false}, []bool{true, true, true, true}},
//}
//for k, v := range testData5 {
//received, _ := e.Compare(v.comparator, v.comparando)
//if !reflect.DeepEqual(v.expected, received) {
//t.Error(
//"\nTest: ", k+1, "\n",
//"Expected:\n",
//v.expected, "\n",
//"Received:\n",
//received,
//)
//}
//}
//}

func TestSeries_Index(t *testing.T) {
	table := []struct {
		series   Series
		indexes  interface{}
		expected string
	}{
		{
			Strings([]string{"A", "B", "C", "K", "D"}),
			[]int{2, 1, 4, 4, 0, 3},
			"[C B D D A K]",
		},
		{
			Strings([]string{"A", "B", "C", "K", "D"}),
			int(1),
			"[B]",
		},
		{
			Strings([]string{"A", "B", "C", "K", "D"}),
			[]bool{true, false, false, true, true},
			"[A K D]",
		},
		{
			Strings([]string{"A", "B", "C", "K", "D"}),
			Ints([]int{3, 2, 1, 0}),
			"[K C B A]",
		},
		{
			Strings([]string{"A", "B", "C", "K", "D"}),
			Ints([]int{1}),
			"[B]",
		},
		{
			Strings([]string{"A", "B", "C", "K", "D"}),
			Ints(2),
			"[C]",
		},
		{
			Strings([]string{"A", "B", "C", "K", "D"}),
			Bools([]bool{true, false, false, true, true}),
			"[A K D]",
		},
	}
	for testnum, test := range table {
		sub := test.series.Subset(test.indexes)
		if sub.Err() != nil {
			t.Errorf(
				"Test:%v\nError:%v",
				testnum, sub.Err(),
			)
			continue
		}
		expected := test.expected
		received := fmt.Sprint(sub)
		if expected != received {
			t.Errorf(
				"Test:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, expected, received,
			)
		}
	}
}

func checkTypes(s Series) error {
	var types []Type
	for _, e := range s.elements {
		types = append(types, e.Type())
	}
	for _, t := range types {
		if t != s.t {
			return fmt.Errorf("bad types for %v Series:\n%v", s.t, types)
		}
	}
	return nil
}

func TestStrings(t *testing.T) {
	table := []struct {
		series   Series
		expected string
	}{
		{
			Strings([]string{"A", "B", "C", "D"}),
			"[A B C D]",
		},
		{
			Strings([]string{"A"}),
			"[A]",
		},
		{
			Strings("A"),
			"[A]",
		},
		{
			Strings([]int{1, 2, 3}),
			"[1 2 3]",
		},
		{
			Strings([]int{2}),
			"[2]",
		},
		{
			Strings(-1),
			"[-1]",
		},
		{
			Strings([]float64{1, 2, 3}),
			"[1.000000 2.000000 3.000000]",
		},
		{
			Strings([]float64{2}),
			"[2.000000]",
		},
		{
			Strings(-1.0),
			"[-1.000000]",
		},
		{
			Strings(math.NaN()),
			"[NaN]",
		},
		{
			Strings(math.Inf(1)),
			"[+Inf]",
		},
		{
			Strings(math.Inf(-1)),
			"[-Inf]",
		},
		{
			Strings([]bool{true, true, false}),
			"[true true false]",
		},
		{
			Strings([]bool{false}),
			"[false]",
		},
		{
			Strings(true),
			"[true]",
		},
		{
			Strings(nil),
			"[NaN]",
		},
		{
			Strings(Strings([]string{"A", "B", "C"})),
			"[A B C]",
		},
	}
	for testnum, test := range table {
		expected := test.expected
		received := fmt.Sprint(test.series)
		if expected != received {
			t.Errorf(
				"Test:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, expected, received,
			)
		}
		err := checkTypes(test.series)
		if err != nil {
			t.Errorf("Test:%v\nError:%v", testnum, err)
		}
	}
}

func TestInts(t *testing.T) {
	table := []struct {
		series   Series
		expected string
	}{
		{
			Ints([]string{"A", "B", "1", "2"}),
			"[NaN NaN 1 2]",
		},
		{
			Ints([]string{"1"}),
			"[1]",
		},
		{
			Ints("2"),
			"[2]",
		},
		{
			Ints([]int{1, 2, 3}),
			"[1 2 3]",
		},
		{
			Ints([]int{2}),
			"[2]",
		},
		{
			Ints(-1),
			"[-1]",
		},
		{
			Ints([]float64{1, 2, 3}),
			"[1 2 3]",
		},
		{
			Ints([]float64{2}),
			"[2]",
		},
		{
			Ints(-1.0),
			"[-1]",
		},
		{
			Ints(math.NaN()),
			"[NaN]",
		},
		{
			Ints(math.Inf(1)),
			"[NaN]",
		},
		{
			Ints(math.Inf(-1)),
			"[NaN]",
		},
		{
			Ints([]bool{true, true, false}),
			"[1 1 0]",
		},
		{
			Ints([]bool{false}),
			"[0]",
		},
		{
			Ints(true),
			"[1]",
		},
		{
			Ints(nil),
			"[NaN]",
		},
		{
			Ints(Strings([]string{"1", "2", "3"})),
			"[1 2 3]",
		},
		{
			Ints(Ints([]string{"1", "2", "3"})),
			"[1 2 3]",
		},
	}
	for testnum, test := range table {
		expected := test.expected
		received := fmt.Sprint(test.series)
		if expected != received {
			t.Errorf(
				"Test:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, expected, received,
			)
		}
		err := checkTypes(test.series)
		if err != nil {
			t.Errorf("Test:%v\nError:%v", testnum, err)
		}
	}
}

func TestFloats(t *testing.T) {
	table := []struct {
		series   Series
		expected string
	}{
		{
			Floats([]string{"A", "B", "1", "2"}),
			"[NaN NaN 1.000000 2.000000]",
		},
		{
			Floats([]string{"1"}),
			"[1.000000]",
		},
		{
			Floats("2.1"),
			"[2.100000]",
		},
		{
			Floats([]int{1, 2, 3}),
			"[1.000000 2.000000 3.000000]",
		},
		{
			Floats([]int{2}),
			"[2.000000]",
		},
		{
			Floats(-1),
			"[-1.000000]",
		},
		{
			Floats([]float64{1.1, 2, 3}),
			"[1.100000 2.000000 3.000000]",
		},
		{
			Floats([]float64{2}),
			"[2.000000]",
		},
		{
			Floats(-1.0),
			"[-1.000000]",
		},
		{
			Floats(math.NaN()),
			"[NaN]",
		},
		{
			Floats(math.Inf(1)),
			"[+Inf]",
		},
		{
			Floats(math.Inf(-1)),
			"[-Inf]",
		},
		{
			Floats([]bool{true, true, false}),
			"[1.000000 1.000000 0.000000]",
		},
		{
			Floats([]bool{false}),
			"[0.000000]",
		},
		{
			Floats(true),
			"[1.000000]",
		},
		{
			Floats(nil),
			"[NaN]",
		},
		{
			Floats(Strings([]string{"1", "2", "3"})),
			"[1.000000 2.000000 3.000000]",
		},
	}
	for testnum, test := range table {
		expected := test.expected
		received := fmt.Sprint(test.series)
		if expected != received {
			t.Errorf(
				"Test:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, expected, received,
			)
		}
		err := checkTypes(test.series)
		if err != nil {
			t.Errorf("Test:%v\nError:%v", testnum, err)
		}
	}
}

func TestBools(t *testing.T) {
	table := []struct {
		series   Series
		expected string
	}{
		{
			Bools([]string{"A", "true", "1", "f"}),
			"[NaN true true false]",
		},
		{
			Bools([]string{"t"}),
			"[true]",
		},
		{
			Bools("False"),
			"[false]",
		},
		{
			Bools([]int{1, 2, 0}),
			"[true NaN false]",
		},
		{
			Bools([]int{1}),
			"[true]",
		},
		{
			Bools(-1),
			"[NaN]",
		},
		{
			Bools([]float64{1, 2, 0}),
			"[true NaN false]",
		},
		{
			Bools([]float64{0}),
			"[false]",
		},
		{
			Bools(-1.0),
			"[NaN]",
		},
		{
			Bools(math.NaN()),
			"[NaN]",
		},
		{
			Bools(math.Inf(1)),
			"[NaN]",
		},
		{
			Bools(math.Inf(-1)),
			"[NaN]",
		},
		{
			Bools([]bool{true, true, false}),
			"[true true false]",
		},
		{
			Bools([]bool{false}),
			"[false]",
		},
		{
			Bools(true),
			"[true]",
		},
		{
			Bools(nil),
			"[NaN]",
		},
		{
			Bools(Strings([]string{"1", "0", "1"})),
			"[true false true]",
		},
	}
	for testnum, test := range table {
		expected := test.expected
		received := fmt.Sprint(test.series)
		if expected != received {
			t.Errorf(
				"Test:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, expected, received,
			)
		}
		err := checkTypes(test.series)
		if err != nil {
			t.Errorf("Test:%v\nError:%v", testnum, err)
		}
	}
}

func TestSeries_Copy(t *testing.T) {
	tests := []Series{
		Strings([]string{"1", "2", "3", "a", "b", "c"}),
		Ints([]string{"1", "2", "3", "a", "b", "c"}),
		Floats([]string{"1", "2", "3", "a", "b", "c"}),
		Bools([]string{"1", "0", "1", "t", "f", "c"}),
	}
	for testnum, test := range tests {
		a := test
		b := a.Copy()
		if fmt.Sprint(a) != fmt.Sprint(b) {
			t.Error("Different values when copying String elements")
		}
		if reflect.DeepEqual(addr(a), addr(b)) {
			t.Errorf("Test:%v\nSame memory address:\na:%v\nb:%v", testnum, addr(a), addr(b))
		}
	}
}

func TestSeries_Records(t *testing.T) {
	tests := []struct {
		series   Series
		expected []string
	}{
		{
			Strings([]string{"1", "2", "3", "a", "b", "c"}),
			[]string{"1", "2", "3", "a", "b", "c"},
		},
		{
			Ints([]string{"1", "2", "3", "a", "b", "c"}),
			[]string{"1", "2", "3", "NaN", "NaN", "NaN"},
		},
		{
			Floats([]string{"1", "2", "3", "a", "b", "c"}),
			[]string{"1.000000", "2.000000", "3.000000", "NaN", "NaN", "NaN"},
		},
		{
			Bools([]string{"1", "0", "1", "t", "f", "c"}),
			[]string{"true", "false", "true", "true", "false", "NaN"},
		},
	}
	for testnum, test := range tests {
		expected := test.expected
		received := test.series.Records()
		if !reflect.DeepEqual(expected, received) {
			t.Errorf(
				"Test:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, expected, received,
			)
		}
	}
}

func TestSeries_Float(t *testing.T) {
	var precision float64 = 0.0000001
	floatEquals := func(x, y []float64) bool {
		if len(x) != len(y) {
			return false
		}
		for i := 0; i < len(x); i++ {
			a := x[i]
			b := y[i]
			if (a-b) > precision || (b-a) > precision {
				return false
			}
		}
		return true
	}
	tests := []struct {
		series   Series
		expected []float64
	}{
		{
			Strings([]string{"1", "2", "3", "a", "b", "c"}),
			[]float64{1, 2, 3, math.NaN(), math.NaN(), math.NaN()},
		},
		{
			Ints([]string{"1", "2", "3", "a", "b", "c"}),
			[]float64{1, 2, 3, math.NaN(), math.NaN(), math.NaN()},
		},
		{
			Floats([]string{"1", "2", "3", "a", "b", "c"}),
			[]float64{1, 2, 3, math.NaN(), math.NaN(), math.NaN()},
		},
		{
			Bools([]string{"1", "0", "1", "t", "f", "c"}),
			[]float64{1, 0, 1, 1, 0, math.NaN()},
		},
	}
	for testnum, test := range tests {
		expected := test.expected
		received := test.series.Float()
		if !floatEquals(expected, received) {
			t.Errorf(
				"Test:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, expected, received,
			)
		}
	}
}

func TestSeries_Concat(t *testing.T) {
	tests := []struct {
		a        Series
		b        Series
		expected []string
	}{
		{
			Strings([]string{"1", "2", "3"}),
			Strings([]string{"a", "b", "c"}),
			[]string{"1", "2", "3", "a", "b", "c"},
		},
		{
			Ints([]string{"1", "2", "3"}),
			Ints([]string{"a", "4", "c"}),
			[]string{"1", "2", "3", "NaN", "4", "NaN"},
		},
		{
			Floats([]string{"1", "2", "3"}),
			Floats([]string{"a", "4", "c"}),
			[]string{"1.000000", "2.000000", "3.000000", "NaN", "4.000000", "NaN"},
		},
		{
			Bools([]string{"1", "1", "0"}),
			Bools([]string{"0", "0", "0"}),
			[]string{"true", "true", "false", "false", "false", "false"},
		},
	}
	for testnum, test := range tests {
		ab := test.a.Concat(test.b)
		received := ab.Records()
		expected := test.expected
		if !reflect.DeepEqual(expected, received) {
			t.Errorf(
				"Test:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, expected, received,
			)
		}
		addrorig := append(addr(test.a), addr(test.b)...)
		addrcombined := addr(ab)
		if reflect.DeepEqual(addrorig, addrcombined) {
			t.Errorf(
				"Test:%v\nSame memory addresses:\nOriginal:\n%v\nCombined:\n%v",
				testnum, addrorig, addrcombined,
			)
		}
	}
}

//func TestEq(t *testing.T) {
//s1 := "123"
//s2 := "Hello"
//a := stringElement{&s1}
//b := stringElement{&s2}
//if !a.Eq(a) || a.Eq(b) {
//t.Error("String Eq() not working properly")
//}
//i1 := 123
//i2 := 234
//c := intElement{&i1}
//d := intElement{&i2}
//if !c.Eq(c) || d.Eq(c) {
//t.Error("Int Eq() not working properly")
//}
//if !c.Eq(a) || c.Eq(b) || c.Eq(stringElement{nil}) {
//t.Error("Int Eq() not working properly")
//}
//if !a.Eq(c) || a.Eq(d) || a.Eq(stringElement{nil}) {
//t.Error("String Eq() not working properly")
//}
//fval1 := 123.0
//fval2 := 321.456
//f1 := floatElement{&fval1}
//f2 := floatElement{&fval2}
//if !f1.Eq(f1) || f1.Eq(f2) {
//t.Error("Float Eq() not working properly")
//}
//if !f1.Eq(c) || f1.Eq(d) || f1.Eq(stringElement{nil}) {
//t.Error("Float Eq() not working properly")
//}
//}
