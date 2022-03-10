package series

import (
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
)

// Check that there are no shared memory addreses between the elements of two Series
//func checkAddr(addra, addrb []string) error {
//for i := 0; i < len(addra); i++ {
//for j := 0; j < len(addrb); j++ {
//if addra[i] == "<nil>" || addrb[j] == "<nil>" {
//continue
//}
//if addra[i] == addrb[j] {
//return fmt.Errorf("found same address on\nA:%v\nB:%v", i, j)
//}
//}
//}
//return nil
//}

// Check that all the types on a Series are the same type and that it matches with
// Series.t
func checkTypes(s Series) error {
	var types []Type
	for i := 0; i < s.Len(); i++ {
		e := s.elements.Elem(i)
		types = append(types, e.Type())
	}
	for _, t := range types {
		if t != s.t {
			return fmt.Errorf("bad types for %v Series:\n%v", s.t, types)
		}
	}
	return nil
}

// compareFloats compares floating point values up to the number of digits specified.
// Returns true if both values are equal with the given precision
func compareFloats(lvalue, rvalue float64, digits int) bool {
	if math.IsNaN(lvalue) || math.IsNaN(rvalue) {
		return math.IsNaN(lvalue) && math.IsNaN(rvalue)
	}
	d := math.Pow(10.0, float64(digits))
	lv := int(lvalue * d)
	rv := int(rvalue * d)
	return lv == rv
}

// compareSeries compares two series using Eq comparator.
// Returns true if Eq's bool output is all true.
func compareSeries(x, y Series) (bool, error) {
	equals := x.Compare(Eq, y)
	if err := equals.Err; err != nil {
		return false, err
	}
	for i := 0; i < equals.Len(); i++ {
		eq, err := equals.Elem(i).Bool()
		if err != nil {
			return false, nil
		}
		if !eq {
			return false, nil
		}
	}
	return true, nil
}

func TestSeries_Compare(t *testing.T) {
	table := []struct {
		series     Series
		comparator Comparator
		comparando interface{}
		expected   Series
	}{
		{
			Strings([]string{"A", "B", "C", "B", "D", "BADA"}),
			Eq,
			"B",
			Bools([]bool{false, true, false, true, false, false}),
		},
		{
			Strings([]string{"B"}),
			Eq,
			Strings([]string{"A", "B", "C", "B", "D", "BADA"}),
			Bools([]bool{false, true, false, true, false, false}),
		},
		{
			Strings([]string{"A", "B", "C", "B", "D", "BADA"}),
			Eq,
			[]string{"B", "B", "C", "D", "A", "A"},
			Bools([]bool{false, true, true, false, false, false}),
		},
		{
			Ints([]int{0, 2, 1, 5, 9}),
			Eq,
			[]int{0, 2, 0, 5, 10},
			Bools([]bool{true, true, false, true, false}),
		},
		{
			Floats([]float64{0.1, 2, 1, 5, 9}),
			Eq,
			"2",
			Bools([]bool{false, true, false, false, false}),
		},
		{
			Floats([]float64{0.1, 2, 1, 5, 9}),
			Eq,
			[]float64{0.1, 2, 0, 5, 10},
			Bools([]bool{true, true, false, true, false}),
		},
		{
			Bools([]bool{true, true, false}),
			Eq,
			"true",
			Bools([]bool{true, true, false}),
		},
		{
			Bools([]bool{true, true, false}),
			Eq,
			[]bool{true, false, false},
			Bools([]bool{true, false, true}),
		},
		{
			StringsList([][]string{{"AA", "BB"}, {"CC", "DD"}, {"EEE", "FFF"}}),
			Eq,
			[]string{"CC", "DD"},
			Bools([]bool{false, true, false}),
		},
		{
			StringsList([][]string{{"AA", "BB"}, {"CC", "DD"}, {"EEE", "FFF"}}),
			Eq,
			[][]string{{"CC", "DD"}},
			Bools([]bool{false, true, false}),
		},
		{
			StringsList([][]string{{"AA", "BB"}, {"CC", "DD"}, {"EEE", "FFF"}}),
			Eq,
			[][]string{{"AAA", "BBB"}, {"CC", "DD"}, {"EE", "FF"}},
			Bools([]bool{false, true, false}),
		},
		{
			IntsList([][]int{{0, 1}, {2, 3}, {1, 2}, {5, 6}, {9, 10}}),
			Eq,
			[]int{2, 3},
			Bools([]bool{false, true, false, false, false}),
		},
		{
			IntsList([][]int{{0, 1}, {2, 3}, {1, 2}, {5, 6}, {9, 10}}),
			Eq,
			[][]int{{2, 3}},
			Bools([]bool{false, true, false, false, false}),
		},
		{
			IntsList([][]int{{0, 1}, {2, 3}, {1, 2}, {5, 6}, {9, 10}}),
			Eq,
			[][]int{{0, 0}, {2, 3}, {1, 2}, {6, 6}, {9, 9}},
			Bools([]bool{false, true, true, false, false}),
		},
		{
			FloatsList([][]float64{{0.1, 0.2}, {2, 4.2}, {1, 0}, {5, 0.25}, {9, 8.1}}),
			Eq,
			[]float64{0.1, 0.2},
			Bools([]bool{true, false, false, false, false}),
		},
		{
			FloatsList([][]float64{{0.1, 0.2}, {2, 4.2}, {1, 0}, {5, 0.25}, {9, 8.1}}),
			Eq,
			[][]float64{{0.1, 0.2}},
			Bools([]bool{true, false, false, false, false}),
		},
		{
			FloatsList([][]float64{{0.1, 0.2}, {2, 4.2}, {1, 0}, {5, 0.25}, {9, 8.1}}),
			Eq,
			[][]float64{{0.2, 0.1}, {2, 4.2}, {1, 0}, {5, 0.225}, {9.9, 8.1}},
			Bools([]bool{false, true, true, false, false}),
		},
		{
			BoolsList([][]bool{{true, true}, {true, false}, {false, false}}),
			Eq,
			[]bool{false, false},
			Bools([]bool{false, false, true}),
		},
		{
			BoolsList([][]bool{{true, true}, {true, false}, {false, false}}),
			Eq,
			[][]bool{{false, false}},
			Bools([]bool{false, false, true}),
		},
		{
			BoolsList([][]bool{{true, true}, {true, false}, {false, false}}),
			Eq,
			[][]bool{{true, false}, {true, false}, {false, false}},
			Bools([]bool{false, true, true}),
		},
		{
			Strings([]string{"A", "B", "C", "B", "D", "BADA"}),
			Neq,
			"B",
			Bools([]bool{true, false, true, false, true, true}),
		},
		{
			Strings([]string{"A", "B", "C", "B", "D", "BADA"}),
			Neq,
			[]string{"B", "B", "C", "D", "A", "A"},
			Bools([]bool{true, false, false, true, true, true}),
		},
		{
			Ints([]int{0, 2, 1, 5, 9}),
			Neq,
			"2",
			Bools([]bool{true, false, true, true, true}),
		},
		{
			Ints([]int{0, 2, 1, 5, 9}),
			Neq,
			[]int{0, 2, 0, 5, 10},
			Bools([]bool{false, false, true, false, true}),
		},
		{
			Floats([]float64{0.1, 2, 1, 5, 9}),
			Neq,
			"2",
			Bools([]bool{true, false, true, true, true}),
		},
		{
			Floats([]float64{0.1, 2, 1, 5, 9}),
			Neq,
			[]float64{0.1, 2, 0, 5, 10},
			Bools([]bool{false, false, true, false, true}),
		},
		{
			Bools([]bool{true, true, false}),
			Neq,
			"true",
			Bools([]bool{false, false, true}),
		},
		{
			Bools([]bool{true, true, false}),
			Neq,
			[]bool{true, false, false},
			Bools([]bool{false, true, false}),
		},
		{
			StringsList([][]string{{"AA", "BB"}, {"CC", "DD"}, {"EEE", "FFF"}}),
			Neq,
			[]string{"CC", "DD"},
			Bools([]bool{true, false, true}),
		},
		{
			StringsList([][]string{{"AA", "BB"}, {"CC", "DD"}, {"EEE", "FFF"}}),
			Neq,
			[][]string{{"CC", "DD"}},
			Bools([]bool{true, false, true}),
		},
		{
			StringsList([][]string{{"AA", "BB"}, {"CC", "DD"}, {"EEE", "FFF"}}),
			Neq,
			[][]string{{"AAA", "BBB"}, {"CC", "DD"}, {"EE", "FF"}},
			Bools([]bool{true, false, true}),
		},
		{
			IntsList([][]int{{0, 1}, {2, 3}, {1, 2}, {5, 6}, {9, 10}}),
			Neq,
			[]int{2, 3},
			Bools([]bool{true, false, true, true, true}),
		},
		{
			IntsList([][]int{{0, 1}, {2, 3}, {1, 2}, {5, 6}, {9, 10}}),
			Neq,
			[][]int{{2, 3}},
			Bools([]bool{true, false, true, true, true}),
		},
		{
			IntsList([][]int{{0, 1}, {2, 3}, {1, 2}, {5, 6}, {9, 10}}),
			Neq,
			[][]int{{0, 0}, {2, 3}, {1, 2}, {6, 6}, {9, 9}},
			Bools([]bool{true, false, false, true, true}),
		},
		{
			FloatsList([][]float64{{0.1, 0.2}, {2, 4.2}, {1, 0}, {5, 0.25}, {9, 8.1}}),
			Neq,
			[]float64{0.1, 0.2},
			Bools([]bool{false, true, true, true, true}),
		},
		{
			FloatsList([][]float64{{0.1, 0.2}, {2, 4.2}, {1, 0}, {5, 0.25}, {9, 8.1}}),
			Neq,
			[][]float64{{0.1, 0.2}},
			Bools([]bool{false, true, true, true, true}),
		},
		{
			FloatsList([][]float64{{0.1, 0.2}, {2, 4.2}, {1, 0}, {5, 0.25}, {9, 8.1}}),
			Neq,
			[][]float64{{0.2, 0.1}, {2, 4.2}, {1, 0}, {5, 0.225}, {9.9, 8.1}},
			Bools([]bool{true, false, false, true, true}),
		},
		{
			BoolsList([][]bool{{true, true}, {true, false}, {false, false}}),
			Neq,
			[]bool{false, false},
			Bools([]bool{true, true, false}),
		},
		{
			BoolsList([][]bool{{true, true}, {true, false}, {false, false}}),
			Neq,
			[][]bool{{false, false}},
			Bools([]bool{true, true, false}),
		},
		{
			BoolsList([][]bool{{true, true}, {true, false}, {false, false}}),
			Neq,
			[][]bool{{true, false}, {true, false}, {false, false}},
			Bools([]bool{true, false, false}),
		},
		{
			Strings([]string{"A", "B", "C", "B", "D", "BADA"}),
			Greater,
			"B",
			Bools([]bool{false, false, true, false, true, true}),
		},
		{
			Strings([]string{"A", "B", "C", "B", "D", "BADA"}),
			Greater,
			[]string{"B", "B", "C", "D", "A", "A"},
			Bools([]bool{false, false, false, false, true, true}),
		},
		{
			Ints([]int{0, 2, 1, 5, 9}),
			Greater,
			"2",
			Bools([]bool{false, false, false, true, true}),
		},
		{
			Ints([]int{0, 2, 1, 5, 9}),
			Greater,
			[]int{0, 2, 0, 5, 10},
			Bools([]bool{false, false, true, false, false}),
		},
		{
			Floats([]float64{0.1, 2, 1, 5, 9}),
			Greater,
			"2",
			Bools([]bool{false, false, false, true, true}),
		},
		{
			Floats([]float64{0.1, 2, 1, 5, 9}),
			Greater,
			[]float64{0.1, 2, 0, 5, 10},
			Bools([]bool{false, false, true, false, false}),
		},
		{
			Bools([]bool{true, true, false}),
			Greater,
			"true",
			Bools([]bool{false, false, false}),
		},
		{
			Bools([]bool{true, true, false}),
			Greater,
			[]bool{true, false, false},
			Bools([]bool{false, true, false}),
		},
		{
			StringsList([][]string{{"A"}, {"B"}, {"C"}, {"B"}, {"D"}, {"BADA"}}),
			Greater,
			[]string{"B"},
			Bools([]bool{false, false, true, false, true, true}),
		},
		{
			StringsList([][]string{{"A"}, {"B"}, {"C"}, {"B"}, {"D"}, {"BADA"}}),
			Greater,
			[][]string{{"B"}},
			Bools([]bool{false, false, true, false, true, true}),
		},
		{
			StringsList([][]string{{"A"}, {"B"}, {"C"}, {"B"}, {"D"}, {"BADA"}}),
			Greater,
			[][]string{{"0"}, {"B"}, {"B"}, {"B"}, {"D"}, {"BACA"}},
			Bools([]bool{true, false, true, false, false, true}),
		},
		{
			StringsList([][]string{{"A", "A"}, {"B", "B"}, {"C", "C"}, {"B"}, {"D"}, {"BADA"}, {"BADA", "BACA"}}),
			Greater,
			[]string{"B", "B"},
			Bools([]bool{false, false, true, false, false, false, true}),
		},
		{
			StringsList([][]string{{"A", "A"}, {"B", "B"}, {"C", "C"}, {"B"}, {"D"}, {"BADA"}, {"BADA", "BACA"}}),
			Greater,
			[][]string{{"B", "B"}},
			Bools([]bool{false, false, true, false, false, false, true}),
		},
		{
			StringsList([][]string{{"A", "A"}, {"B", "B"}, {"C", "C"}, {"B"}, {"D"}, {"BADA"}, {"BADA", "BACA"}, {"A", "B", "C"}}),
			Greater,
			[][]string{{"0", "0"}, {"B", "B"}, {"B", "B"}, {"B"}, {"D"}, {"AADA"}, {"AADA", "AACA"}, {"A", "B"}},
			Bools([]bool{true, false, true, false, false, true, true, true}),
		},
		{
			IntsList([][]int{{0}, {2}, {1}, {5}, {9}}),
			Greater,
			[]int{2},
			Bools([]bool{false, false, false, true, true}),
		},
		{
			IntsList([][]int{{0}, {2}, {1}, {5}, {9}}),
			Greater,
			[][]int{{2}},
			Bools([]bool{false, false, false, true, true}),
		},
		{
			IntsList([][]int{{0}, {2}, {1}, {5}, {9}}),
			Greater,
			[][]int{{0}, {2}, {0}, {5}, {10}},
			Bools([]bool{false, false, true, false, false}),
		},
		{
			IntsList([][]int{{1, 0}, {2}, {1}, {5}, {9}}),
			Greater,
			[][]int{{0, 0}, {2, 1}, {0}, {5}, {10}},
			Bools([]bool{false, false, true, false, false}),
		},
		{
			IntsList([][]int{{1, 0}, {2}, {1}, {5}, {9}, {10, 100, 1000}}),
			Greater,
			[][]int{{0, 0}, {2, 1}, {0}, {5}, {10}, {10}},
			Bools([]bool{false, false, true, false, false, true}),
		},
		{
			FloatsList([][]float64{{0.1, 0.2}, {2, 4.2}, {1, 0}, {5, 0.25}, {9, 8.1}}),
			Greater,
			[]float64{0.1, 0.2},
			Bools([]bool{false, true, false, true, true}),
		},
		{
			FloatsList([][]float64{{0.1, 0.2}, {2, 4.2}, {1, 0}, {5, 0.25}, {9, 8.1}}),
			Greater,
			[][]float64{{0.1, 0.2}},
			Bools([]bool{false, true, false, true, true}),
		},
		{
			FloatsList([][]float64{{0.1, 0.2}, {2, 4.2}, {1, 0}, {5, 0.25}, {9, 8.1}}),
			Greater,
			[][]float64{{0.2, 0.1}, {1, 2.2}, {1, 0}, {4, 0.16}, {9.9, 8.1}},
			Bools([]bool{false, true, false, true, false}),
		},
		{
			FloatsList([][]float64{{0.1, 0.2}, {2, 4.2}, {1, 0}, {5, 0.25}, {9, 8.1}, {3.14}, {0.01, 0.02}}),
			Greater,
			[][]float64{{0.2, 0.1}, {1, 2.2}, {1, 0}, {4, 0.16}, {9.9, 8.1}, {1.23, 4.56}, {0.003}},
			Bools([]bool{false, true, false, true, false, false, true}),
		},
		{
			BoolsList([][]bool{{true}, {false}}),
			Greater,
			[]bool{true},
			Bools([]bool{false, false}),
		},
		{
			BoolsList([][]bool{{true}, {false}}),
			Greater,
			[][]bool{{true}},
			Bools([]bool{false, false}),
		},
		{
			BoolsList([][]bool{{true}, {true}, {false}, {false}}),
			Greater,
			[][]bool{{true}, {false}, {true}, {false}},
			Bools([]bool{false, true, false, false}),
		},
		{
			BoolsList([][]bool{{true, true}, {true, false}, {false, true}, {false, false}}),
			Greater,
			[][]bool{{false, false}, {false}, {false, false}, {false, false}},
			Bools([]bool{true, true, false, false}),
		},
		{
			BoolsList([][]bool{{true, true}, {true, false}, {false, false}}),
			Greater,
			[]bool{false, false},
			Bools([]bool{true, false, false}),
		},
		{
			BoolsList([][]bool{{true, true}, {true, false}, {false, false}, {false}}),
			Greater,
			[][]bool{{false, false}},
			Bools([]bool{true, false, false, false}),
		},
		{
			Strings([]string{"A", "B", "C", "B", "D", "BADA"}),
			GreaterEq,
			"B",
			Bools([]bool{false, true, true, true, true, true}),
		},
		{
			Strings([]string{"A", "B", "C", "B", "D", "BADA"}),
			GreaterEq,
			[]string{"B", "B", "C", "D", "A", "A"},
			Bools([]bool{false, true, true, false, true, true}),
		},
		{
			Ints([]int{0, 2, 1, 5, 9}),
			GreaterEq,
			"2",
			Bools([]bool{false, true, false, true, true}),
		},
		{
			Ints([]int{0, 2, 1, 5, 9}),
			GreaterEq,
			[]int{0, 2, 0, 5, 10},
			Bools([]bool{true, true, true, true, false}),
		},
		{
			Floats([]float64{0.1, 2, 1, 5, 9}),
			GreaterEq,
			"2",
			Bools([]bool{false, true, false, true, true}),
		},
		{
			Floats([]float64{0.1, 2, 1, 5, 9}),
			GreaterEq,
			[]float64{0.1, 2, 0, 5, 10},
			Bools([]bool{true, true, true, true, false}),
		},
		{
			Bools([]bool{true, true, false}),
			GreaterEq,
			"true",
			Bools([]bool{true, true, false}),
		},
		{
			Bools([]bool{true, true, false}),
			GreaterEq,
			[]bool{true, false, false},
			Bools([]bool{true, true, true}),
		},
		{
			StringsList([][]string{{"A"}, {"B"}, {"C"}, {"B"}, {"D"}, {"BADA"}}),
			GreaterEq,
			[]string{"B"},
			Bools([]bool{false, true, true, true, true, true}),
		},
		{
			StringsList([][]string{{"A"}, {"B"}, {"C"}, {"B"}, {"D"}, {"BADA"}}),
			GreaterEq,
			[][]string{{"B"}},
			Bools([]bool{false, true, true, true, true, true}),
		},
		{
			StringsList([][]string{{"A"}, {"B"}, {"C"}, {"B"}, {"D"}, {"BADA"}}),
			GreaterEq,
			[][]string{{"0"}, {"B"}, {"B"}, {"B"}, {"D"}, {"BACA"}},
			Bools([]bool{true, true, true, true, true, true}),
		},
		{
			StringsList([][]string{{"A", "A"}, {"B", "B"}, {"C", "C"}, {"B"}, {"D"}, {"BADA"}, {"BADA", "BACA"}}),
			GreaterEq,
			[]string{"B", "B"},
			Bools([]bool{false, true, true, false, false, false, true}),
		},
		{
			StringsList([][]string{{"A", "A"}, {"B", "B"}, {"C", "C"}, {"B"}, {"D"}, {"BADA"}, {"BADA", "BACA"}}),
			GreaterEq,
			[][]string{{"B", "B"}},
			Bools([]bool{false, true, true, false, false, false, true}),
		},
		{
			StringsList([][]string{{"A", "A"}, {"B", "B"}, {"C", "C"}, {"B"}, {"D"}, {"BADA"}, {"BADA", "BACA"}, {"A", "B", "C"}}),
			GreaterEq,
			[][]string{{"0", "0"}, {"B", "B"}, {"B", "B"}, {"B"}, {"D"}, {"AADA"}, {"AADA", "AACA"}, {"A", "B"}},
			Bools([]bool{true, true, true, true, true, true, true, true}),
		},
		{
			IntsList([][]int{{0}, {2}, {1}, {5}, {9}}),
			GreaterEq,
			[]int{2},
			Bools([]bool{false, true, false, true, true}),
		},
		{
			IntsList([][]int{{0}, {2}, {1}, {5}, {9}}),
			GreaterEq,
			[][]int{{2}},
			Bools([]bool{false, true, false, true, true}),
		},
		{
			IntsList([][]int{{0}, {2}, {1}, {5}, {9}}),
			GreaterEq,
			[][]int{{0}, {2}, {0}, {5}, {10}},
			Bools([]bool{true, true, true, true, false}),
		},
		{
			IntsList([][]int{{1, 0}, {2}, {1}, {5}, {9}}),
			GreaterEq,
			[][]int{{0, 0}, {2, 1}, {0}, {5}, {10}},
			Bools([]bool{true, false, true, true, false}),
		},
		{
			IntsList([][]int{{0}, {2}, {1}, {5}, {9}}),
			GreaterEq,
			[][]int{{0, 1}, {2}, {0}, {5}, {10}},
			Bools([]bool{false, true, true, true, false}),
		},
		{
			IntsList([][]int{{1, 0}, {2}, {1}, {5}, {9}}),
			GreaterEq,
			[][]int{{0, 0}, {2}, {0}, {5}, {10}},
			Bools([]bool{true, true, true, true, false}),
		},
		{
			IntsList([][]int{{1, 1}, {2}, {1}, {5}, {9}}),
			GreaterEq,
			[][]int{{0, 0}, {2}, {0}, {5}, {10}},
			Bools([]bool{true, true, true, true, false}),
		},
		{
			IntsList([][]int{{1, 1}, {2}, {1}, {5}, {9}, {10, 100, 1000}}),
			GreaterEq,
			[][]int{{0, 0}, {2}, {0}, {5}, {10}, {10}},
			Bools([]bool{true, true, true, true, false, true}),
		},
		{
			FloatsList([][]float64{{0.1, 0.2}, {2, 4.2}, {1, 0}, {5, 0.25}, {9, 8.1}}),
			GreaterEq,
			[]float64{0.1, 0.2},
			Bools([]bool{true, true, false, true, true}),
		},
		{
			FloatsList([][]float64{{0.1, 0.2}, {2, 4.2}, {1, 0}, {5, 0.25}, {9, 8.1}}),
			GreaterEq,
			[][]float64{{0.1, 0.2}},
			Bools([]bool{true, true, false, true, true}),
		},
		{
			FloatsList([][]float64{{0.1, 0.2}, {2, 4.2}, {1, 0}, {5, 0.25}, {9, 8.1}, {3.14}, {0.01, 0.02}}),
			GreaterEq,
			[][]float64{{0.2, 0.1}, {1, 2.2}, {1, 0}, {4, 0.16}, {9.9, 8.1}, {1.23, 4.56}, {0.003}},
			Bools([]bool{false, true, true, true, false, false, true}),
		},
		{
			BoolsList([][]bool{{true}, {false}}),
			GreaterEq,
			[]bool{true},
			Bools([]bool{true, false}),
		},
		{
			BoolsList([][]bool{{true}, {false}}),
			GreaterEq,
			[][]bool{{true}},
			Bools([]bool{true, false}),
		},
		{
			BoolsList([][]bool{{true}, {true}, {false}, {false}}),
			GreaterEq,
			[][]bool{{true}, {false}, {true}, {false}},
			Bools([]bool{true, true, false, true}),
		},
		{
			BoolsList([][]bool{{true, true}, {true, false}, {false, true}, {false, false}}),
			GreaterEq,
			[][]bool{{false, false}, {false}, {false, false}, {false, false}},
			Bools([]bool{true, true, true, true}),
		},
		{
			BoolsList([][]bool{{true, true}, {true, false}, {false, false}}),
			GreaterEq,
			[]bool{false, false},
			Bools([]bool{true, true, true}),
		},
		{
			BoolsList([][]bool{{true, true}, {true, false}, {false, false}, {false}}),
			GreaterEq,
			[][]bool{{false, false}},
			Bools([]bool{true, true, true, false}),
		},
		{
			Strings([]string{"A", "B", "C", "B", "D", "BADA"}),
			Less,
			"B",
			Bools([]bool{true, false, false, false, false, false}),
		},
		{
			Strings([]string{"A", "B", "C", "B", "D", "BADA"}),
			Less,
			[]string{"B", "B", "C", "D", "A", "A"},
			Bools([]bool{true, false, false, true, false, false}),
		},
		{
			Ints([]int{0, 2, 1, 5, 9}),
			Less,
			"2",
			Bools([]bool{true, false, true, false, false}),
		},
		{
			Ints([]int{0, 2, 1, 5, 9}),
			Less,
			[]int{0, 2, 0, 5, 10},
			Bools([]bool{false, false, false, false, true}),
		},
		{
			Floats([]float64{0.1, 2, 1, 5, 9}),
			Less,
			"2",
			Bools([]bool{true, false, true, false, false}),
		},
		{
			Floats([]float64{0.1, 2, 1, 5, 9}),
			Less,
			[]float64{0.1, 2, 0, 5, 10},
			Bools([]bool{false, false, false, false, true}),
		},
		{
			Bools([]bool{true, true, false}),
			Less,
			"true",
			Bools([]bool{false, false, true}),
		},
		{
			Bools([]bool{true, true, false}),
			Less,
			[]bool{true, false, false},
			Bools([]bool{false, false, false}),
		},
		{
			StringsList([][]string{{"A"}, {"B"}, {"C"}, {"B"}, {"D"}, {"BADA"}}),
			Less,
			[]string{"B"},
			Bools([]bool{true, false, false, false, false, false}),
		},
		{
			StringsList([][]string{{"A"}, {"B"}, {"C"}, {"B"}, {"D"}, {"BADA"}}),
			Less,
			[][]string{{"B"}},
			Bools([]bool{true, false, false, false, false, false}),
		},
		{
			StringsList([][]string{{"A"}, {"B"}, {"C"}, {"B"}, {"D"}, {"BADA"}}),
			Less,
			[][]string{{"0"}, {"B"}, {"B"}, {"B"}, {"D"}, {"BACA"}},
			Bools([]bool{false, false, false, false, false, false}),
		},
		{
			StringsList([][]string{{"A", "A"}, {"B", "B"}, {"C", "C"}, {"B"}, {"D"}, {"BADA"}, {"BADA", "BACA"}}),
			Less,
			[]string{"B", "B"},
			Bools([]bool{true, false, false, true, true, true, false}),
		},
		{
			StringsList([][]string{{"A", "A"}, {"B", "B"}, {"C", "C"}, {"B"}, {"D"}, {"BADA"}, {"BADA", "BACA"}}),
			Less,
			[][]string{{"B", "B"}},
			Bools([]bool{true, false, false, true, true, true, false}),
		},
		{
			StringsList([][]string{{"A", "A"}, {"B", "B"}, {"C", "C"}, {"B"}, {"D"}, {"BADA"}, {"BADA", "BACA"}, {"A", "B", "C"}}),
			Less,
			[][]string{{"0", "0"}, {"B", "B"}, {"B", "B"}, {"B"}, {"D"}, {"AADA"}, {"AADA", "AACA"}, {"A", "B"}},
			Bools([]bool{false, false, false, false, false, false, false, false}),
		},
		{
			IntsList([][]int{{0}, {2}, {1}, {5}, {9}}),
			Less,
			[]int{2},
			Bools([]bool{true, false, true, false, false}),
		},
		{
			IntsList([][]int{{0}, {2}, {1}, {5}, {9}}),
			Less,
			[][]int{{2}},
			Bools([]bool{true, false, true, false, false}),
		},
		{
			IntsList([][]int{{0}, {2}, {1}, {5}, {9}}),
			Less,
			[][]int{{0}, {2}, {0}, {5}, {10}},
			Bools([]bool{false, false, false, false, true}),
		},
		{
			IntsList([][]int{{1, 0}, {2}, {1}, {5}, {9}}),
			Less,
			[][]int{{2, 1}, {2, 1}, {0}, {5}, {10}},
			Bools([]bool{true, true, false, false, true}),
		},
		{
			IntsList([][]int{{1, 0}, {2}, {1}, {5}, {9}, {10, 100, 1000}}),
			Less,
			[][]int{{2, 1}, {2, 1}, {0}, {5}, {10}, {10}},
			Bools([]bool{true, true, false, false, true, false}),
		},
		{
			FloatsList([][]float64{{0.1, 0.2}, {2, 4.2}, {1, 0}, {0.05, 0.025}, {9, 8.1}}),
			Less,
			[]float64{0.1, 0.2},
			Bools([]bool{false, false, false, true, false}),
		},
		{
			FloatsList([][]float64{{0.1, 0.2}, {2, 4.2}, {1, 0}, {0.05, 0.025}, {9, 8.1}, {3.14}}),
			Less,
			[][]float64{{0.1, 0.2}},
			Bools([]bool{false, false, false, true, false, true}),
		},
		{
			FloatsList([][]float64{{0.1, 0.2}, {2, 4.2}, {1, 0}, {0.05, 0.025}, {9, 8.1}, {3.14}, {0.01}}),
			Less,
			[][]float64{{0.1}},
			Bools([]bool{false, false, false, false, false, false, true}),
		},
		{
			FloatsList([][]float64{{0.1, 0.2}, {2, 4.2}, {1, 0}, {0.05, 0.025}, {9, 8.1}}),
			Less,
			[][]float64{{0.2, 0.3}, {1, 2.2}, {1, 0}, {4, 0.16}, {9.9, 8.1}},
			Bools([]bool{true, false, false, true, false}),
		},
		{
			BoolsList([][]bool{{true}, {false}}),
			Less,
			[]bool{true},
			Bools([]bool{false, true}),
		},
		{
			BoolsList([][]bool{{true}, {false}}),
			Less,
			[][]bool{{true}},
			Bools([]bool{false, true}),
		},
		{
			BoolsList([][]bool{{true}, {true}, {false}, {false}}),
			Less,
			[][]bool{{true}, {false}, {true}, {false}},
			Bools([]bool{false, false, true, false}),
		},
		{
			BoolsList([][]bool{{true, true}, {true, false}, {false, true}, {false, false}, {false, false}}),
			Less,
			[][]bool{{false, false}, {false}, {false, false}, {false, false}, {true, true}},
			Bools([]bool{false, false, false, false, true}),
		},
		{
			BoolsList([][]bool{{true, true}, {true, false}, {false, false}}),
			Less,
			[]bool{false, false},
			Bools([]bool{false, false, false}),
		},
		{
			BoolsList([][]bool{{true, true}, {true, false}, {false, false}}),
			Less,
			[][]bool{{false, false}},
			Bools([]bool{false, false, false}),
		},
		{
			BoolsList([][]bool{{true, true}, {true, false}, {false, false}}),
			Less,
			[]bool{true, true},
			Bools([]bool{false, false, true}),
		},
		{
			BoolsList([][]bool{{true, true}, {true, false}, {false, false}}),
			Less,
			[][]bool{{true, true}},
			Bools([]bool{false, false, true}),
		},
		{
			BoolsList([][]bool{{true, true}, {true, false}, {false, false}, {false}}),
			Less,
			[][]bool{{true, true}},
			Bools([]bool{false, false, true, true}),
		},
		{
			Strings([]string{"A", "B", "C", "B", "D", "BADA"}),
			LessEq,
			"B",
			Bools([]bool{true, true, false, true, false, false}),
		},
		{
			Strings([]string{"A", "B", "C", "B", "D", "BADA"}),
			LessEq,
			[]string{"B", "B", "C", "D", "A", "A"},
			Bools([]bool{true, true, true, true, false, false}),
		},
		{
			Ints([]int{0, 2, 1, 5, 9}),
			LessEq,
			"2",
			Bools([]bool{true, true, true, false, false}),
		},
		{
			Ints([]int{0, 2, 1, 5, 9}),
			LessEq,
			[]int{0, 2, 0, 5, 10},
			Bools([]bool{true, true, false, true, true}),
		},
		{
			Floats([]float64{0.1, 2, 1, 5, 9}),
			LessEq,
			"2",
			Bools([]bool{true, true, true, false, false}),
		},
		{
			Floats([]float64{0.1, 2, 1, 5, 9}),
			LessEq,
			[]float64{0.1, 2, 0, 5, 10},
			Bools([]bool{true, true, false, true, true}),
		},
		{
			Bools([]bool{true, true, false}),
			LessEq,
			"true",
			Bools([]bool{true, true, true}),
		},
		{
			Bools([]bool{true, true, false}),
			LessEq,
			[]bool{true, false, false},
			Bools([]bool{true, false, true}),
		},
		{
			StringsList([][]string{{"A"}, {"B"}, {"C"}, {"B"}, {"D"}, {"BADA"}}),
			LessEq,
			[]string{"B"},
			Bools([]bool{true, true, false, true, false, false}),
		},
		{
			StringsList([][]string{{"A"}, {"B"}, {"C"}, {"B"}, {"D"}, {"BADA"}}),
			LessEq,
			[][]string{{"B"}},
			Bools([]bool{true, true, false, true, false, false}),
		},
		{
			StringsList([][]string{{"A"}, {"B"}, {"C"}, {"B"}, {"D"}, {"BADA"}}),
			LessEq,
			[][]string{{"0"}, {"B"}, {"B"}, {"B"}, {"D"}, {"BACA"}},
			Bools([]bool{false, true, false, true, true, false}),
		},
		{
			StringsList([][]string{{"A", "A"}, {"B", "B"}, {"C", "C"}, {"B"}, {"D"}, {"BADA"}, {"BADA", "BACA"}}),
			LessEq,
			[]string{"B", "B"},
			Bools([]bool{true, true, false, true, true, true, false}),
		},
		{
			StringsList([][]string{{"A", "A"}, {"B", "B"}, {"C", "C"}, {"B"}, {"D"}, {"BADA"}, {"BADA", "BACA"}}),
			LessEq,
			[][]string{{"B", "B"}},
			Bools([]bool{true, true, false, true, true, true, false}),
		},
		{
			StringsList([][]string{{"A", "A"}, {"B", "B"}, {"C", "C"}, {"B"}, {"D"}, {"BADA"}, {"BADA", "BACA"}, {"A", "B", "C"}}),
			LessEq,
			[][]string{{"0", "0"}, {"B", "B"}, {"B", "B"}, {"B"}, {"D"}, {"AADA"}, {"AADA", "AACA"}, {"A", "B"}},
			Bools([]bool{false, true, false, true, true, false, false, false}),
		},
		{
			IntsList([][]int{{0}, {2}, {1}, {5}, {9}}),
			LessEq,
			[]int{2},
			Bools([]bool{true, true, true, false, false}),
		},
		{
			IntsList([][]int{{0}, {2}, {1}, {5}, {9}}),
			LessEq,
			[][]int{{2}},
			Bools([]bool{true, true, true, false, false}),
		},
		{
			IntsList([][]int{{0}, {2}, {1}, {5}, {9}}),
			LessEq,
			[][]int{{0}, {2}, {0}, {5}, {10}},
			Bools([]bool{true, true, false, true, true}),
		},
		{
			IntsList([][]int{{1, 0}, {2}, {1}, {5}, {9}, {10, 100, 1000}}),
			LessEq,
			[][]int{{2, 1}, {2, 1}, {0}, {5}, {10}, {10}},
			Bools([]bool{true, true, false, true, true, false}),
		},
		{
			FloatsList([][]float64{{0.1, 0.2}, {2, 4.2}, {1, 0}, {0.05, 0.025}, {9, 8.1}}),
			LessEq,
			[]float64{0.1, 0.2},
			Bools([]bool{true, false, false, true, false}),
		},
		{
			FloatsList([][]float64{{0.1, 0.2}, {2, 4.2}, {1, 0}, {0.05, 0.025}, {9, 8.1}}),
			LessEq,
			[][]float64{{0.1, 0.2}},
			Bools([]bool{true, false, false, true, false}),
		},
		{
			FloatsList([][]float64{{0.1, 0.2}, {2, 4.2}, {1, 0}, {0.05, 0.025}, {9, 8.1}}),
			LessEq,
			[][]float64{{0.2, 0.3}, {1, 2.2}, {1, 0}, {4, 0.16}, {9.9, 8.1}},
			Bools([]bool{true, false, true, true, true}),
		},
		{
			FloatsList([][]float64{{0.1, 0.2}, {2, 4.2}, {1, 0}, {0.05}, {9, 8.1}}),
			LessEq,
			[][]float64{{0.2, 0.3}, {1, 2.2}, {1, 0}, {4, 0.16}, {9.9}},
			Bools([]bool{true, false, true, true, false}),
		},
		{
			BoolsList([][]bool{{true}, {false}}),
			LessEq,
			[]bool{true},
			Bools([]bool{true, true}),
		},
		{
			BoolsList([][]bool{{true}, {false}}),
			LessEq,
			[][]bool{{true}},
			Bools([]bool{true, true}),
		},
		{
			BoolsList([][]bool{{true}, {true}, {false}, {false}}),
			LessEq,
			[][]bool{{true}, {false}, {true}, {false}},
			Bools([]bool{true, false, true, true}),
		},
		{
			BoolsList([][]bool{{true, true}, {true, false}, {false, true}, {false, false}, {false, false}}),
			LessEq,
			[][]bool{{false, false}, {false}, {false, false}, {false, false}, {true, true}},
			Bools([]bool{false, false, false, true, true}),
		},
		{
			BoolsList([][]bool{{true, true}, {true, false}, {false, false}}),
			LessEq,
			[]bool{false, false},
			Bools([]bool{false, false, true}),
		},
		{
			BoolsList([][]bool{{true, true}, {true, false}, {false, false}}),
			LessEq,
			[][]bool{{false, false}},
			Bools([]bool{false, false, true}),
		},
		{
			BoolsList([][]bool{{true, true}, {true, false}, {false, false}}),
			LessEq,
			[]bool{true, true},
			Bools([]bool{true, true, true}),
		},
		{
			BoolsList([][]bool{{true, true}, {true, false}, {false, false}, {false}}),
			LessEq,
			[][]bool{{true, true}},
			Bools([]bool{true, true, true, true}),
		},
		{
			Strings([]string{"A", "B", "C", "B", "D", "BADA"}),
			In,
			"B",
			Bools([]bool{false, true, false, true, false, false}),
		},
		{
			Strings([]string{"Hello", "world", "this", "is", "a", "test"}),
			In,
			[]string{"cat", "world", "hello", "a"},
			Bools([]bool{false, true, false, false, true, false}),
		},
		{
			Ints([]int{0, 2, 1, 5, 9}),
			In,
			"2",
			Bools([]bool{false, true, false, false, false}),
		},
		{
			Ints([]int{0, 2, 1, 5, 9}),
			In,
			[]int{2, 99, 1234, 9},
			Bools([]bool{false, true, false, false, true}),
		},
		{
			Floats([]float64{0.1, 2, 1, 5, 9}),
			In,
			"2",
			Bools([]bool{false, true, false, false, false}),
		},
		{
			Floats([]float64{0.1, 2, 1, 5, 9}),
			In,
			[]float64{2, 99, 1234, 9},
			Bools([]bool{false, true, false, false, true}),
		},
		{
			Bools([]bool{true, true, false}),
			In,
			"true",
			Bools([]bool{true, true, false}),
		},
		{
			Bools([]bool{true, true, false}),
			In,
			[]bool{false, false, false},
			Bools([]bool{false, false, true}),
		},
		{
			StringsList([][]string{{"A"}, {"B"}, {"C"}, {"B"}, {"D"}, {"BADA"}}),
			In,
			[]string{"B"},
			Bools([]bool{false, true, false, true, false, false}),
		},
		{
			StringsList([][]string{{"A"}, {"B"}, {"C"}, {"B"}, {"D"}, {"BADA"}}),
			In,
			[][]string{{"B"}},
			Bools([]bool{false, true, false, true, false, false}),
		},
		{
			StringsList([][]string{{"A"}, {"B"}, {"C"}, {"B", "C"}, {"D"}, {"BADA"}}),
			In,
			[]string{"B", "C"},
			Bools([]bool{false, false, false, true, false, false}),
		},
		{
			StringsList([][]string{{"A"}, {"B"}, {"C"}, {"B", "C"}, {"D"}, {"BADA"}}),
			In,
			[][]string{{"B", "C"}},
			Bools([]bool{false, false, false, true, false, false}),
		},
		{
			StringsList([][]string{{"A"}, {"B"}, {"C"}, {"B", "C"}, {"D"}, {"BADA"}}),
			In,
			[][]string{{"A"}, {"B"}, {"B", "C"}},
			Bools([]bool{true, true, false, true, false, false}),
		},
		{
			IntsList([][]int{{1}, {2}, {2}, {0, 1}, {2, 3}, {1, 2}, {5, 6}, {9, 10}}),
			In,
			[]int{2},
			Bools([]bool{false, true, true, false, false, false, false, false}),
		},
		{
			IntsList([][]int{{1}, {2}, {2}, {0, 1}, {2, 3}, {1, 2}, {5, 6}, {9, 10}}),
			In,
			[][]int{{2}},
			Bools([]bool{false, true, true, false, false, false, false, false}),
		},
		{
			IntsList([][]int{{1}, {2}, {2}, {0, 1}, {2, 3}, {1, 2}, {5, 6}, {9, 10}}),
			In,
			[]int{2, 3},
			Bools([]bool{false, false, false, false, true, false, false, false}),
		},
		{
			IntsList([][]int{{1}, {2}, {2}, {0, 1}, {2, 3}, {1, 2}, {5, 6}, {9, 10}}),
			In,
			[][]int{{2, 3}},
			Bools([]bool{false, false, false, false, true, false, false, false}),
		},
		{
			IntsList([][]int{{1}, {2}, {2}, {0, 1}, {2, 3}, {1, 2}, {5, 6}, {9, 10}}),
			In,
			[][]int{{2}, {2, 3}},
			Bools([]bool{false, true, true, false, true, false, false, false}),
		},
		{
			FloatsList([][]float64{{0.1}, {0.2}, {0.2}, {0.1, 0.2}, {2, 4.2}, {1, 0}, {5, 0.25}, {9, 8.1}}),
			In,
			[]float64{0.2},
			Bools([]bool{false, true, true, false, false, false, false, false}),
		},
		{
			FloatsList([][]float64{{0.1}, {0.2}, {0.2}, {0.1, 0.2}, {2, 4.2}, {1, 0}, {5, 0.25}, {9, 8.1}}),
			In,
			[][]float64{{0.2}},
			Bools([]bool{false, true, true, false, false, false, false, false}),
		},
		{
			FloatsList([][]float64{{0.1}, {0.2}, {0.2}, {0.1, 0.2}, {2, 4.2}, {1, 0}, {5, 0.25}, {9, 8.1}}),
			In,
			[]float64{0.1, 0.2},
			Bools([]bool{false, false, false, true, false, false, false, false}),
		},
		{
			FloatsList([][]float64{{0.1}, {0.2}, {0.2}, {0.1, 0.2}, {2, 4.2}, {1, 0}, {5, 0.25}, {9, 8.1}}),
			In,
			[][]float64{{0.1, 0.2}},
			Bools([]bool{false, false, false, true, false, false, false, false}),
		},
		{
			FloatsList([][]float64{{0.1}, {0.2}, {0.2}, {0.1, 0.2}, {2, 4.2}, {1, 0}, {5, 0.25}, {9, 8.1}}),
			In,
			[][]float64{{0.2}, {0.1, 0.2}},
			Bools([]bool{false, true, true, true, false, false, false, false}),
		},
		{
			BoolsList([][]bool{{true}, {false}, {false}, {true, true}, {true, false}, {false, false}}),
			In,
			[]bool{false},
			Bools([]bool{false, true, true, false, false, false}),
		},
		{
			BoolsList([][]bool{{true}, {false}, {false}, {true, true}, {true, false}, {false, false}}),
			In,
			[][]bool{{false}},
			Bools([]bool{false, true, true, false, false, false}),
		},
		{
			BoolsList([][]bool{{true}, {false}, {false}, {true, true}, {true, false}, {false, false}}),
			In,
			[]bool{false, false},
			Bools([]bool{false, false, false, false, false, true}),
		},
		{
			BoolsList([][]bool{{true}, {false}, {false}, {true, true}, {true, false}, {false, false}}),
			In,
			[][]bool{{false, false}},
			Bools([]bool{false, false, false, false, false, true}),
		},
		{
			BoolsList([][]bool{{true}, {false}, {false}, {true, true}, {true, false}, {false, false}}),
			In,
			[][]bool{{false}, {false, false}},
			Bools([]bool{false, true, true, false, false, true}),
		},
	}
	for testnum, test := range table {
		testid := fmt.Sprintf("\nIndex: %v\nSeries: %v (%v)\nComparator: %v", testnum, test.series, test.series.t, test.comparator)

		a := test.series
		b := a.Compare(test.comparator, test.comparando)
		if err := b.Err; err != nil {
			t.Errorf("Test:%v\nError:%v", testid, err)
		}
		expected := test.expected.Records()
		received := b.Records()
		if !reflect.DeepEqual(expected, received) {
			t.Errorf(
				"Test:%v\nExpected:\n%v\nReceived:\n%v",
				testid, expected, received,
			)
		}
		if err := checkTypes(b); err != nil {
			t.Errorf(
				"Test:%v\nError:%v",
				testid, err,
			)
		}
		//if err := checkAddr(a.Addr(), b.Addr()); err != nil {
		//t.Errorf("Test:%v\nError:%v\nA:%v\nB:%v", testnum, err, a.Addr(), b.Addr())
		//}
	}
}

func TestSeries_Compare_CompFunc(t *testing.T) {
	table := []struct {
		series     Series
		comparator Comparator
		comparando interface{}
		expected   Series
		panic      bool
	}{
		{
			Strings([]string{"A", "B", "C", "B", "D", "BADA"}),
			CompFunc,
			func(el Element) bool {
				if el.Type() == String {
					if val, ok := el.Val().(string); ok {
						return strings.HasPrefix(val, "B")
					}
					return false
				}
				return false
			},
			Bools([]bool{false, true, false, true, false, true}),
			false,
		},
		{
			Strings([]string{"A", "B", "C", "B", "D", "BADA"}),
			CompFunc,
			func(el Element) {},
			Bools([]bool{false, false, false, false, false}),
			true,
		},
	}
	for testnum, test := range table {
		func() {
			defer func() {
				if r := recover(); r != nil {
					// recovered
					if !test.panic {
						t.Errorf("did not expected panic but was '%v'", r)
					}
				} else {
					// nothing to recover from
					if test.panic {
						t.Errorf("exptected panic but did not panic")
					}
				}
			}()

			a := test.series
			b := a.Compare(test.comparator, test.comparando)
			if err := b.Err; err != nil {
				t.Errorf("Test:%v\nError:%v", testnum, err)
			}
			expected := test.expected.Records()
			received := b.Records()
			if !reflect.DeepEqual(expected, received) {
				t.Errorf(
					"Test:%v\nExpected:\n%v\nReceived:\n%v",
					testnum, expected, received,
				)
			}
			if err := checkTypes(b); err != nil {
				t.Errorf(
					"Test:%v\nError:%v",
					testnum, err,
				)
			}
		}()
	}
}

func TestSeries_Subset(t *testing.T) {
	table := []struct {
		series   Series
		indexes  Indexes
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

		{
			StringsList([]string{"A", "B", "C", "K", "D"}),
			[]int{0},
			"[[A B C K D]]",
		},
		{
			StringsList([][]string{{"A"}, {"B"}, {"C"}, {"K"}, {"D"}}),
			[]int{2, 1, 4, 4, 0, 3},
			"[[C] [B] [D] [D] [A] [K]]",
		},
		{
			StringsList([][]string{{"A"}, {"B"}, {"C"}, {"K"}, {"D"}}),
			[]int{1},
			"[[B]]",
		},

		{
			StringsList([][]string{{"A"}, {"B"}, {"C"}, {"K"}, {"D"}}),
			[]bool{true, false, false, true, true},
			"[[A] [K] [D]]",
		},
		{
			StringsList([][]string{{"A"}, {"B"}, {"C"}, {"K"}, {"D"}}),
			Ints([]int{3, 2, 1, 0}),
			"[[K] [C] [B] [A]]",
		},
		{
			StringsList([][]string{{"A"}, {"B"}, {"C"}, {"K"}, {"D"}}),
			Ints([]int{1}),
			"[[B]]",
		},
		{
			StringsList([][]string{{"A"}, {"B"}, {"C"}, {"K"}, {"D"}}),
			Ints(2),
			"[[C]]",
		},
		{
			StringsList([][]string{{"A"}, {"B"}, {"C"}, {"K"}, {"D"}}),
			Bools([]bool{true, false, false, true, true}),
			"[[A] [K] [D]]",
		},
	}
	for testnum, test := range table {
		a := test.series
		b := a.Subset(test.indexes)
		if err := b.Err; err != nil {
			t.Errorf("Test:%v\nError:%v", testnum, err)
		}
		expected := test.expected
		received := fmt.Sprint(b)
		if expected != received {
			t.Errorf(
				"Test:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, expected, received,
			)
		}
		if err := checkTypes(b); err != nil {
			t.Errorf(
				"Test:%v\nError:%v",
				testnum, err,
			)
		}
		//if err := checkAddr(a.Addr(), b.Addr()); err != nil {
		//t.Errorf("Test:%v\nError:%v\nA:%v\nB:%v", testnum, err, a.Addr(), b.Addr())
		//}
	}
}

func TestSeries_Set(t *testing.T) {
	table := []struct {
		series   Series
		indexes  Indexes
		values   Series
		expected string
	}{
		{
			Strings([]string{"A", "B", "C", "K", "D"}),
			[]int{1, 2, 4},
			Ints([]string{"1", "2", "3"}),
			"[A 1 2 K 3]",
		},
		{
			Strings([]string{"A", "B", "C", "K", "D"}),
			[]bool{false, true, true, false, true},
			Ints([]string{"1", "2", "3"}),
			"[A 1 2 K 3]",
		},
		{
			Strings([]string{"A", "B", "C", "K", "D"}),
			Ints([]int{1, 2, 4}),
			Ints([]string{"1", "2", "3"}),
			"[A 1 2 K 3]",
		},
		{
			Strings([]string{"A", "B", "C", "K", "D"}),
			Bools([]bool{false, true, true, false, true}),
			Ints([]string{"1", "2", "3"}),
			"[A 1 2 K 3]",
		},

		{
			StringsList([][]string{{"A"}, {"B"}, {"C"}, {"K"}, {"D"}}),
			[]int{1, 2, 4},
			Ints([]string{"1", "2", "3"}),
			"[[A] [1] [2] [K] [3]]",
		},
		{
			StringsList([][]string{{"A"}, {"B"}, {"C"}, {"K"}, {"D"}}),
			[]bool{false, true, true, false, true},
			Ints([]string{"1", "2", "3"}),
			"[[A] [1] [2] [K] [3]]",
		},
		{
			StringsList([][]string{{"A"}, {"B"}, {"C"}, {"K"}, {"D"}}),
			Ints([]int{1, 2, 4}),
			Ints([]string{"1", "2", "3"}),
			"[[A] [1] [2] [K] [3]]",
		},
		{
			StringsList([][]string{{"A"}, {"B"}, {"C"}, {"K"}, {"D"}}),
			Bools([]bool{false, true, true, false, true}),
			Ints([]string{"1", "2", "3"}),
			"[[A] [1] [2] [K] [3]]",
		},

		{
			StringsList([][]string{{"A"}, {"B"}, {"C"}, {"K"}, {"D"}}),
			[]int{1, 2, 4},
			IntsList([][]string{{"1", "10"}, {"2", "20"}, {"3", "30"}}),
			"[[A] [1 10] [2 20] [K] [3 30]]",
		},
		{
			StringsList([][]string{{"A"}, {"B"}, {"C"}, {"K"}, {"D"}}),
			[]bool{false, true, true, false, true},
			IntsList([][]string{{"1", "10"}, {"2", "20"}, {"3", "30"}}),
			"[[A] [1 10] [2 20] [K] [3 30]]",
		},
		{
			StringsList([][]string{{"A"}, {"B"}, {"C"}, {"K"}, {"D"}}),
			Ints([]int{1, 2, 4}),
			IntsList([][]string{{"1", "10"}, {"2", "20"}, {"3", "30"}}),
			"[[A] [1 10] [2 20] [K] [3 30]]",
		},
		{
			StringsList([][]string{{"A"}, {"B"}, {"C"}, {"K"}, {"D"}}),
			Bools([]bool{false, true, true, false, true}),
			IntsList([][]string{{"1", "10"}, {"2", "20"}, {"3", "30"}}),
			"[[A] [1 10] [2 20] [K] [3 30]]",
		},
	}
	for testnum, test := range table {
		b := test.series.Set(test.indexes, test.values)
		if err := b.Err; err != nil {
			t.Errorf("Test:%v\nError:%v", testnum, err)
		}
		expected := test.expected
		received := fmt.Sprint(b)
		if expected != received {
			t.Errorf(
				"Test:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, expected, received,
			)
		}
		if err := checkTypes(b); err != nil {
			t.Errorf(
				"Test:%v\nError:%v",
				testnum, err,
			)
		}
		//if err := checkAddr(test.values.Addr(), b.Addr()); err != nil {
		//t.Errorf("Test:%v\nError:%v\nNV:%v\nB:%v", testnum, err, test.values.Addr(), b.Addr())
		//}
	}
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
			Strings([]int{}),
			"[]",
		},
		{
			Strings(nil),
			"[NaN]",
		},
		{
			Strings(Strings([]string{"A", "B", "C"})),
			"[A B C]",
		},
		{
			Strings([]interface{}{"A", "B", "C", "D", 1, 2, 3}),
			"[A B C D 1 2 3]",
		},
		{
			Strings([]interface{}{"A", "B", "C", "D", nil, 1, 2, 3, nil, nil}),
			"[A B C D NaN 1 2 3 NaN NaN]",
		},
	}
	for testnum, test := range table {
		if err := test.series.Err; err != nil {
			t.Errorf("Test:%v\nError:%v", testnum, err)
		}
		expected := test.expected
		received := fmt.Sprint(test.series)
		if expected != received {
			t.Errorf(
				"Test:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, expected, received,
			)
		}
		if err := checkTypes(test.series); err != nil {
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
			Ints([]int{}),
			"[]",
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
		{
			Ints([]interface{}{"A", "B", "1", "2"}),
			"[NaN NaN 1 2]",
		},
		{
			Ints([]interface{}{1, 2, 3}),
			"[1 2 3]",
		},
		{
			Ints([]interface{}{"A", "B", nil, "1", "2", nil}),
			"[NaN NaN NaN 1 2 NaN]",
		},
	}
	for testnum, test := range table {
		if err := test.series.Err; err != nil {
			t.Errorf("Test:%v\nError:%v", testnum, err)
		}
		expected := test.expected
		received := fmt.Sprint(test.series)
		if expected != received {
			t.Errorf(
				"Test:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, expected, received,
			)
		}
		if err := checkTypes(test.series); err != nil {
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
			Floats([]int{}),
			"[]",
		},
		{
			Floats(nil),
			"[NaN]",
		},
		{
			Floats(Strings([]string{"1", "2", "3"})),
			"[1.000000 2.000000 3.000000]",
		},
		{
			Floats([]interface{}{"A", "B", "1", "2"}),
			"[NaN NaN 1.000000 2.000000]",
		},
		{
			Floats([]interface{}{1.1, 2, 3}),
			"[1.100000 2.000000 3.000000]",
		},
		{
			Floats([]interface{}{"A", "B", nil, "1", "2", "3.14", nil}),
			"[NaN NaN NaN 1.000000 2.000000 3.140000 NaN]",
		},
	}
	for testnum, test := range table {
		if err := test.series.Err; err != nil {
			t.Errorf("Test:%v\nError:%v", testnum, err)
		}
		expected := test.expected
		received := fmt.Sprint(test.series)
		if expected != received {
			t.Errorf(
				"Test:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, expected, received,
			)
		}
		if err := checkTypes(test.series); err != nil {
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
			Bools([]int{}),
			"[]",
		},
		{
			Bools(nil),
			"[NaN]",
		},
		{
			Bools(Strings([]string{"1", "0", "1"})),
			"[true false true]",
		},
		{
			Bools([]interface{}{"A", "true", "1", "f"}),
			"[NaN true true false]",
		},
		{
			Bools([]interface{}{true, true, false}),
			"[true true false]",
		},
		{
			Bools([]interface{}{"A", nil, "true", "1", "f", nil}),
			"[NaN NaN true true false NaN]",
		},
	}
	for testnum, test := range table {
		if err := test.series.Err; err != nil {
			t.Errorf("Test:%v\nError:%v", testnum, err)
		}
		expected := test.expected
		received := fmt.Sprint(test.series)
		if expected != received {
			t.Errorf(
				"Test:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, expected, received,
			)
		}
		if err := checkTypes(test.series); err != nil {
			t.Errorf("Test:%v\nError:%v", testnum, err)
		}
	}
}

func TestStringsList(t *testing.T) {
	table := []struct {
		series   Series
		expected string
	}{
		// Initialization using 1-D slice or single value.
		{
			StringsList([]string{"A", "B", "C", "D"}),
			"[[A B C D]]",
		},
		{
			StringsList([]string{"A", "B", "C", "D", "NaN"}),
			"[[NaN]]",
		},
		{
			StringsList([]string{"A"}),
			"[[A]]",
		},
		{
			StringsList([]string{"NaN"}),
			"[[NaN]]",
		},
		{
			StringsList("A"),
			"[[A]]",
		},
		{
			StringsList("NaN"),
			"[[NaN]]",
		},
		{
			StringsList([]int{1, 2, 3}),
			"[[1 2 3]]",
		},
		{
			StringsList([]int{2}),
			"[[2]]",
		},
		{
			StringsList([]int32{2}),
			"[[2]]",
		},
		{
			StringsList([]int64{2}),
			"[[2]]",
		},
		{
			StringsList(-1),
			"[[-1]]",
		},
		{
			StringsList([]float64{1, 2, 3}),
			"[[1.000000 2.000000 3.000000]]",
		},
		{
			StringsList([]float64{2}),
			"[[2.000000]]",
		},
		{
			StringsList([]float32{2}),
			"[[2.000000]]",
		},
		{
			StringsList(-1.0),
			"[[-1.000000]]",
		},
		{
			StringsList(math.NaN()),
			"[[NaN]]",
		},
		{
			StringsList(math.Inf(1)),
			"[[+Inf]]",
		},
		{
			StringsList(math.Inf(-1)),
			"[[-Inf]]",
		},
		{
			StringsList([]bool{true, true, false}),
			"[[true true false]]",
		},
		{
			StringsList([]bool{false}),
			"[[false]]",
		},
		{
			StringsList(true),
			"[[true]]",
		},
		{
			StringsList(false),
			"[[false]]",
		},
		{
			StringsList([]string{}),
			"[]",
		},
		{
			StringsList([]int{}),
			"[]",
		},
		{
			StringsList(nil),
			"[[NaN]]",
		},
		{
			StringsList(StringsList([]string{"A", "B", "C"})),
			"[[A B C]]",
		},

		// Initialization using 1-D slice of interface.
		{
			StringsList([]interface{}{"A", "B", "C", "D"}),
			"[[A] [B] [C] [D]]",
		},
		{
			StringsList([]interface{}{"A", "B", "C", "D", 1, 2, 3}),
			"[[A] [B] [C] [D] [1] [2] [3]]",
		},
		{
			StringsList([]interface{}{"A", "B", "C", "D", 1, 2, 3, nil}),
			"[[A] [B] [C] [D] [1] [2] [3] [NaN]]",
		},
		{
			StringsList([]interface{}{[]string{"A", "B"}, []string{"C", "D"}}),
			"[[A B] [C D]]",
		},
		{
			StringsList([]interface{}{nil}),
			"[[NaN]]",
		},
		{
			StringsList([]interface{}{}),
			"[]",
		},
		{
			StringsList(StringsList([]interface{}{"A", "B", "C"})),
			"[[A] [B] [C]]",
		},

		// Initialization using 2-D slice.
		{
			StringsList([][]string{{"A", "B"}, {"C", "D"}}),
			"[[A B] [C D]]",
		},
		{
			StringsList([][]string{{"A"}}),
			"[[A]]",
		},
		{
			StringsList([][]int{{1}, {2, 3}}),
			"[[1] [2 3]]",
		},
		{
			StringsList([][]int{{2}}),
			"[[2]]",
		},
		{
			StringsList([][]int32{{2}}),
			"[[2]]",
		},
		{
			StringsList([][]int64{{2}}),
			"[[2]]",
		},
		{
			StringsList([][]float64{{1}, {2, 3}}),
			"[[1.000000] [2.000000 3.000000]]",
		},
		{
			StringsList([][]float64{{2}}),
			"[[2.000000]]",
		},
		{
			StringsList([][]float32{{2}}),
			"[[2.000000]]",
		},
		{
			StringsList([][]bool{{true}, {true, false}}),
			"[[true] [true false]]",
		},
		{
			StringsList([][]bool{{false}}),
			"[[false]]",
		},
		{
			StringsList([][]string{}),
			"[]",
		},
		{
			StringsList([][]int{}),
			"[]",
		},
		{
			StringsList(StringsList([][]string{{"A"}, {"B", "C"}})),
			"[[A] [B C]]",
		},
		{
			StringsList(IntsList([][]int{{1}, {2, 3}})),
			"[[1] [2 3]]",
		},
		{
			StringsList(FloatsList([][]float64{{1}, {2, 3}})),
			"[[1.000000] [2.000000 3.000000]]",
		},
		{
			StringsList(BoolsList([][]bool{{true}, {true, false}})),
			"[[true] [true false]]",
		},

		// Initialization using 2-D slice of interface.
		{
			StringsList([][]interface{}{{"A", "B"}, {"C", "D"}}),
			"[[A B] [C D]]",
		},
		{
			StringsList([][]interface{}{{"A", "B"}, {"C", "D"}, {1, 2}}),
			"[[A B] [C D] [1 2]]",
		},
		{
			StringsList([][]interface{}{{"A", "B"}, {"C", "D"}, {1, 2}, nil, {4}}),
			"[[A B] [C D] [1 2] [NaN] [4]]",
		},
		{
			StringsList(StringsList([][]interface{}{{"A"}, {"B", "C"}})),
			"[[A] [B C]]",
		},
		{
			StringsList(StringsList([][]interface{}{{"A"}, {"B", "C"}, {1}, {2, 3}})),
			"[[A] [B C] [1] [2 3]]",
		},
		{
			StringsList(StringsList([][]interface{}{{"A"}, {"B", "C"}, {1}, {2, 3}, nil, {4}})),
			"[[A] [B C] [1] [2 3] [NaN] [4]]",
		},
	}
	for testnum, test := range table {
		if err := test.series.Err; err != nil {
			t.Errorf("Test:%v\nError:%v", testnum, err)
		}
		expected := test.expected
		received := fmt.Sprint(test.series)
		if expected != received {
			t.Errorf(
				"Test:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, expected, received,
			)
		}
		if err := checkTypes(test.series); err != nil {
			t.Errorf("Test:%v\nError:%v", testnum, err)
		}
	}
}

func TestIntsList(t *testing.T) {
	table := []struct {
		series   Series
		expected string
	}{
		// Initialization using 1-D slice or single value.
		{
			IntsList([]string{"A", "B", "1", "2"}),
			"[[NaN]]",
		},
		{
			IntsList([]string{"1"}),
			"[[1]]",
		},
		{
			IntsList([]string{"NaN"}),
			"[[NaN]]",
		},
		{
			IntsList("2"),
			"[[2]]",
		},
		{
			IntsList("NaN"),
			"[[NaN]]",
		},
		{
			IntsList("A"),
			"[[NaN]]",
		},
		{
			IntsList([]int{1, 2, 3}),
			"[[1 2 3]]",
		},
		{
			IntsList([]int{2}),
			"[[2]]",
		},
		{
			IntsList(-1),
			"[[-1]]",
		},
		{
			IntsList(int32(-1)),
			"[[-1]]",
		},
		{
			IntsList(int64(-1)),
			"[[-1]]",
		},
		{
			IntsList([]float64{1, 2, 3}),
			"[[1 2 3]]",
		},
		{
			IntsList([]float64{2}),
			"[[2]]",
		},
		{
			IntsList(float64(-1.0)),
			"[[-1]]",
		},
		{
			IntsList(float32(-1.0)),
			"[[-1]]",
		},
		{
			IntsList(math.NaN()),
			"[[NaN]]",
		},
		{
			IntsList(math.Inf(1)),
			"[[NaN]]",
		},
		{
			IntsList(math.Inf(-1)),
			"[[NaN]]",
		},
		{
			IntsList(float32(math.Inf(-1))),
			"[[NaN]]",
		},
		{
			IntsList([]bool{true, true, false}),
			"[[1 1 0]]",
		},
		{
			IntsList([]bool{false}),
			"[[0]]",
		},
		{
			IntsList(true),
			"[[1]]",
		},
		{
			IntsList(false),
			"[[0]]",
		},
		{
			IntsList([]int{}),
			"[]",
		},
		{
			IntsList(nil),
			"[[NaN]]",
		},
		{
			IntsList(Strings([]string{"1", "2", "3"})),
			"[[1] [2] [3]]",
		},
		{
			IntsList(IntsList([]string{"1", "2", "3"})),
			"[[1 2 3]]",
		},

		// Initialization using 1-D slice of interface.
		{
			IntsList([]interface{}{"A", "B", "1", "2"}),
			"[[NaN] [NaN] [1] [2]]",
		},
		{
			IntsList([]interface{}{1, 2, 3}),
			"[[1] [2] [3]]",
		},
		{
			IntsList([]interface{}{1, 2, 3, nil}),
			"[[1] [2] [3] [NaN]]",
		},
		{
			IntsList([]interface{}{[]int{1}, []int{2, 3}, 4, 5, nil}),
			"[[1] [2 3] [4] [5] [NaN]]",
		},
		{
			IntsList([]interface{}{nil}),
			"[[NaN]]",
		},
		{
			IntsList([]interface{}{}),
			"[]",
		},
		{
			IntsList(IntsList([]interface{}{"1", "2", "3", 4, 5})),
			"[[1] [2] [3] [4] [5]]",
		},

		// Initialization using 2-D slice.
		{
			IntsList([][]string{{"A", "B"}, {"1", "2"}}),
			"[[NaN] [1 2]]",
		},
		{
			IntsList([][]string{{"1"}, {"2", "3"}}),
			"[[1] [2 3]]",
		},
		{
			IntsList([][]int{{1}, {2, 3}}),
			"[[1] [2 3]]",
		},
		{
			IntsList([][]int{{2}}),
			"[[2]]",
		},
		{
			IntsList([][]int32{{2}}),
			"[[2]]",
		},
		{
			IntsList([][]int64{{2}}),
			"[[2]]",
		},
		{
			IntsList([][]float64{{1.1}, {2.2, 3.3}}),
			"[[1] [2 3]]",
		},
		{
			IntsList([][]float64{{2.2}}),
			"[[2]]",
		},
		{
			IntsList([][]float32{{2.2}}),
			"[[2]]",
		},
		{
			IntsList([][]float64{{math.NaN()}}),
			"[[NaN]]",
		},
		{
			IntsList([][]float64{{math.Inf(1)}}),
			"[[NaN]]",
		},
		{
			IntsList([][]float64{{math.Inf(-1)}}),
			"[[NaN]]",
		},
		{
			IntsList([][]float32{{float32(math.Inf(-1))}}),
			"[[NaN]]",
		},
		{
			IntsList([][]bool{{true}, {true, false}}),
			"[[1] [1 0]]",
		},
		{
			IntsList([][]bool{{false}}),
			"[[0]]",
		},
		{
			IntsList([][]int{}),
			"[]",
		},
		{
			IntsList(StringsList([][]string{{"1"}, {"2", "3"}})),
			"[[1] [2 3]]",
		},
		{
			IntsList(IntsList([][]string{{"1"}, {"2", "3"}})),
			"[[1] [2 3]]",
		},
		{
			IntsList(FloatsList([][]float64{{1.1}, {2.2, 3.3}})),
			"[[1] [2 3]]",
		},
		{
			IntsList(BoolsList([][]bool{{true}, {true, false}})),
			"[[1] [1 0]]",
		},

		// Initialization using 2-D slice of interface.
		{
			IntsList([][]interface{}{{"A", "B"}, {"1", "2"}}),
			"[[NaN] [1 2]]",
		},
		{
			IntsList([][]interface{}{{"1"}, {"2", "3"}}),
			"[[1] [2 3]]",
		},
		{
			IntsList([][]interface{}{{"1"}, {"2", "3"}, nil, {4, 5, 6}}),
			"[[1] [2 3] [NaN] [4 5 6]]",
		},
		{
			IntsList([][]interface{}{{1}, {2, 3}, {"4"}, {"5", "6"}}),
			"[[1] [2 3] [4] [5 6]]",
		},
	}
	for testnum, test := range table {
		if err := test.series.Err; err != nil {
			t.Errorf("Test:%v\nError:%v", testnum, err)
		}
		expected := test.expected
		received := fmt.Sprint(test.series)
		if expected != received {
			t.Errorf(
				"Test:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, expected, received,
			)
		}
		if err := checkTypes(test.series); err != nil {
			t.Errorf("Test:%v\nError:%v", testnum, err)
		}
	}
}

func TestFloatsList(t *testing.T) {
	table := []struct {
		series   Series
		expected string
	}{
		// Initialization using 1-D slice or single value.
		{
			FloatsList([]string{"A", "B", "1", "2"}),
			"[[NaN]]",
		},
		{
			FloatsList([]string{"1"}),
			"[[1.000000]]",
		},
		{
			FloatsList([]string{"NaN"}),
			"[[NaN]]",
		},
		{
			FloatsList("2.1"),
			"[[2.100000]]",
		},
		{
			FloatsList("NaN"),
			"[[NaN]]",
		},
		{
			FloatsList("A"),
			"[[NaN]]",
		},
		{
			FloatsList([]int{1, 2, 3}),
			"[[1.000000 2.000000 3.000000]]",
		},
		{
			FloatsList([]int{2}),
			"[[2.000000]]",
		},
		{
			FloatsList(-1),
			"[[-1.000000]]",
		},
		{
			FloatsList(int32(-1)),
			"[[-1.000000]]",
		},
		{
			FloatsList(int64(-1)),
			"[[-1.000000]]",
		},
		{
			FloatsList([]float64{1.1, 2, 3}),
			"[[1.100000 2.000000 3.000000]]",
		},
		{
			FloatsList([]float64{2}),
			"[[2.000000]]",
		},
		{
			FloatsList(float64(-1.0)),
			"[[-1.000000]]",
		},
		{
			FloatsList(float32(-1.0)),
			"[[-1.000000]]",
		},
		{
			FloatsList(math.NaN()),
			"[[NaN]]",
		},
		{
			FloatsList(math.Inf(1)),
			"[[+Inf]]",
		},
		{
			FloatsList(math.Inf(-1)),
			"[[-Inf]]",
		},
		{
			FloatsList([]bool{true, true, false}),
			"[[1.000000 1.000000 0.000000]]",
		},
		{
			FloatsList([]bool{false}),
			"[[0.000000]]",
		},
		{
			FloatsList(true),
			"[[1.000000]]",
		},
		{
			FloatsList(false),
			"[[0.000000]]",
		},
		{
			FloatsList([]int{}),
			"[]",
		},
		{
			FloatsList(nil),
			"[[NaN]]",
		},
		{
			FloatsList(Strings([]string{"1", "2", "3"})),
			"[[1.000000] [2.000000] [3.000000]]",
		},
		{
			FloatsList(FloatsList([]string{"1", "2", "3"})),
			"[[1.000000 2.000000 3.000000]]",
		},

		// Initialization using 1-D slice of interface.
		{
			FloatsList([]interface{}{"A", "B", "1", "2"}),
			"[[NaN] [NaN] [1.000000] [2.000000]]",
		},
		{
			FloatsList([]interface{}{"1"}),
			"[[1.000000]]",
		},
		{
			FloatsList([]interface{}{"1", "2", 3.14, nil}),
			"[[1.000000] [2.000000] [3.140000] [NaN]]",
		},
		{
			FloatsList([]interface{}{[]string{"1", "2"}, []float64{3.14}, 0.25, nil}),
			"[[1.000000 2.000000] [3.140000] [0.250000] [NaN]]",
		},
		{
			FloatsList([]interface{}{nil}),
			"[[NaN]]",
		},
		{
			FloatsList([]interface{}{}),
			"[]",
		},
		{
			FloatsList([]interface{}{1.1, 2, 3}),
			"[[1.100000] [2.000000] [3.000000]]",
		},
		{
			FloatsList(FloatsList([]interface{}{"1", "2", "3"})),
			"[[1.000000] [2.000000] [3.000000]]",
		},

		// Initialization using 2-D slice.
		{
			FloatsList([][]string{{"A", "B"}, {"1", "2"}}),
			"[[NaN] [1.000000 2.000000]]",
		},
		{
			FloatsList([][]string{{"1"}}),
			"[[1.000000]]",
		},
		{
			FloatsList([][]int{{1}, {2, 3}}),
			"[[1.000000] [2.000000 3.000000]]",
		},
		{
			FloatsList([][]int{{2}}),
			"[[2.000000]]",
		},
		{
			FloatsList([][]int32{{2}}),
			"[[2.000000]]",
		},
		{
			FloatsList([][]int64{{2}}),
			"[[2.000000]]",
		},
		{
			FloatsList([][]float64{{1.1}, {2, 3}}),
			"[[1.100000] [2.000000 3.000000]]",
		},
		{
			FloatsList([][]float64{{2}}),
			"[[2.000000]]",
		},
		{
			FloatsList([][]float32{{2}}),
			"[[2.000000]]",
		},
		{
			FloatsList([][]float64{{math.NaN()}}),
			"[[NaN]]",
		},
		{
			FloatsList([][]float64{{math.Inf(1)}}),
			"[[+Inf]]",
		},
		{
			FloatsList([][]float64{{math.Inf(-1)}}),
			"[[-Inf]]",
		},
		{
			FloatsList([][]bool{{true}, {true, false}}),
			"[[1.000000] [1.000000 0.000000]]",
		},
		{
			FloatsList([][]bool{{false}}),
			"[[0.000000]]",
		},
		{
			FloatsList([][]int{}),
			"[]",
		},
		{
			FloatsList(StringsList([][]string{{"1"}, {"2", "3"}})),
			"[[1.000000] [2.000000 3.000000]]",
		},
		{
			FloatsList(IntsList([][]int{{1}, {2, 3}})),
			"[[1.000000] [2.000000 3.000000]]",
		},
		{
			FloatsList(FloatsList([][]float64{{1.1}, {2, 3}})),
			"[[1.100000] [2.000000 3.000000]]",
		},
		{
			FloatsList(BoolsList([][]bool{{true}, {true, false}})),
			"[[1.000000] [1.000000 0.000000]]",
		},

		// Initialization using 2-D slice of interface.
		{
			FloatsList([][]interface{}{{"A", "B"}, {"1", "2"}}),
			"[[NaN] [1.000000 2.000000]]",
		},
		{
			FloatsList([][]interface{}{{1.1}, {2, 3}}),
			"[[1.100000] [2.000000 3.000000]]",
		},
		{
			FloatsList([][]interface{}{{1.1}, {2, 3}, nil, {"3.14", "0.25"}}),
			"[[1.100000] [2.000000 3.000000] [NaN] [3.140000 0.250000]]",
		},
		{
			FloatsList(FloatsList([][]interface{}{{1.1}, {2, 3}})),
			"[[1.100000] [2.000000 3.000000]]",
		},
		{
			FloatsList(FloatsList([][]interface{}{{1.1}, {2, 3}, nil, {"3.14", "0.25"}})),
			"[[1.100000] [2.000000 3.000000] [NaN] [3.140000 0.250000]]",
		},
	}
	for testnum, test := range table {
		if err := test.series.Err; err != nil {
			t.Errorf("Test:%v\nError:%v", testnum, err)
		}
		expected := test.expected
		received := fmt.Sprint(test.series)
		if expected != received {
			t.Errorf(
				"Test:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, expected, received,
			)
		}
		if err := checkTypes(test.series); err != nil {
			t.Errorf("Test:%v\nError:%v", testnum, err)
		}
	}
}

func TestBoolsList(t *testing.T) {
	table := []struct {
		series   Series
		expected string
	}{
		// Initialization using 1-D slice or single value.
		{
			BoolsList([]string{"A", "true", "1", "f"}),
			"[[NaN]]",
		},
		{
			BoolsList([]string{"t"}),
			"[[true]]",
		},
		{
			BoolsList("False"),
			"[[false]]",
		},
		{
			BoolsList("True"),
			"[[true]]",
		},
		{
			BoolsList("NaN"),
			"[[NaN]]",
		},
		{
			BoolsList("A"),
			"[[NaN]]",
		},
		{
			BoolsList([]int{1, 2, 0}),
			"[[NaN]]",
		},
		{
			BoolsList([]int{1}),
			"[[true]]",
		},
		{
			BoolsList(-1),
			"[[NaN]]",
		},
		{
			BoolsList(0),
			"[[false]]",
		},
		{
			BoolsList(1),
			"[[true]]",
		},
		{
			BoolsList(int32(-1)),
			"[[NaN]]",
		},
		{
			BoolsList(int32(0)),
			"[[false]]",
		},
		{
			BoolsList(int32(1)),
			"[[true]]",
		},
		{
			BoolsList(int64(-1)),
			"[[NaN]]",
		},
		{
			BoolsList(int64(0)),
			"[[false]]",
		},
		{
			BoolsList(int64(1)),
			"[[true]]",
		},
		{
			BoolsList([]float64{1, 2, 0}),
			"[[NaN]]",
		},
		{
			BoolsList([]float64{0}),
			"[[false]]",
		},
		{
			BoolsList(float64(-1.0)),
			"[[NaN]]",
		},
		{
			BoolsList(float64(1.0)),
			"[[true]]",
		},
		{
			BoolsList(float64(0.0)),
			"[[false]]",
		},
		{
			BoolsList(float32(-1.0)),
			"[[NaN]]",
		},
		{
			BoolsList(float32(1.0)),
			"[[true]]",
		},
		{
			BoolsList(float32(0.0)),
			"[[false]]",
		},
		{
			BoolsList(math.NaN()),
			"[[NaN]]",
		},
		{
			BoolsList(math.Inf(1)),
			"[[NaN]]",
		},
		{
			BoolsList(math.Inf(-1)),
			"[[NaN]]",
		},
		{
			BoolsList([]bool{true, true, false}),
			"[[true true false]]",
		},
		{
			BoolsList([]bool{false}),
			"[[false]]",
		},
		{
			BoolsList(true),
			"[[true]]",
		},
		{
			BoolsList([]int{}),
			"[]",
		},
		{
			BoolsList(nil),
			"[[NaN]]",
		},
		{
			BoolsList(Strings([]string{"1", "0", "1"})),
			"[[true] [false] [true]]",
		},

		// Initialization using 1-D slice of interface.
		{
			BoolsList([]interface{}{"A", "true", "1", "f"}),
			"[[NaN] [true] [true] [false]]",
		},
		{
			BoolsList([]interface{}{"t", 0, "true", 1}),
			"[[true] [false] [true] [true]]",
		},
		{
			BoolsList([]interface{}{"t", 0, "true", 1, nil}),
			"[[true] [false] [true] [true] [NaN]]",
		},
		{
			BoolsList([]interface{}{[]string{"t", "true", "1"}, "false", []int{1, 0}, nil}),
			"[[true true true] [false] [true false] [NaN]]",
		},
		{
			BoolsList([]interface{}{nil}),
			"[[NaN]]",
		},
		{
			BoolsList([]interface{}{}),
			"[]",
		},
		{
			BoolsList(Strings([]interface{}{"1", "0", "1"})),
			"[[true] [false] [true]]",
		},

		// Initialization using 2-D slice.
		{
			BoolsList([][]string{{"A"}, {"NaN"}, {"true", "1", "f"}}),
			"[[NaN] [NaN] [true true false]]",
		},
		{
			BoolsList([][]string{{"t"}}),
			"[[true]]",
		},
		{
			BoolsList([][]int{{1}, {2, 0}}),
			"[[true] [NaN]]",
		},
		{
			BoolsList([][]int{{0}, {1}, {2}}),
			"[[false] [true] [NaN]]",
		},
		{
			BoolsList([][]int32{{0}, {1}, {2}}),
			"[[false] [true] [NaN]]",
		},
		{
			BoolsList([][]int64{{0}, {1}, {2}}),
			"[[false] [true] [NaN]]",
		},
		{
			BoolsList([][]float64{{1}, {2, 0}}),
			"[[true] [NaN]]",
		},
		{
			BoolsList([][]float64{{0}, {1}, {2}}),
			"[[false] [true] [NaN]]",
		},
		{
			BoolsList([][]float32{{0}, {1}, {2}}),
			"[[false] [true] [NaN]]",
		},
		{
			BoolsList([][]float64{{math.NaN()}}),
			"[[NaN]]",
		},
		{
			BoolsList([][]float64{{math.Inf(1)}}),
			"[[NaN]]",
		},
		{
			BoolsList([][]float64{{math.Inf(-1)}}),
			"[[NaN]]",
		},
		{
			BoolsList([][]bool{{true}, {true, false}}),
			"[[true] [true false]]",
		},
		{
			BoolsList([][]bool{{false}}),
			"[[false]]",
		},
		{
			BoolsList([][]int{}),
			"[]",
		},
		{
			BoolsList(nil),
			"[[NaN]]",
		},
		{
			BoolsList(StringsList([][]string{{"1"}, {"0", "1"}, {"1", "2"}})),
			"[[true] [false true] [NaN]]",
		},
		{
			BoolsList(IntsList([][]int{{1}, {0, 1}, {1, 2}})),
			"[[true] [false true] [true false]]",
		},
		{
			BoolsList(FloatsList([][]float64{{1}, {0, 1}, {1, 2}})),
			"[[true] [false true] [true false]]",
		},
		{
			BoolsList(BoolsList([][]bool{{true}, {false, true}})),
			"[[true] [false true]]",
		},

		// Initialization using 2-D slice of interface.
		{
			BoolsList([][]interface{}{{"A"}, {"NaN"}, {"true", "1", "f"}}),
			"[[NaN] [NaN] [true true false]]",
		},
		{
			BoolsList([][]interface{}{{"t"}, {"false"}, {"true", 1, "f"}, {1, 0, 1}}),
			"[[true] [false] [true true false] [true false true]]",
		},
		{
			BoolsList([][]interface{}{{"t"}, {"false"}, {"true", 1, "f"}, nil, {1, 0, 1}, nil}),
			"[[true] [false] [true true false] [NaN] [true false true] [NaN]]",
		},
		{
			BoolsList(BoolsList([][]interface{}{{true}, {false, true}})),
			"[[true] [false true]]",
		},
		{
			BoolsList(BoolsList([][]interface{}{{true}, nil, {false, true}})),
			"[[true] [NaN] [false true]]",
		},
	}
	for testnum, test := range table {
		if err := test.series.Err; err != nil {
			t.Errorf("Test:%v\nError:%v", testnum, err)
		}
		expected := test.expected
		received := fmt.Sprint(test.series)
		if expected != received {
			t.Errorf(
				"Test:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, expected, received,
			)
		}
		if err := checkTypes(test.series); err != nil {
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

		StringsList([]string{"1", "2", "3", "a", "b", "c"}),
		IntsList([]string{"1", "2", "3", "a", "b", "c"}),
		FloatsList([]string{"1", "2", "3", "a", "b", "c"}),
		BoolsList([]string{"1", "0", "1", "t", "f", "c"}),

		StringsList([][]string{{"1", "2", "3"}, {"a", "b", "c"}}),
		IntsList([][]string{{"1", "2", "3"}, {"a", "b", "c"}}),
		FloatsList([][]string{{"1", "2", "3"}, {"a", "b", "c"}}),
		BoolsList([][]string{{"1", "0", "1"}, {"t", "f", "c"}}),
	}
	for testnum, test := range tests {
		a := test
		b := a.Copy()
		if fmt.Sprint(a) != fmt.Sprint(b) {
			t.Error("Different values when copying String elements")
		}
		if err := b.Err; err != nil {
			t.Errorf("Test:%v\nError:%v", testnum, err)
		}
		if err := checkTypes(b); err != nil {
			t.Errorf("Test:%v\nError:%v", testnum, err)
		}
		//if err := checkAddr(a.Addr(), b.Addr()); err != nil {
		//t.Errorf("Test:%v\nError:%v\nA:%v\nB:%v", testnum, err, a.Addr(), b.Addr())
		//}
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

		{
			StringsList([]string{"1", "2", "3", "a", "b", "c"}),
			[]string{"[1 2 3 a b c]"},
		},
		{
			IntsList([]string{"1", "2", "3", "a", "b", "c"}),
			[]string{"[NaN]"},
		},
		{
			FloatsList([]string{"1", "2", "3", "a", "b", "c"}),
			[]string{"[NaN]"},
		},
		{
			BoolsList([]string{"1", "0", "1", "t", "f", "c"}),
			[]string{"[NaN]"},
		},

		{
			StringsList([][]string{{"1", "2", "3"}, {"a", "b", "c"}}),
			[]string{"[1 2 3]", "[a b c]"},
		},
		{
			IntsList([][]string{{"1", "2", "3"}, {"a", "b", "c"}}),
			[]string{"[1 2 3]", "[NaN]"},
		},
		{
			FloatsList([][]string{{"1", "2", "3"}, {"a", "b", "c"}}),
			[]string{"[1.000000 2.000000 3.000000]", "[NaN]"},
		},
		{
			BoolsList([][]string{{"1", "0", "1"}, {"t", "f", "c"}}),
			[]string{"[true false true]", "[NaN]"},
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

func TestSeries_Int(t *testing.T) {
	tests := []struct {
		series   Series
		expected []int
		wantErr  bool
	}{
		{
			Strings([]string{"1", "2", "3", "a", "b", "c"}),
			nil,
			true,
		},
		{
			Ints([]string{"1", "2", "3", "a", "b", "c"}),
			nil,
			true,
		},
		{
			Floats([]string{"1", "2", "3", "a", "b", "c"}),
			nil,
			true,
		},
		{
			Bools([]string{"1", "0", "1", "t", "f", "c"}),
			nil,
			true,
		},

		{
			Strings([]string{"1", "2", "3"}),
			[]int{1, 2, 3},
			false,
		},
		{
			Ints([]string{"1", "2", "3"}),
			[]int{1, 2, 3},
			false,
		},
		{
			Floats([]string{"1", "2", "3"}),
			[]int{1, 2, 3},
			false,
		},
		{
			Bools([]string{"1", "0", "1", "t", "f"}),
			[]int{1, 0, 1, 1, 0},
			false,
		},

		{
			StringsList([]string{"1", "2", "3", "a", "b", "c"}),
			nil,
			true,
		},
		{
			IntsList([]string{"1", "2", "3", "a", "b", "c"}),
			nil,
			true,
		},
		{
			FloatsList([]string{"1", "2", "3", "a", "b", "c"}),
			nil,
			true,
		},
		{
			BoolsList([]string{"1", "0", "1", "t", "f", "c"}),
			nil,
			true,
		},

		{
			StringsList([]string{"1", "2", "3"}),
			nil,
			false,
		},
		{
			IntsList([]string{"1", "2", "3"}),
			nil,
			false,
		},
		{
			FloatsList([]string{"1", "2", "3"}),
			nil,
			false,
		},
		{
			BoolsList([]string{"1", "0", "1", "t", "f"}),
			nil,
			false,
		},

		{
			StringsList([][]string{{"1", "2", "3"}, {"a", "b", "c"}}),
			nil,
			true,
		},
		{
			IntsList([][]string{{"1", "2", "3"}, {"a", "b", "c"}}),
			nil,
			true,
		},
		{
			FloatsList([][]string{{"1", "2", "3"}, {"a", "b", "c"}}),
			nil,
			true,
		},
		{
			BoolsList([][]string{{"1", "0", "1"}, {"t", "f", "c"}}),
			nil,
			true,
		},

		{
			StringsList([][]string{{"1", "2", "3"}}),
			nil,
			true,
		},
		{
			IntsList([][]string{{"1", "2", "3"}}),
			nil,
			true,
		},
		{
			FloatsList([][]string{{"1", "2", "3"}}),
			nil,
			true,
		},
		{
			BoolsList([][]string{{"1", "0", "1"}}),
			nil,
			true,
		},
	}
	for testnum, test := range tests {
		expected := test.expected
		received, err := test.series.Int()
		if test.wantErr && err == nil {
			t.Errorf("Test: %v series.Int() should return error", testnum)
		}
		if !reflect.DeepEqual(expected, received) {
			t.Errorf(
				"Test:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, expected, received,
			)
		}
	}
}

func TestSeries_Float(t *testing.T) {
	precision := 0.0000001
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

		{
			StringsList([]string{"1", "2", "3", "a", "b", "c"}),
			[]float64{0},
		},
		{
			IntsList([]string{"1", "2", "3", "a", "b", "c"}),
			[]float64{math.NaN()},
		},
		{
			FloatsList([]string{"1", "2", "3", "a", "b", "c"}),
			[]float64{math.NaN()},
		},
		{
			BoolsList([]string{"1", "0", "1", "t", "f", "c"}),
			[]float64{math.NaN()},
		},

		{
			StringsList([]string{"1", "2", "3"}),
			[]float64{0},
		},
		{
			IntsList([]string{"1", "2", "3"}),
			[]float64{0},
		},
		{
			FloatsList([]string{"1", "2", "3"}),
			[]float64{0},
		},
		{
			BoolsList([]string{"1", "0", "1", "t", "f"}),
			[]float64{0},
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

func TestSeries_Bool(t *testing.T) {
	tests := []struct {
		series   Series
		expected []bool
		wantErr  bool
	}{
		{
			Strings([]string{"1", "2", "3", "a", "b", "c"}),
			nil,
			true,
		},
		{
			Ints([]string{"1", "2", "3", "a", "b", "c"}),
			nil,
			true,
		},
		{
			Floats([]string{"1", "2", "3", "a", "b", "c"}),
			nil,
			true,
		},
		{
			Bools([]string{"1", "0", "1", "t", "f", "c"}),
			nil,
			true,
		},

		{
			Strings([]string{"true", "t", "1", "false", "f", "0"}),
			[]bool{true, true, true, false, false, false},
			false,
		},
		{
			Ints([]string{"1", "0"}),
			[]bool{true, false},
			false,
		},
		{
			Floats([]string{"1.000000", "0.000000"}),
			[]bool{true, false},
			false,
		},
		{
			Bools([]string{"true", "t", "1", "false", "f", "0"}),
			[]bool{true, true, true, false, false, false},
			false,
		},

		{
			StringsList([]string{"1", "2", "3", "a", "b", "c"}),
			nil,
			true,
		},
		{
			IntsList([]string{"1", "2", "3", "a", "b", "c"}),
			nil,
			true,
		},
		{
			FloatsList([]string{"1", "2", "3", "a", "b", "c"}),
			nil,
			true,
		},
		{
			BoolsList([]string{"1", "0", "1", "t", "f", "c"}),
			nil,
			true,
		},

		{
			StringsList([]string{"true", "t", "1", "false", "f", "0"}),
			nil,
			false,
		},
		{
			IntsList([]string{"1", "0"}),
			nil,
			false,
		},
		{
			FloatsList([]string{"1.000000", "0.000000"}),
			nil,
			false,
		},
		{
			BoolsList([]string{"true", "t", "1", "false", "f", "0"}),
			nil,
			false,
		},

		{
			StringsList([][]string{{"1", "2", "3"}, {"a", "b", "c"}}),
			nil,
			true,
		},
		{
			IntsList([][]string{{"1", "2", "3"}, {"a", "b", "c"}}),
			nil,
			true,
		},
		{
			FloatsList([][]string{{"1", "2", "3"}, {"a", "b", "c"}}),
			nil,
			true,
		},
		{
			BoolsList([][]string{{"1", "0", "1"}, {"t", "f", "c"}}),
			nil,
			true,
		},

		{
			StringsList([][]string{{"true", "t", "1"}, {"false", "f", "0"}}),
			nil,
			false,
		},
		{
			IntsList([][]string{{"1"}, {"0"}}),
			nil,
			false,
		},
		{
			FloatsList([][]string{{"1.000000"}, {"0.000000"}}),
			nil,
			false,
		},
		{
			BoolsList([][]string{{"true", "t", "1"}, {"false", "f", "0"}}),
			nil,
			false,
		},
	}
	for testnum, test := range tests {
		expected := test.expected
		received, err := test.series.Bool()
		if test.wantErr && err == nil {
			t.Errorf("Test: %v series.Bool() should return error", testnum)
		}
		if !reflect.DeepEqual(expected, received) {
			t.Errorf(
				"Test:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, expected, received,
			)
		}
	}
}

func TestSeries_StringList(t *testing.T) {
	tests := []struct {
		series   Series
		expected [][]string
	}{
		{
			Strings([]string{"1", "2", "3", "a", "b", "c"}),
			[][]string{{"1"}, {"2"}, {"3"}, {"a"}, {"b"}, {"c"}},
		},
		{
			Ints([]string{"1", "2", "3", "a", "b", "c"}),
			[][]string{{"1"}, {"2"}, {"3"}, {"NaN"}, {"NaN"}, {"NaN"}},
		},
		{
			Floats([]string{"1", "2", "3", "a", "b", "c"}),
			[][]string{{"1.000000"}, {"2.000000"}, {"3.000000"}, {"NaN"}, {"NaN"}, {"NaN"}},
		},
		{
			Bools([]string{"1", "0", "1", "t", "f", "c"}),
			[][]string{{"true"}, {"false"}, {"true"}, {"true"}, {"false"}, {"NaN"}},
		},

		{
			StringsList([]string{"1", "2", "3", "a", "b", "c"}),
			[][]string{{"1", "2", "3", "a", "b", "c"}},
		},
		{
			IntsList([]string{"1", "2", "3", "a", "b", "c"}),
			[][]string{{"NaN"}},
		},
		{
			FloatsList([]string{"1", "2", "3", "a", "b", "c"}),
			[][]string{{"NaN"}},
		},
		{
			BoolsList([]string{"1", "0", "1", "t", "f", "c"}),
			[][]string{{"NaN"}},
		},

		{
			StringsList([][]string{{"1", "2", "3"}, {"a", "b", "c"}}),
			[][]string{{"1", "2", "3"}, {"a", "b", "c"}},
		},
		{
			IntsList([][]string{{"1", "2", "3"}, {"a", "b", "c"}}),
			[][]string{{"1", "2", "3"}, {"NaN"}},
		},
		{
			FloatsList([][]string{{"1", "2", "3"}, {"a", "b", "c"}}),
			[][]string{{"1.000000", "2.000000", "3.000000"}, {"NaN"}},
		},
		{
			BoolsList([][]string{{"1", "0", "1"}, {"t", "f", "c"}}),
			[][]string{{"true", "false", "true"}, {"NaN"}},
		},
	}
	for testnum, test := range tests {
		expected := test.expected
		received := test.series.StringList()
		if !reflect.DeepEqual(expected, received) {
			t.Errorf(
				"Test:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, expected, received,
			)
		}
	}
}

func TestSeries_IntList(t *testing.T) {
	tests := []struct {
		series   Series
		expected [][]int
		wantErr  bool
	}{
		{
			Strings([]string{"1", "2", "3", "a", "b", "c"}),
			nil,
			true,
		},
		{
			Ints([]string{"1", "2", "3", "a", "b", "c"}),
			nil,
			true,
		},
		{
			Floats([]string{"1", "2", "3", "a", "b", "c"}),
			nil,
			true,
		},
		{
			Bools([]string{"1", "0", "1", "t", "f", "c"}),
			nil,
			true,
		},

		{
			Strings([]string{"1", "2", "3"}),
			[][]int{{1}, {2}, {3}},
			false,
		},
		{
			Ints([]string{"1", "2", "3"}),
			[][]int{{1}, {2}, {3}},
			false,
		},
		{
			Floats([]string{"1", "2", "3"}),
			[][]int{{1}, {2}, {3}},
			false,
		},
		{
			Bools([]string{"1", "0", "t", "f"}),
			[][]int{{1}, {0}, {1}, {0}},
			false,
		},

		{
			StringsList([]string{"1", "2", "3", "a", "b", "c"}),
			nil,
			true,
		},
		{
			IntsList([]string{"1", "2", "3", "a", "b", "c"}),
			nil,
			true,
		},
		{
			FloatsList([]string{"1", "2", "3", "a", "b", "c"}),
			nil,
			true,
		},
		{
			BoolsList([]string{"1", "0", "1", "t", "f", "c"}),
			nil,
			true,
		},

		{
			StringsList([]string{"1", "2", "3"}),
			[][]int{{1, 2, 3}},
			false,
		},
		{
			IntsList([]string{"1", "2", "3"}),
			[][]int{{1, 2, 3}},
			false,
		},
		{
			FloatsList([]string{"1", "2", "3"}),
			[][]int{{1, 2, 3}},
			false,
		},
		{
			BoolsList([]string{"1", "0", "t", "f"}),
			[][]int{{1, 0, 1, 0}},
			false,
		},

		{
			StringsList([][]string{{"1", "2", "3"}, {"a", "b", "c"}}),
			nil,
			true,
		},
		{
			IntsList([][]string{{"1", "2", "3"}, {"a", "b", "c"}}),
			nil,
			true,
		},
		{
			FloatsList([][]string{{"1", "2", "3"}, {"a", "b", "c"}}),
			nil,
			true,
		},
		{
			BoolsList([][]string{{"1", "0", "1"}, {"true", "false"}, {"t", "f", "c"}}),
			nil,
			true,
		},

		{
			StringsList([][]string{{"1", "2", "3"}}),
			[][]int{{1, 2, 3}},
			false,
		},
		{
			IntsList([][]string{{"1", "2", "3"}}),
			[][]int{{1, 2, 3}},
			false,
		},
		{
			FloatsList([][]string{{"1", "2", "3"}}),
			[][]int{{1, 2, 3}},
			false,
		},
		{
			BoolsList([][]string{{"1", "0", "1"}, {"true", "false"}}),
			[][]int{{1, 0, 1}, {1, 0}},
			false,
		},
	}
	for testnum, test := range tests {
		expected := test.expected
		received, err := test.series.IntList()
		if test.wantErr && err == nil {
			t.Errorf("Test: %v series.IntList() should return error", testnum)
		}
		if !reflect.DeepEqual(expected, received) {
			t.Errorf(
				"Test:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, expected, received,
			)
		}
	}
}

func TestSeries_FloatList(t *testing.T) {
	precision := 0.0000001
	floatListEquals := func(x, y [][]float64) bool {
		if len(x) != len(y) {
			return false
		}
		for i := 0; i < len(x); i++ {
			if len(x[i]) != len(y[i]) {
				return false
			}
			for j := 0; j < len(x[i]); j++ {
				a := x[i][j]
				b := y[i][j]
				if (a-b) > precision || (b-a) > precision {
					return false
				}
			}
		}
		return true
	}
	tests := []struct {
		series   Series
		expected [][]float64
	}{
		{
			Strings([]string{"1", "2", "3", "a", "b", "c"}),
			[][]float64{{1}, {2}, {3}, {math.NaN()}, {math.NaN()}, {math.NaN()}},
		},
		{
			Ints([]string{"1", "2", "3", "a", "b", "c"}),
			[][]float64{{1}, {2}, {3}, {math.NaN()}, {math.NaN()}, {math.NaN()}},
		},
		{
			Floats([]string{"1", "2", "3", "a", "b", "c"}),
			[][]float64{{1}, {2}, {3}, {math.NaN()}, {math.NaN()}, {math.NaN()}},
		},
		{
			Bools([]string{"1", "0", "1", "t", "f", "c"}),
			[][]float64{{1}, {0}, {1}, {1}, {0}, {math.NaN()}},
		},

		{
			StringsList([]string{"1", "2", "3", "a", "b", "c"}),
			[][]float64{{math.NaN()}},
		},
		{
			IntsList([]string{"1", "2", "3", "a", "b", "c"}),
			[][]float64{{math.NaN()}},
		},
		{
			FloatsList([]string{"1", "2", "3", "a", "b", "c"}),
			[][]float64{{math.NaN()}},
		},
		{
			BoolsList([]string{"1", "0", "1", "t", "f", "c"}),
			[][]float64{{math.NaN()}},
		},

		{
			StringsList([][]string{{"1", "2", "3"}, {"a", "b", "c"}}),
			[][]float64{{1, 2, 3}, {math.NaN()}},
		},
		{
			IntsList([][]string{{"1", "2", "3"}, {"a", "b", "c"}}),
			[][]float64{{1, 2, 3}, {math.NaN()}},
		},
		{
			FloatsList([][]string{{"1", "2", "3"}, {"a", "b", "c"}}),
			[][]float64{{1, 2, 3}, {math.NaN()}},
		},
		{
			BoolsList([][]string{{"1", "0", "1"}, {"true", "false"}, {"t", "f", "c"}}),
			[][]float64{{1, 0, 1}, {1, 0}, {math.NaN()}},
		},
	}
	for testnum, test := range tests {
		expected := test.expected
		received := test.series.FloatList()
		if !floatListEquals(expected, received) {
			t.Errorf(
				"Test:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, expected, received,
			)
		}
	}
}

func TestSeries_BoolList(t *testing.T) {
	tests := []struct {
		series   Series
		expected [][]bool
		wantErr  bool
	}{
		{
			Strings([]string{"1", "2", "3", "a", "b", "c"}),
			nil,
			true,
		},
		{
			Ints([]string{"1", "2", "3", "a", "b", "c"}),
			nil,
			true,
		},
		{
			Floats([]string{"1", "2", "3", "a", "b", "c"}),
			nil,
			true,
		},
		{
			Bools([]string{"1", "0", "t", "f", "c"}),
			nil,
			true,
		},

		{
			Strings([]string{"1", "0"}),
			[][]bool{{true}, {false}},
			false,
		},
		{
			Ints([]string{"1", "0"}),
			[][]bool{{true}, {false}},
			false,
		},
		{
			Floats([]string{"1", "0"}),
			[][]bool{{true}, {false}},
			false,
		},
		{
			Bools([]string{"1", "0", "t", "f"}),
			[][]bool{{true}, {false}, {true}, {false}},
			false,
		},

		{
			StringsList([][]string{{"1", "2", "3", "a", "b", "c"}}),
			nil,
			false,
		},
		{
			StringsList([][]string{{"1", "2", "3", "a", "b", "c", "NaN"}}),
			nil,
			true,
		},
		{
			IntsList([][]string{{"1", "2", "3", "a", "b", "c"}}),
			nil,
			true,
		},
		{
			FloatsList([][]string{{"1", "2", "3", "a", "b", "c"}}),
			nil,
			true,
		},
		{
			BoolsList([][]string{{"1", "0", "t", "f", "c"}}),
			nil,
			true,
		},

		{
			StringsList([]string{"1", "0"}),
			[][]bool{{true, false}},
			false,
		},
		{
			IntsList([]string{"1", "0"}),
			[][]bool{{true, false}},
			false,
		},
		{
			FloatsList([]string{"1", "0"}),
			[][]bool{{true, false}},
			false,
		},
		{
			BoolsList([]string{"1", "0", "t", "f"}),
			[][]bool{{true, false, true, false}},
			false,
		},

		{
			StringsList([][]string{{"1", "0"}, {"0", "1"}}),
			[][]bool{{true, false}, {false, true}},
			false,
		},
		{
			IntsList([][]string{{"1", "0"}, {"0", "1"}}),
			[][]bool{{true, false}, {false, true}},
			false,
		},
		{
			FloatsList([][]string{{"1", "0"}, {"0", "1"}}),
			[][]bool{{true, false}, {false, true}},
			false,
		},
		{
			BoolsList([][]string{{"1", "0", "t", "f"}, {"0", "1"}}),
			[][]bool{{true, false, true, false}, {false, true}},
			false,
		},
	}
	for testnum, test := range tests {
		expected := test.expected
		received, err := test.series.BoolList()
		if test.wantErr && err == nil {
			t.Errorf("Test: %v series.BoolList() should return error", testnum)
		}
		if !reflect.DeepEqual(expected, received) {
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

		{
			StringsList([]string{"1", "2", "3"}),
			StringsList([]string{"a", "b", "c"}),
			[]string{"[1 2 3]", "[a b c]"},
		},
		{
			IntsList([]string{"1", "2", "3"}),
			IntsList([]string{"a", "4", "c"}),
			[]string{"[1 2 3]", "[NaN]"},
		},
		{
			FloatsList([]string{"1", "2", "3"}),
			FloatsList([]string{"a", "4", "c"}),
			[]string{"[1.000000 2.000000 3.000000]", "[NaN]"},
		},
		{
			BoolsList([]string{"1", "1", "0"}),
			BoolsList([]string{"0", "0", "0"}),
			[]string{"[true true false]", "[false false false]"},
		},

		{
			StringsList([][]string{{"1"}, {"2", "3"}}),
			StringsList([][]string{{"a"}, {"b", "c"}}),
			[]string{"[1]", "[2 3]", "[a]", "[b c]"},
		},
		{
			IntsList([][]string{{"1"}, {"2", "3"}}),
			IntsList([][]string{{"a"}, {"4", "c"}}),
			[]string{"[1]", "[2 3]", "[NaN]", "[NaN]"},
		},
		{
			FloatsList([][]string{{"1"}, {"2", "3"}}),
			FloatsList([][]string{{"a"}, {"4", "c"}}),
			[]string{"[1.000000]", "[2.000000 3.000000]", "[NaN]", "[NaN]"},
		},
		{
			BoolsList([][]string{{"1"}, {"1", "0"}}),
			BoolsList([][]string{{"0"}, {"0", "0"}}),
			[]string{"[true]", "[true false]", "[false]", "[false false]"},
		},
	}
	for testnum, test := range tests {
		ab := test.a.Concat(test.b)
		if err := ab.Err; err != nil {
			t.Errorf("Test:%v\nError:%v", testnum, err)
		}
		received := ab.Records()
		expected := test.expected
		if !reflect.DeepEqual(expected, received) {
			t.Errorf(
				"Test:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, expected, received,
			)
		}
		//a := test.a
		//b := ab
		//if err := checkAddr(a.Addr(), b.Addr()); err != nil {
		//t.Errorf("Test:%v\nError:%v\nA:%v\nAB:%v", testnum, err, a.Addr(), b.Addr())
		//}
		//a = test.b
		//b = ab
		//if err := checkAddr(a.Addr(), b.Addr()); err != nil {
		//t.Errorf("Test:%v\nError:%v\nB:%v\nAB:%v", testnum, err, a.Addr(), b.Addr())
		//}
	}
}

func TestSeries_Concat_2(t *testing.T) {
	tests := []struct {
		a        Series
		b        Series
		expected Series
	}{
		{
			Strings([]string{"1", "2", "3"}),
			Strings([]string{"a", "b", "c"}),
			Strings([]string{"1", "2", "3", "a", "b", "c"}),
		},
		{
			Ints([]int{1, 2, 3}),
			Ints([]int{4, 5, 6}),
			Ints([]int{1, 2, 3, 4, 5, 6}),
		},
		{
			Floats([]float64{1.1, 2.2, 3.3}),
			Floats([]float64{4.4, 5.5, 6.6}),
			Floats([]float64{1.1, 2.2, 3.3, 4.4, 5.5, 6.6}),
		},
		{
			Bools([]bool{true, true, false}),
			Bools([]bool{false, false, true}),
			Bools([]bool{true, true, false, false, false, true}),
		},

		{
			StringsList([]string{"1", "2", "3"}),
			StringsList([]string{"a", "b", "c"}),
			StringsList([][]string{{"1", "2", "3"}, {"a", "b", "c"}}),
		},
		{
			IntsList([]int{1, 2, 3}),
			IntsList([]int{4, 5, 6}),
			IntsList([][]int{{1, 2, 3}, {4, 5, 6}}),
		},
		{
			FloatsList([]float64{1.1, 2.2, 3.3}),
			FloatsList([]float64{4.4, 5.5, 6.6}),
			FloatsList([][]float64{{1.1, 2.2, 3.3}, {4.4, 5.5, 6.6}}),
		},
		{
			BoolsList([]bool{true, true, false}),
			BoolsList([]bool{false, false, true}),
			BoolsList([][]bool{{true, true, false}, {false, false, true}}),
		},

		{
			StringsList([][]string{{"1"}, {"2", "3"}}),
			StringsList([][]string{{"a"}, {"b", "c"}}),
			StringsList([][]string{{"1"}, {"2", "3"}, {"a"}, {"b", "c"}}),
		},
		{
			IntsList([][]int{{1}, {2, 3}}),
			IntsList([][]int{{4}, {5, 6}}),
			IntsList([][]int{{1}, {2, 3}, {4}, {5, 6}}),
		},
		{
			FloatsList([][]float64{{1.1}, {2.2, 3.3}}),
			FloatsList([][]float64{{4.4}, {5.5, 6.6}}),
			FloatsList([][]float64{{1.1}, {2.2, 3.3}, {4.4}, {5.5, 6.6}}),
		},
		{
			BoolsList([][]bool{{true}, {true, false}}),
			BoolsList([][]bool{{false}, {false, true}}),
			BoolsList([][]bool{{true}, {true, false}, {false}, {false, true}}),
		},
	}
	for testnum, test := range tests {
		expected := test.expected
		received := test.a.Concat(test.b)
		if err := received.Err; err != nil {
			t.Errorf("Test:%v\nError:%v", testnum, err)
		}
		isEquals, err := compareSeries(expected, received)
		if err != nil {
			t.Errorf("Test:%v\nError:%v", testnum, err)
		}
		if !isEquals {
			t.Errorf(
				"Test:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, expected, received,
			)
		}
	}
}

func TestSeries_Order(t *testing.T) {
	tests := []struct {
		series   Series
		reverse  bool
		expected []int
	}{
		{
			Ints([]string{"2", "1", "3", "NaN", "4", "NaN"}),
			false,
			[]int{1, 0, 2, 4, 3, 5},
		},
		{
			Floats([]string{"2", "1", "3", "NaN", "4", "NaN"}),
			false,
			[]int{1, 0, 2, 4, 3, 5},
		},
		{
			Strings([]string{"c", "b", "a"}),
			false,
			[]int{2, 1, 0},
		},
		{
			Bools([]bool{true, false, false, false, true}),
			false,
			[]int{1, 2, 3, 0, 4},
		},
		{
			Ints([]string{"2", "1", "3", "NaN", "4", "NaN"}),
			true,
			[]int{4, 2, 0, 1, 3, 5},
		},
		{
			Floats([]string{"2", "1", "3", "NaN", "4", "NaN"}),
			true,
			[]int{4, 2, 0, 1, 3, 5},
		},
		{
			Strings([]string{"c", "b", "a"}),
			true,
			[]int{0, 1, 2},
		},
		{
			Bools([]bool{true, false, false, false, true}),
			true,
			[]int{0, 4, 1, 2, 3},
		},

		// int_list, reversed=false
		{
			IntsList([]int{3, 2, 1}),
			false,
			[]int{0},
		},
		{
			IntsList([][]int{{3}, {2}, {1}}),
			false,
			[]int{2, 1, 0},
		},
		{
			IntsList([][]int{{1, 3}, {1, 2}, {1, 1}}),
			false,
			[]int{2, 1, 0},
		},
		{
			IntsList([][]int{{3, 3}, {2, 2}, {1, 1}}),
			false,
			[]int{2, 1, 0},
		},
		{
			IntsList([][]int{{1, 1, 1}, {1, 1}, {1}}),
			false,
			[]int{2, 1, 0},
		},
		{
			IntsList([][]int{{1, 1, 1}, {1, 1}, nil, {}, nil, {1}}),
			false,
			[]int{3, 5, 1, 0, 2, 4},
		},
		{
			IntsList([][]string{{"1", "1", "1"}, {"1", "1"}, {"NaN"}, {}, {"B"}, {"1"}}),
			false,
			[]int{3, 5, 1, 0, 2, 4},
		},

		// int_list, reversed=true
		{
			IntsList([]int{3, 2, 1}),
			true,
			[]int{0},
		},
		{
			IntsList([][]int{{3}, {2}, {1}}),
			true,
			[]int{0, 1, 2},
		},
		{
			IntsList([][]int{{1, 3}, {1, 2}, {1, 1}}),
			true,
			[]int{0, 1, 2},
		},
		{
			IntsList([][]int{{3, 3}, {2, 2}, {1, 1}}),
			true,
			[]int{0, 1, 2},
		},
		{
			IntsList([][]int{{1, 1, 1}, {1, 1}, {1}}),
			true,
			[]int{0, 1, 2},
		},
		{
			IntsList([][]int{{1, 1, 1}, {1, 1}, nil, {}, nil, {1}}),
			true,
			[]int{0, 1, 5, 3, 2, 4},
		},
		{
			IntsList([][]string{{"1", "1", "1"}, {"1", "1"}, {"NaN"}, {}, {"B"}, {"1"}}),
			true,
			[]int{0, 1, 5, 3, 2, 4},
		},

		{
			StringsList([][]string{{"A", "B", "C"}, {"D"}, {"E", "F"}}),
			false,
			[]int{1, 2, 0},
		},
		{
			FloatsList([][]float64{{3.14, 0.25}, nil, {}, {0.1}}),
			false,
			[]int{2, 3, 0, 1},
		},
		{
			BoolsList([][]bool{{true}, {false}, {false, false}, {false, true}}),
			false,
			[]int{1, 0, 2, 3},
		},
	}
	for testnum, test := range tests {
		received := test.series.Order(test.reverse)
		expected := test.expected
		if !reflect.DeepEqual(expected, received) {
			t.Errorf(
				"Test:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, expected, received,
			)
		}
	}
}

func TestSeries_IsNaN(t *testing.T) {
	tests := []struct {
		series   Series
		expected []bool
	}{
		{
			Ints([]string{"2", "1", "3", "NaN", "4", "NaN"}),
			[]bool{false, false, false, true, false, true},
		},
		{
			Floats([]string{"A", "1", "B", "3"}),
			[]bool{true, false, true, false},
		},
		{
			IntsList([]string{"2", "1", "3", "NaN", "4", "NaN"}),
			[]bool{true},
		},
		{
			FloatsList([]string{"A", "1", "B", "3"}),
			[]bool{true},
		},
		{
			IntsList([]string{"2", "1", "3", "4"}),
			[]bool{false},
		},
		{
			FloatsList([]string{"1", "3"}),
			[]bool{false},
		},
		{
			IntsList([][]string{{"2", "1", "3"}, {"NaN", "4", "NaN"}}),
			[]bool{false, true},
		},
		{
			FloatsList([][]string{{"1", "3"}, {"A", "B"}}),
			[]bool{false, true},
		},
		{
			IntsList([][]string{{"2"}, {"1", "3"}, {"4"}}),
			[]bool{false, false, false},
		},
		{
			FloatsList([][]string{{"1"}, {"3"}, {"3.14", "4.5"}}),
			[]bool{false, false, false},
		},
	}
	for testnum, test := range tests {
		received := test.series.IsNaN()
		expected := test.expected
		if !reflect.DeepEqual(expected, received) {
			t.Errorf(
				"Test:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, expected, received,
			)
		}
	}
}

func TestSeries_StdDev(t *testing.T) {
	tests := []struct {
		series   Series
		expected float64
	}{
		{
			Ints([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
			3.02765,
		},
		{
			Floats([]float64{1.0, 2.0, 3.0}),
			1.0,
		},
		{
			Strings([]string{"A", "B", "C", "D"}),
			math.NaN(),
		},
		{
			Bools([]bool{true, true, false, true}),
			0.5,
		},
		{
			Floats([]float64{}),
			math.NaN(),
		},

		{
			IntsList([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
			3.02765,
		},
		{
			IntsList([][]int{{1, 2, 3, 4, 5}, {6, 7, 8, 9, 10}}),
			3.02765,
		},
		{
			FloatsList([]float64{1.0, 2.0, 3.0}),
			1.0,
		},
		{
			FloatsList([][]float64{{1.0}, {2.0, 3.0}}),
			1.0,
		},
		{
			StringsList([]string{"A", "B", "C", "D"}),
			math.NaN(),
		},
		{
			StringsList([][]string{{"A", "B"}, {"C", "D"}}),
			math.NaN(),
		},
		{
			BoolsList([]bool{true, true, false, true}),
			0.5,
		},
		{
			BoolsList([][]bool{{true, true}, {false, true}}),
			0.5,
		},
		{
			FloatsList([]float64{}),
			math.NaN(),
		},
		{
			FloatsList([][]float64{{}}),
			math.NaN(),
		},
	}

	for testnum, test := range tests {
		received := test.series.StdDev()
		expected := test.expected
		if !compareFloats(received, expected, 6) {
			t.Errorf(
				"Test:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, expected, received,
			)
		}
	}
}

func TestSeries_Mean(t *testing.T) {
	tests := []struct {
		series   Series
		expected float64
	}{
		{
			Ints([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
			5.5,
		},
		{
			Floats([]float64{1.0, 2.0, 3.0}),
			2.0,
		},
		{
			Strings([]string{"A", "B", "C", "D"}),
			math.NaN(),
		},
		{
			Bools([]bool{true, true, false, true}),
			0.75,
		},
		{
			Floats([]float64{}),
			math.NaN(),
		},

		{
			IntsList([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
			5.5,
		},
		{
			IntsList([][]int{{1, 2, 3, 4, 5}, {6, 7, 8, 9, 10}}),
			5.5,
		},
		{
			FloatsList([]float64{1.0, 2.0, 3.0}),
			2.0,
		},
		{
			FloatsList([][]float64{{1.0}, {2.0, 3.0}}),
			2.0,
		},
		{
			StringsList([]string{"A", "B", "C", "D"}),
			math.NaN(),
		},
		{
			StringsList([][]string{{"A", "B"}, {"C", "D"}}),
			math.NaN(),
		},
		{
			BoolsList([]bool{true, true, false, true}),
			0.75,
		},
		{
			BoolsList([][]bool{{true, true}, {false, true}}),
			0.75,
		},
		{
			FloatsList([]float64{}),
			math.NaN(),
		},
		{
			FloatsList([][]float64{{}}),
			math.NaN(),
		},
	}

	for testnum, test := range tests {
		received := test.series.Mean()
		expected := test.expected
		if !compareFloats(received, expected, 6) {
			t.Errorf(
				"Test:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, expected, received,
			)
		}
	}
}

func TestSeries_Max(t *testing.T) {
	tests := []struct {
		series   Series
		expected float64
	}{
		{
			Ints([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
			10,
		},
		{
			Floats([]float64{1.0, 2.0, 3.0}),
			3.0,
		},
		{
			Strings([]string{"A", "B", "C", "D"}),
			math.NaN(),
		},
		{
			Bools([]bool{true, true, false, true}),
			1.0,
		},
		{
			Floats([]float64{}),
			math.NaN(),
		},

		{
			IntsList([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
			10,
		},
		{
			IntsList([][]int{{1, 2, 3, 4, 5}, {6, 7, 8, 9, 10}}),
			10,
		},
		{
			FloatsList([]float64{1.0, 2.0, 3.0}),
			3.0,
		},
		{
			FloatsList([][]float64{{1.0}, {2.0, 3.0}}),
			3.0,
		},
		{
			StringsList([]string{"A", "B", "C", "D"}),
			math.NaN(),
		},
		{
			StringsList([][]string{{"A", "B"}, {"C", "D"}}),
			math.NaN(),
		},
		{
			BoolsList([]bool{true, true, false, true}),
			1.0,
		},
		{
			BoolsList([][]bool{{true, true}, {false, true}}),
			1.0,
		},
		{
			FloatsList([]float64{}),
			math.NaN(),
		},
		{
			FloatsList([][]float64{{}}),
			math.NaN(),
		},
	}

	for testnum, test := range tests {
		received := test.series.Max()
		expected := test.expected
		if !compareFloats(received, expected, 6) {
			t.Errorf(
				"Test:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, expected, received,
			)
		}
	}
}

func TestSeries_Median(t *testing.T) {
	tests := []struct {
		series   Series
		expected float64
	}{
		{
			// Extreme observations should not factor in.
			Ints([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 100, 1000, 10000}),
			7,
		},
		{
			// Change in order should not influence result.
			Ints([]int{1, 2, 3, 10, 100, 1000, 10000, 4, 5, 6, 7, 8, 9}),
			7,
		},
		{
			Floats([]float64{
				20.2755, 4.98964, -20.2006, 1.19854, 1.89977,
				1.51178, -17.4687, 4.65567, -8.65952, 6.31649,
			}),
			1.705775,
		},
		{
			// Change in order should not influence result.
			Floats([]float64{
				4.98964, -20.2006, 1.89977, 1.19854,
				1.51178, -17.4687, -8.65952, 20.2755, 4.65567, 6.31649,
			}),
			1.705775,
		},
		{
			Strings([]string{"A", "B", "C", "D"}),
			math.NaN(),
		},
		{
			Bools([]bool{true, true, false, true}),
			math.NaN(),
		},
		{
			Floats([]float64{}),
			math.NaN(),
		},

		{
			// Extreme observations should not factor in.
			IntsList([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 100, 1000, 10000}),
			7,
		},
		{
			// Extreme observations should not factor in.
			IntsList([][]int{{1, 2, 3, 4, 5, 6, 7, 8, 9}, {10, 100, 1000, 10000}}),
			7,
		},
		{
			// Change in order should influence result.
			IntsList([]int{1, 2, 3, 10, 100, 1000, 10000, 4, 5, 6, 7, 8, 9}),
			7,
		},
		{
			// Change in order should influence result.
			IntsList([][]int{{1, 2, 3, 10, 100, 1000, 10000}, {4, 5, 6, 7, 8, 9}}),
			7,
		},

		{
			FloatsList([]float64{
				20.2755, 4.98964, -20.2006, 1.19854, 1.89977,
				1.51178, -17.4687, 4.65567, -8.65952, 6.31649,
			}),
			1.705775,
		},
		{
			// Change in order should not influence result.
			FloatsList([][]float64{
				{4.98964, -20.2006, 1.89977, 1.19854},
				{1.51178, -17.4687, -8.65952, 20.2755, 4.65567, 6.31649},
			}),
			1.705775,
		},

		{
			StringsList([]string{"A", "B", "C", "D"}),
			math.NaN(),
		},
		{
			StringsList([][]string{{"A", "B"}, {"C", "D"}}),
			math.NaN(),
		},
		{
			BoolsList([]bool{true, true, false, true}),
			math.NaN(),
		},
		{
			BoolsList([][]bool{{true, true}, {false, true}}),
			math.NaN(),
		},
		{
			FloatsList([]float64{}),
			math.NaN(),
		},
		{
			FloatsList([][]float64{{}}),
			math.NaN(),
		},
	}

	for testnum, test := range tests {
		received := test.series.Median()
		expected := test.expected
		if !compareFloats(received, expected, 6) {
			t.Errorf(
				"Test:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, expected, received,
			)
		}
	}
}

func TestSeries_Min(t *testing.T) {
	tests := []struct {
		series   Series
		expected float64
	}{
		{
			Ints([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
			1.0,
		},
		{
			Floats([]float64{1.0, 2.0, 3.0}),
			1.0,
		},
		{
			Strings([]string{"A", "B", "C", "D"}),
			math.NaN(),
		},
		{
			Bools([]bool{true, true, false, true}),
			0.0,
		},
		{
			Floats([]float64{}),
			math.NaN(),
		},

		{
			IntsList([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
			1.0,
		},
		{
			IntsList([][]int{{1, 2, 3, 4, 5}, {6, 7, 8, 9, 10}}),
			1.0,
		},
		{
			FloatsList([]float64{1.0, 2.0, 3.0}),
			1.0,
		},
		{
			FloatsList([][]float64{{1.0}, {2.0, 3.0}}),
			1.0,
		},
		{
			StringsList([]string{"A", "B", "C", "D"}),
			math.NaN(),
		},
		{
			StringsList([][]string{{"A", "B"}, {"C", "D"}}),
			math.NaN(),
		},
		{
			BoolsList([]bool{true, true, false, true}),
			0.0,
		},
		{
			BoolsList([][]bool{{true, true}, {false, true}}),
			0.0,
		},
		{
			FloatsList([]float64{}),
			math.NaN(),
		},
		{
			FloatsList([][]float64{{}}),
			math.NaN(),
		},
	}

	for testnum, test := range tests {
		received := test.series.Min()
		expected := test.expected
		if !compareFloats(received, expected, 6) {
			t.Errorf(
				"Test:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, expected, received,
			)
		}
	}
}

func TestSeries_MaxStr(t *testing.T) {
	tests := []struct {
		series   Series
		expected string
	}{
		{
			Ints([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
			"",
		},
		{
			Floats([]float64{1.0, 2.0, 3.0}),
			"",
		},
		{
			Strings([]string{"A", "B", "C", "D"}),
			"D",
		},
		{
			Strings([]string{"quick", "Brown", "fox", "Lazy", "dog"}),
			"quick",
		},
		{
			Bools([]bool{true, true, false, true}),
			"",
		},
		{
			Floats([]float64{}),
			"",
		},

		{
			IntsList([][]int{{1, 2, 3, 4, 5}, {6, 7, 8, 9, 10}}),
			"",
		},
		{
			FloatsList([][]float64{{1.0}, {2.0, 3.0}}),
			"",
		},
		{
			StringsList([][]string{{"A", "B"}, {"C", "D"}}),
			"D",
		},
		{
			StringsList([][]string{{"quick", "Brown", "fox"}, {"Lazy", "dog"}}),
			"quick",
		},
		{
			BoolsList([][]bool{{true, true}, {false, true}}),
			"",
		},
		{
			FloatsList([][]float64{{}}),
			"",
		},
	}

	for testnum, test := range tests {
		received := test.series.MaxStr()
		expected := test.expected
		if received != expected {
			t.Errorf(
				"Test:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, expected, received,
			)
		}
	}
}

func TestSeries_MinStr(t *testing.T) {
	tests := []struct {
		series   Series
		expected string
	}{
		{
			Ints([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
			"",
		},
		{
			Floats([]float64{1.0, 2.0, 3.0}),
			"",
		},
		{
			Strings([]string{"A", "B", "C", "D"}),
			"A",
		},
		{
			Strings([]string{"quick", "Brown", "fox", "Lazy", "dog"}),
			"Brown",
		},
		{
			Bools([]bool{true, true, false, true}),
			"",
		},
		{
			Floats([]float64{}),
			"",
		},

		{
			IntsList([][]int{{1, 2, 3, 4, 5}, {6, 7, 8, 9, 10}}),
			"",
		},
		{
			FloatsList([][]float64{{1.0}, {2.0, 3.0}}),
			"",
		},
		{
			StringsList([][]string{{"A", "B"}, {"C", "D"}}),
			"A",
		},
		{
			StringsList([][]string{{"quick", "Brown", "fox"}, {"Lazy", "dog"}}),
			"Brown",
		},
		{
			BoolsList([][]bool{{true, true}, {false, true}}),
			"",
		},
		{
			FloatsList([][]float64{{}}),
			"",
		},
	}

	for testnum, test := range tests {
		received := test.series.MinStr()
		expected := test.expected
		if received != expected {
			t.Errorf(
				"Test:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, expected, received,
			)
		}
	}
}

func TestSeries_Quantile(t *testing.T) {
	tests := []struct {
		series   Series
		p        float64
		expected float64
	}{
		{
			Ints([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
			0.9,
			9,
		},
		{
			Floats([]float64{3.141592, math.Sqrt(3), 2.718281, math.Sqrt(2)}),
			0.8,
			3.141592,
		},
		{
			Floats([]float64{1.0, 2.0, 3.0}),
			0.5,
			2.0,
		},
		{
			Strings([]string{"A", "B", "C", "D"}),
			0.25,
			math.NaN(),
		},
		{
			Bools([]bool{false, false, false, true}),
			0.75,
			0.0,
		},
		{
			Floats([]float64{}),
			0.50,
			math.NaN(),
		},

		{
			IntsList([][]int{{1, 2, 3, 4, 5}, {6, 7, 8, 9, 10}}),
			0.9,
			9,
		},
		{
			FloatsList([][]float64{{3.141592, math.Sqrt(3)}, {2.718281, math.Sqrt(2)}}),
			0.8,
			3.141592,
		},
		{
			FloatsList([][]float64{{1.0}, {2.0, 3.0}}),
			0.5,
			2.0,
		},
		{
			StringsList([][]string{{"A", "B"}, {"C", "D"}}),
			0.25,
			math.NaN(),
		},
		{
			BoolsList([][]bool{{false, false}, {false, true}}),
			0.75,
			0.0,
		},
		{
			FloatsList([][]float64{{}}),
			0.50,
			math.NaN(),
		},
	}

	for testnum, test := range tests {
		received := test.series.Quantile(test.p)
		expected := test.expected
		if !compareFloats(received, expected, 6) {
			t.Errorf(
				"Test:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, expected, received,
			)
		}
	}
}

func TestSeries_Map(t *testing.T) {
	tests := []struct {
		series   Series
		expected Series
	}{
		{
			Bools([]bool{false, true, false, false, true}),
			Bools([]bool{false, true, false, false, true}),
		},
		{
			Floats([]float64{1.5, -3.23, -0.337397, -0.380079, 1.60979, 34.}),
			Floats([]float64{3, -6.46, -0.674794, -0.760158, 3.21958, 68.}),
		},
		{
			Floats([]float64{math.Pi, math.Phi, math.SqrtE, math.Cbrt(64)}),
			Floats([]float64{2 * math.Pi, 2 * math.Phi, 2 * math.SqrtE, 2 * math.Cbrt(64)}),
		},
		{
			Strings([]string{"XyZApple", "XyZBanana", "XyZCitrus", "XyZDragonfruit"}),
			Strings([]string{"Apple", "Banana", "Citrus", "Dragonfruit"}),
		},
		{
			Strings([]string{"San Francisco", "XyZTokyo", "MoscowXyZ", "XyzSydney"}),
			Strings([]string{"San Francisco", "Tokyo", "MoscowXyZ", "XyzSydney"}),
		},
		{
			Ints([]int{23, 13, 101, -64, -3}),
			Ints([]int{28, 18, 106, -59, 2}),
		},
		{
			Ints([]string{"morning", "noon", "afternoon", "evening", "night"}),
			Ints([]int{5, 5, 5, 5, 5}),
		},

		{
			BoolsList([][]bool{{false, true}, {false, false}, {true}, {true, true}, {true, true, false}}),
			BoolsList([][]bool{{false}, {false}, {true}, {true}, {false}}),
		},
		{
			FloatsList([][]float64{{1.5, -3.23}, {-0.337397, -0.380079}, {1.60979, 34.}}),
			FloatsList([][]float64{{1.5}, {-0.337397}, {34.}}),
		},
		{
			FloatsList([][]float64{{math.Pi, math.Phi, math.SqrtE, math.Cbrt(64)}}),
			FloatsList([][]float64{{4}}),
		},
		{
			StringsList([][]string{{"San", "Francisco"}, {"Toronto", "Tokyo"}}),
			StringsList([][]string{{"SanFrancisco"}, {"TorontoTokyo"}}),
		},
		{
			IntsList([][]int{{23, 13}, {101, -64}, {-3, -9}}),
			IntsList([][]int{{36}, {37}, {-12}}),
		},
	}

	doubleFloat64 := func(e Element) Element {
		result := e.Copy()
		result.Set(result.Float() * 2)
		return Element(result)
	}

	maxFloat64 := func(e Element) Element {
		floatList := e.FloatList()
		result := floatListElement{}
		max := float64(math.MinInt64)
		for _, e := range floatList {
			if e > max {
				max = e
			}
		}
		result.Set(max)
		return Element(&result)
	}

	// and two booleans
	and := func(e Element) Element {
		result := e.Copy()
		b, err := result.Bool()
		if err != nil {
			t.Errorf("%v", err)
			return Element(nil)
		}
		result.Set(b && true)
		return Element(result)
	}

	// and all elements of bool_list
	andElements := func(e Element) Element {
		boolList, err := e.BoolList()
		if err != nil {
			return nil
		}
		result := boolListElement{}
		and := true
		for _, e := range boolList {
			and = and && e
		}
		result.Set(and)
		return Element(&result)
	}

	// add constant (+5) to value (v)
	add5Int := func(e Element) Element {
		result := e.Copy()
		i, err := result.Int()
		if err != nil {
			return Element(&intElement{
				e:   +5,
				nan: false,
			})
		}
		result.Set(i + 5)
		return Element(result)
	}

	// sum all elements of int_list
	sumIntElements := func(e Element) Element {
		intList, err := e.IntList()
		if err != nil {
			return nil
		}
		result := intListElement{}
		sum := 0
		for _, e := range intList {
			sum = sum + e
		}
		result.Set(sum)
		return Element(&result)
	}

	// trim (XyZ) prefix from string
	trimXyZPrefix := func(e Element) Element {
		result := e.Copy()
		result.Set(strings.TrimPrefix(result.String(), "XyZ"))
		return Element(result)
	}

	concatStrings := func(e Element) Element {
		stringList := e.StringList()
		result := stringListElement{}
		concat := ""
		for _, e := range stringList {
			concat = concat + e
		}
		result.Set(concat)
		return Element(&result)
	}

	for testnum, test := range tests {
		switch test.series.Type() {
		case Bool:
			expected := test.expected
			received := test.series.Map(and)
			for i := 0; i < expected.Len(); i++ {
				e, _ := expected.Elem(i).Bool()
				r, _ := received.Elem(i).Bool()

				if e != r {
					t.Errorf(
						"Test:%v\nExpected:\n%v\nReceived:\n%v",
						testnum, expected, received,
					)
				}
			}

		case Float:
			expected := test.expected
			received := test.series.Map(doubleFloat64)
			for i := 0; i < expected.Len(); i++ {
				if !compareFloats(expected.Elem(i).Float(),
					received.Elem(i).Float(), 6) {
					t.Errorf(
						"Test:%v\nExpected:\n%v\nReceived:\n%v",
						testnum, expected, received,
					)
				}
			}

		case Int:
			expected := test.expected
			received := test.series.Map(add5Int)
			for i := 0; i < expected.Len(); i++ {
				e, _ := expected.Elem(i).Int()
				r, _ := received.Elem(i).Int()
				if e != r {
					t.Errorf(
						"Test:%v\nExpected:\n%v\nReceived:\n%v",
						testnum, expected, received,
					)
				}
			}

		case String:
			expected := test.expected
			received := test.series.Map(trimXyZPrefix)
			for i := 0; i < expected.Len(); i++ {
				if strings.Compare(expected.Elem(i).String(),
					received.Elem(i).String()) != 0 {
					t.Errorf(
						"Test:%v\nExpected:\n%v\nReceived:\n%v",
						testnum, expected, received,
					)
				}
			}

		case BoolList:
			expected := test.expected
			received := test.series.Map(andElements)
			isEquals, err := compareSeries(expected, received)
			if err != nil {
				t.Errorf("Test:%v\nError:%v", testnum, err)
			}
			if !isEquals {
				t.Errorf(
					"Test:%v\nExpected:\n%v\nReceived:\n%v",
					testnum, expected, received,
				)
			}

		case FloatList:
			expected := test.expected
			received := test.series.Map(maxFloat64)
			isEquals, err := compareSeries(expected, received)
			if err != nil {
				t.Errorf("Test:%v\nError:%v", testnum, err)
			}
			if !isEquals {
				t.Errorf(
					"Test:%v\nExpected:\n%v\nReceived:\n%v",
					testnum, expected, received,
				)
			}

		case IntList:
			expected := test.expected
			received := test.series.Map(sumIntElements)
			isEquals, err := compareSeries(expected, received)
			if err != nil {
				t.Errorf("Test:%v\nError:%v", testnum, err)
			}
			if !isEquals {
				t.Errorf(
					"Test:%v\nExpected:\n%v\nReceived:\n%v",
					testnum, expected, received,
				)
			}

		case StringList:
			expected := test.expected
			received := test.series.Map(concatStrings)
			isEquals, err := compareSeries(expected, received)
			if err != nil {
				t.Errorf("Test:%v\nError:%v", testnum, err)
			}
			if !isEquals {
				t.Errorf(
					"Test:%v\nExpected:\n%v\nReceived:\n%v",
					testnum, expected, received,
				)
			}

		default:
		}
	}
}

func TestSeries_Sum(t *testing.T) {
	tests := []struct {
		series   Series
		expected float64
	}{
		{
			// Extreme observations should not factor in.
			Ints([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 100, 1000, 10000}),
			11155,
		},
		{
			// Change in order should not influence result.
			Ints([]int{1, 2, 3, 10, 100, 1000, 10000, 4, 5, 6, 7, 8, 9}),
			11155,
		},
		{
			Floats([]float64{
				20.2755, 4.98964, -20.2006, 1.19854, 1.89977,
				1.51178, -17.4687, 4.65567, -8.65952, 6.31649,
			}),
			-5.481429999999998,
		},
		{
			Strings([]string{"A", "B", "C", "D"}),
			math.NaN(),
		},
		{
			Bools([]bool{true, true, false, true}),
			math.NaN(),
		},
		{
			Floats([]float64{}),
			math.NaN(),
		},

		{
			// Extreme observations should not factor in.
			IntsList([][]int{{1, 2, 3, 4, 5, 6, 7, 8, 9}, {10, 100, 1000, 10000}}),
			11155,
		},
		{
			// Change in order should not influence result.
			IntsList([][]int{{1, 2, 3, 10, 100, 1000, 10000}, {4, 5, 6, 7, 8, 9}}),
			11155,
		},
		{
			FloatsList([][]float64{
				{20.2755, 4.98964, -20.2006, 1.19854, 1.89977},
				{1.51178, -17.4687, 4.65567, -8.65952, 6.31649},
			}),
			-5.481429999999998,
		},
		{
			StringsList([][]string{{"A", "B"}, {"C", "D"}}),
			math.NaN(),
		},
		{
			BoolsList([][]bool{{true, true}, {false, true}}),
			math.NaN(),
		},
		{
			FloatsList([][]float64{{}}),
			math.NaN(),
		},
	}

	for testnum, test := range tests {
		received := test.series.Sum()
		expected := test.expected
		if !compareFloats(received, expected, 6) {
			t.Errorf(
				"Test:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, expected, received,
			)
		}
	}
}

func TestSeries_Slice(t *testing.T) {
	seriesWithErr := Ints([]int{})
	seriesWithErr.Err = fmt.Errorf("slice index out of bounds")

	tests := []struct {
		j        int
		k        int
		series   Series
		expected Series
	}{
		{
			0,
			3,
			Ints([]int{1, 2, 3, 4, 5}),
			Ints([]int{1, 2, 3}),
		},
		{
			1,
			1,
			Ints([]int{1, 2, 3, 4, 5}),
			Ints([]int{}),
		},
		{
			-1,
			1,
			Ints([]int{1, 2, 3, 4, 5}),
			seriesWithErr,
		},
		{
			1,
			5,
			Ints([]int{1, 2, 3, 4, 5}),
			Ints([]int{2, 3, 4, 5}),
		},
		{
			1,
			6,
			Ints([]int{1, 2, 3, 4, 5}),
			seriesWithErr,
		},
		{
			0,
			1,
			IntsList([][]int{{1, 2, 3}, {4, 5}}),
			IntsList([][]int{{1, 2, 3}}),
		},
		{
			1,
			1,
			IntsList([][]int{{1, 2, 3}, {4, 5}}),
			IntsList([][]int{}),
		},
		{
			-1,
			1,
			IntsList([][]int{{1, 2, 3}, {4, 5}}),
			seriesWithErr,
		},
		{
			0,
			5,
			IntsList([][]int{{1, 2, 3}, {4, 5}}),
			seriesWithErr,
		},

		{
			1,
			3,
			IntsList([][]int{{1, 2, 3}, {4, 5}, {6, 7}, {8, 9, 10}}),
			IntsList([][]int{{4, 5}, {6, 7}}),
		},
	}

	for testnum, test := range tests {
		expected := test.expected
		received := test.series.Slice(test.j, test.k)

		for i := 0; i < expected.Len(); i++ {
			if strings.Compare(expected.Elem(i).String(),
				received.Elem(i).String()) != 0 {
				t.Errorf(
					"Test:%v\nExpected:\n%v\nReceived:\n%v",
					testnum, expected, received,
				)
			}
		}

		if expected.Err != nil {
			if received.Err == nil || expected.Err.Error() != received.Err.Error() {
				t.Errorf(
					"Test:%v\nExpected error:\n%v\nReceived:\n%v",
					testnum, expected.Err, received.Err,
				)
			}
		}
	}
}

func TestSeries_Flatten(t *testing.T) {
	tests := []struct {
		series   Series
		expected Series
	}{
		{
			Strings([]string{"A", "B", "C"}),
			Strings([]string{"A", "B", "C"}),
		},
		{
			Ints([]int{1, 2, 3, 4, 5}),
			Ints([]int{1, 2, 3, 4, 5}),
		},
		{
			Floats([]float64{1.5, -3.23, -0.337397, -0.380079, 1.60979, 34.}),
			Floats([]float64{1.5, -3.23, -0.337397, -0.380079, 1.60979, 34.}),
		},
		{
			Bools([]bool{false, true, false}),
			Bools([]bool{false, true, false}),
		},
		{
			StringsList([][]string{{"A", "AA"}, {"B", "BB"}, {"C", "CC"}}),
			Strings([]string{"A", "AA", "B", "BB", "C", "CC"}),
		},
		{
			IntsList([][]int{{1, 11}, {3, 33}}),
			Ints([]int{1, 11, 3, 33}),
		},
		{
			FloatsList([][]float64{{1.5, -3.23, -0.337397}, {-0.380079, 1.60979, 34.}}),
			Floats([]float64{1.5, -3.23, -0.337397, -0.380079, 1.60979, 34.}),
		},
		{
			BoolsList([][]bool{{true, true}, {true, false}, {false, false}}),
			Bools([]bool{true, true, true, false, false, false}),
		},
		{
			IntsList([][]int{{1, 11}, nil, {3, 33}}),
			Ints([]int{1, 11, 3, 33}),
		},
		{
			New([][]string{{"1"}, {"A"}, {"3"}}, IntList, ""),
			Ints([]int{1, 3}),
		},
		{
			BoolsList([][]bool{{true, false}, nil, {false, true}}),
			Bools([]bool{true, false, false, true}),
		},
		{
			New([][]string{{"true", "t"}, {"ASD"}, {"false", "f"}}, BoolList, ""),
			Bools([]bool{true, true, false, false}),
		},

		{
			New([]interface{}{1, 2, 3, 4, 5, 6, 7, 8}, IntList, ""),
			Ints([]int{1, 2, 3, 4, 5, 6, 7, 8}),
		},
		{
			New([][]interface{}{{1, 2, 3, 4}, nil, {5, 6, 7, 8}}, IntList, ""),
			Ints([]int{1, 2, 3, 4, 5, 6, 7, 8}),
		},
		{
			New([][]interface{}{{"1"}, {"A"}, {"3"}}, IntList, ""),
			Ints([]int{1, 3}),
		},
		{
			New([][]interface{}{{"true", "t"}, {"ASD"}, {"false", "f"}}, BoolList, ""),
			Bools([]bool{true, true, false, false}),
		},
	}

	for testnum, test := range tests {
		expected := test.expected
		received := test.series.Flatten()

		if expected.Len() != received.Len() {
			t.Errorf(
				"Test:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, expected, received,
			)
		}

		isEquals, err := compareSeries(expected, received)
		if err != nil {
			t.Errorf("Test:%v\nError:%v", testnum, err)
		}
		if !isEquals {
			t.Errorf(
				"Test:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, expected, received,
			)
		}
	}
}

func TestSeries_Unique(t *testing.T) {
	tests := []struct {
		series   Series
		expected Series
	}{
		{
			Strings([]string{"A", "B", "C", "A", "B"}),
			Strings([]string{"A", "B", "C"}),
		},
		{
			Ints([]int{1, 2, 3, 4, 5, 1, 2, 3}),
			Ints([]int{1, 2, 3, 4, 5}),
		},
		{
			Floats([]float64{1.5, -3.23, 1.5, -3.23, -0.337397, -0.380079, 1.60979, 34.}),
			Floats([]float64{1.5, -3.23, -0.337397, -0.380079, 1.60979, 34.}),
		},
		{
			Bools([]bool{false, true, false, false, false, true}),
			Bools([]bool{false, true}),
		},
		{
			StringsList([][]string{{"A", "AA"}, {"B", "BB"}, {"C", "CC"}, {"C", "CC"}, {"C", "CC"}}),
			StringsList([][]string{{"A", "AA"}, {"B", "BB"}, {"C", "CC"}}),
		},
		{
			IntsList([][]int{{1, 11}, {3, 33}, {3, 33}}),
			IntsList([][]int{{1, 11}, {3, 33}}),
		},
		{
			FloatsList([][]float64{{1.5, -3.23, -0.337397}, {-0.380079, 1.60979, 34.}, {1.5, -3.23, -0.337397}, {-0.380079, 1.60979, 34.}}),
			FloatsList([][]float64{{1.5, -3.23, -0.337397}, {-0.380079, 1.60979, 34.}}),
		},
		{
			BoolsList([][]bool{{true, true}, {true, false}, {false, false}, {true, false}, {false, false}}),
			BoolsList([][]bool{{true, true}, {true, false}, {false, false}}),
		},
		{
			IntsList([][]int{{1, 11}, nil, {3, 33}, {1, 11}}),
			IntsList([][]int{{1, 11}, {3, 33}}),
		},
		{
			New([][]string{{"1"}, {"A"}, {"3"}, {"1"}, {"3"}}, IntList, ""),
			IntsList([][]int{{1}, {3}}),
		},
		{
			BoolsList([][]bool{{true, false}, nil, {false, true}, {false, true}}),
			BoolsList([][]bool{{true, false}, {false, true}}),
		},
		{
			New([][]string{{"true", "t"}, {"ASD"}, {"false", "f"}, {"QWE"}, {"true", "true"}}, BoolList, ""),
			BoolsList([][]bool{{true, true}, {false, false}}),
		},
	}

	for testnum, test := range tests {
		expected := test.expected
		received := test.series.Unique()

		if expected.Len() != received.Len() {
			t.Errorf(
				"Test:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, expected, received,
			)
		}

		for i := 0; i < expected.Len(); i++ {
			if strings.Compare(expected.Elem(i).String(),
				received.Elem(i).String()) != 0 {
				t.Errorf(
					"Test:%v\nExpected:\n%v\nReceived:\n%v",
					testnum, expected, received,
				)
			}
		}
	}
}

func TestSeries_SetMutualExclusiveValue(t *testing.T) {
	type args struct {
		indexes          Indexes
		excludingIndexes Indexes
		newvalues        Series
	}
	tests := []struct {
		name        string
		inputSeries Series
		args        args
		want        Series
	}{
		{
			name:        "all value in indexes and exluding indexes is mutually exclusive",
			inputSeries: Ints([]int{1, 2, 3, 4, 5}),
			args: args{
				indexes:          Bools([]bool{true, true, false, false, true}),
				excludingIndexes: Bools([]bool{false, false, false, false, false}),
				newvalues:        Ints([]int{2, 4, 6, 8, 10}),
			},
			want: Ints([]int{2, 4, 3, 4, 10}),
		},
		{
			name:        "some value in indexes is part of excluding indexes and those indexes is those modified ",
			inputSeries: Ints([]int{1, 2, 3, 4, 5}),
			args: args{
				indexes:          Bools([]bool{true, true, false, false, true}),
				excludingIndexes: Bools([]bool{true, false, false, false, true}),
				newvalues:        Ints([]int{2, 4, 6, 8, 10}),
			},
			want: Ints([]int{1, 4, 3, 4, 5}),
		},
		{
			name:        "one dimension new values",
			inputSeries: New([]interface{}{1, 2, 3, 4, nil}, Int, ""),
			args: args{
				indexes:          Bools([]bool{true, true, false, false, true}),
				excludingIndexes: Bools([]bool{true, true, true, true, false}),
				newvalues:        Ints([]int{-1}),
			},
			want: Ints([]int{1, 2, 3, 4, -1}),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.inputSeries.SetMutualExclusiveValue(tt.args.indexes, tt.args.excludingIndexes, tt.args.newvalues); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.SetMutualExclusiveValue() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_And(t *testing.T) {
	type args struct {
		rightValues interface{}
	}
	tests := []struct {
		name        string
		inputSeries Series
		args        args
		want        Series
	}{
		{
			name:        "and with single series",
			inputSeries: Bools([]bool{true, true, true, true}),
			args: args{
				rightValues: Bools([]bool{false, true, false, true}),
			},
			want: Bools([]bool{false, true, false, true}),
		},
		{
			name:        "and with single bool",
			inputSeries: Bools([]bool{true, true, true, true}),
			args: args{
				rightValues: false,
			},
			want: Bools([]bool{false, false, false, false}),
		},
		{
			name:        "and with multiple series",
			inputSeries: Bools([]bool{true, true, true, true}),
			args: args{
				rightValues: []Series{
					Bools([]bool{false, true, false, true}),
					Bools([]bool{false}),
				},
			},
			want: Bools([]bool{false, false, false, false}),
		},
		{
			name:        "and with multiple series; different dimensions",
			inputSeries: Bools([]bool{true, true, true, true}),
			args: args{
				rightValues: []Series{
					Bools([]bool{false, true, false, true}),
					Bools([]bool{false}),
					Bools([]bool{false, true, false}),
				},
			},
			want: Series{Err: fmt.Errorf("can't compare mismatch length"), t: Bool, elements: boolElements{}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.inputSeries.And(tt.args.rightValues); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.And() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_Or(t *testing.T) {
	type args struct {
		rightValues interface{}
	}
	tests := []struct {
		name        string
		inputSeries Series
		args        args
		want        Series
	}{
		{
			name:        "or with single series",
			inputSeries: Bools([]bool{false, false, false, true}),
			args: args{
				rightValues: Bools([]bool{false, true, false, true}),
			},
			want: Bools([]bool{false, true, false, true}),
		},
		{
			name:        "or with single bool",
			inputSeries: Bools([]bool{true, false, true, false}),
			args: args{
				rightValues: false,
			},
			want: Bools([]bool{true, false, true, false}),
		},
		{
			name:        "or with multiple series",
			inputSeries: Bools([]bool{false, false, false, true}),
			args: args{
				rightValues: []Series{
					Bools([]bool{false, true, false, true}),
					Bools([]bool{false}),
				},
			},
			want: Bools([]bool{false, true, false, true}),
		},
		{
			name:        "or with multiple series; different dimensions",
			inputSeries: Bools([]bool{true, true, true, true}),
			args: args{
				rightValues: []Series{
					Bools([]bool{false, true, false, true}),
					Bools([]bool{false}),
					Bools([]bool{false, true, false}),
				},
			},
			want: Series{Err: fmt.Errorf("can't compare mismatch length"), t: Bool, elements: boolElements{}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.inputSeries.Or(tt.args.rightValues); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.Or() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_XOr(t *testing.T) {
	type args struct {
		rightValues interface{}
	}
	tests := []struct {
		name        string
		inputSeries Series
		args        args
		want        Series
	}{
		{
			name:        "xor with single series",
			inputSeries: Bools([]bool{false, false, false, true}),
			args: args{
				rightValues: Bools([]bool{false, true, false, true}),
			},
			want: Bools([]bool{false, true, false, false}),
		},
		{
			name:        "xor with single bool",
			inputSeries: Bools([]bool{true, false, true, false}),
			args: args{
				rightValues: false,
			},
			want: Bools([]bool{true, false, true, false}),
		},
		{
			name:        "xor with multiple series",
			inputSeries: Bools([]bool{false, false, false, true}),
			args: args{
				rightValues: []Series{
					Bools([]bool{false, true, false, true}),
					Bools([]bool{false}),
				},
			},
			want: Bools([]bool{false, true, false, false}),
		},
		{
			name:        "xor with multiple series; different dimensions",
			inputSeries: Bools([]bool{true, true, true, true}),
			args: args{
				rightValues: []Series{
					Bools([]bool{false, true, false, true}),
					Bools([]bool{false}),
					Bools([]bool{false, true, false}),
				},
			},
			want: Series{Err: fmt.Errorf("can't compare mismatch length"), t: Bool, elements: boolElements{}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.inputSeries.XOr(tt.args.rightValues); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.XOr() = %v, want %v", got, tt.want)
			}
		})
	}
}
