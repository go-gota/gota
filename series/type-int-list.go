package series

import (
	"fmt"
	"math"
	"strconv"
)

type intListElement struct {
	e   []int
	nan bool
}

// force intListElement struct to implement Element interface
var _ Element = (*intListElement)(nil)

func (e *intListElement) Set(value interface{}) {
	e.nan = false
	switch val := value.(type) {
	case string:
		if val == "NaN" {
			e.nan = true
			return
		}
		i, err := strconv.Atoi(value.(string))
		if err != nil {
			e.nan = true
			return
		}
		e.e = make([]int, 1)
		e.e[0] = i
	case int:
		e.e = make([]int, 1)
		e.e[0] = val
	case int32:
		e.e = make([]int, 1)
		e.e[0] = int(val)
	case int64:
		e.e = make([]int, 1)
		e.e[0] = int(val)
	case float32:
		f := val
		if math.IsNaN(float64(f)) ||
			math.IsInf(float64(f), 0) ||
			math.IsInf(float64(f), 1) {
			e.nan = true
			return
		}
		e.e = make([]int, 1)
		e.e[0] = int(f)
	case float64:
		f := val
		if math.IsNaN(f) ||
			math.IsInf(f, 0) ||
			math.IsInf(f, 1) {
			e.nan = true
			return
		}
		e.e = make([]int, 1)
		e.e[0] = int(f)
	case bool:
		e.e = make([]int, 1)
		b := value.(bool)
		if b {
			e.e[0] = 1
		} else {
			e.e[0] = 0
		}
	case []string:
		if val == nil {
			e.nan = true
			return
		}
		l := len(val)
		e.e = make([]int, l)
		for i := 0; i < l; i++ {
			if val[i] == "NaN" {
				e.nan = true
				return
			}
			vi, err := strconv.Atoi(val[i])
			if err != nil {
				e.nan = true
				return
			}
			e.e[i] = vi
		}
	case []int:
		if val == nil {
			e.nan = true
			return
		}
		l := len(val)
		e.e = make([]int, l)
		for i := 0; i < l; i++ {
			e.e[i] = int(val[i])
		}
	case []int32:
		if val == nil {
			e.nan = true
			return
		}
		l := len(val)
		e.e = make([]int, l)
		for i := 0; i < l; i++ {
			e.e[i] = int(val[i])
		}
	case []int64:
		if val == nil {
			e.nan = true
			return
		}
		l := len(val)
		e.e = make([]int, l)
		for i := 0; i < l; i++ {
			e.e[i] = int(val[i])
		}
	case []float32:
		if val == nil {
			e.nan = true
			return
		}
		l := len(val)
		e.e = make([]int, l)
		for i := 0; i < l; i++ {
			f := val[i]
			if math.IsNaN(float64(f)) ||
				math.IsInf(float64(f), 0) ||
				math.IsInf(float64(f), 1) {
				e.nan = true
				return
			}
			e.e[i] = int(f)
		}
	case []float64:
		if val == nil {
			e.nan = true
			return
		}
		l := len(val)
		e.e = make([]int, l)
		for i := 0; i < l; i++ {
			f := val[i]
			if math.IsNaN(f) ||
				math.IsInf(f, 0) ||
				math.IsInf(f, 1) {
				e.nan = true
				return
			}
			e.e[i] = int(f)
		}
	case []bool:
		if val == nil {
			e.nan = true
			return
		}
		l := len(val)
		e.e = make([]int, l)
		for i := 0; i < l; i++ {
			b := val[i]
			if b {
				e.e[i] = 1
			} else {
				e.e[i] = 0
			}
		}
	case Element:
		v, err := val.IntList()
		if err != nil {
			e.nan = true
			return
		}
		e.e = v
	default:
		e.nan = true
		return
	}
}

func (e intListElement) Copy() Element {
	if e.IsNA() {
		return &intListElement{[]int{}, true}
	}
	return &intListElement{e.e, false}
}

func (e intListElement) IsNA() bool {
	return e.nan
}

func (e intListElement) Type() Type {
	return IntList
}

func (e intListElement) Val() ElementValue {
	if e.IsNA() {
		return nil
	}
	return e.e
}

func (e intListElement) String() string {
	if e.IsNA() {
		return "[NaN]"
	}
	return fmt.Sprint(e.e)
}

func (e intListElement) Int() (int, error) {
	if e.IsNA() {
		return 0, fmt.Errorf("can't convert NaN to int")
	}
	return 0, fmt.Errorf("can't convert []int to int")
}

func (e intListElement) Float() float64 {
	if e.IsNA() {
		return math.NaN()
	}
	return 0
}

func (e intListElement) Bool() (bool, error) {
	if e.IsNA() {
		return false, fmt.Errorf("can't convert NaN to bool")
	}
	return false, fmt.Errorf("can't convert []int to bool")
}

func (e intListElement) StringList() []string {
	if e.IsNA() {
		return []string{"NaN"}
	}

	l := make([]string, len(e.e))
	for i := 0; i < len(e.e); i++ {
		l[i] = fmt.Sprint(e.e[i])
	}
	return l
}

func (e intListElement) IntList() ([]int, error) {
	if e.IsNA() {
		return nil, fmt.Errorf("can't convert NaN to []int")
	}
	return e.e, nil
}

func (e intListElement) FloatList() []float64 {
	if e.IsNA() {
		return []float64{math.NaN()}
	}

	l := make([]float64, len(e.e))
	for i := 0; i < len(e.e); i++ {
		l[i] = float64(e.e[i])
	}
	return l
}

func (e intListElement) BoolList() ([]bool, error) {
	if e.IsNA() {
		return nil, fmt.Errorf("can't convert NaN to []bool")
	}

	l := make([]bool, len(e.e))
	for i := 0; i < len(e.e); i++ {
		if e.e[i] == 1 {
			l[i] = true
		} else {
			l[i] = false
		}
	}
	return l, nil
}

// For list element, it is considered to be equal when
// all of its value on the same index is equal.
func (e intListElement) Eq(elem Element) bool {
	if e.IsNA() || elem.IsNA() {
		return e.IsNA() == elem.IsNA()
	}

	list, err := elem.IntList()
	if err != nil {
		return false
	}

	if len(e.e) != len(list) {
		return false
	}

	for i := 0; i < len(e.e); i++ {
		if e.e[i] != list[i] {
			return false
		}
	}

	return true
}

func (e intListElement) Neq(elem Element) bool {
	if e.IsNA() || elem.IsNA() {
		return e.IsNA() != elem.IsNA()
	}

	list, err := elem.IntList()
	if err != nil {
		return false
	}

	if len(e.e) != len(list) {
		return true
	}

	count := 0
	for i := 0; i < len(e.e); i++ {
		if e.e[i] == list[i] {
			count = count + 1
		}
	}

	return count != len(e.e)
}

func (e intListElement) Less(elem Element) bool {
	list, err := elem.IntList()
	if err != nil {
		return false
	}

	if len(e.e) < len(list) {
		return true
	} else if len(e.e) > len(list) {
		return false
	}

	for i := 0; i < len(e.e); i++ {
		if e.e[i] >= list[i] {
			return false
		}
	}

	return true
}

func (e intListElement) LessEq(elem Element) bool {
	list, err := elem.IntList()
	if err != nil {
		return false
	}

	if len(e.e) < len(list) {
		return true
	} else if len(e.e) > len(list) {
		return false
	}

	for i := 0; i < len(e.e); i++ {
		if e.e[i] > list[i] {
			return false
		}
	}

	return true
}

func (e intListElement) Greater(elem Element) bool {
	list, err := elem.IntList()
	if err != nil {
		return false
	}

	if len(e.e) > len(list) {
		return true
	} else if len(e.e) < len(list) {
		return false
	}

	for i := 0; i < len(e.e); i++ {
		if e.e[i] <= list[i] {
			return false
		}
	}

	return true
}

func (e intListElement) GreaterEq(elem Element) bool {
	list, err := elem.IntList()
	if err != nil {
		return false
	}

	if len(e.e) > len(list) {
		return true
	} else if len(e.e) < len(list) {
		return false
	}

	for i := 0; i < len(e.e); i++ {
		if e.e[i] < list[i] {
			return false
		}
	}

	return true
}
