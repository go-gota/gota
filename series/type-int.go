package series

import (
	"fmt"
	"math"
	"strconv"
)

type intElement struct {
	e *int
}

func (e intElement) Set(value interface{}) elementInterface {
	var val int
	switch value.(type) {
	case string:
		i, err := strconv.Atoi(value.(string))
		if err != nil {
			e.e = nil
			return e
		}
		val = i
	case int:
		val = value.(int)
	case float64:
		f := value.(float64)
		if math.IsNaN(f) ||
			math.IsInf(f, 0) ||
			math.IsInf(f, 1) {
			e.e = nil
			return e
		}
		val = int(f)
	case bool:
		b := value.(bool)
		if b {
			val = 1
		} else {
			val = 0
		}
	case elementInterface:
		v, err := value.(elementInterface).Int()
		if err != nil {
			e.e = nil
			return e
		}
		val = v
	default:
		e.e = nil
		return e
	}
	e.e = &val
	return e
}

func (e intElement) Copy() elementInterface {
	if e.e == nil {
		return intElement{nil}
	}
	copy := *e.e
	return intElement{&copy}
}

func (e intElement) IsNA() bool {
	if e.e == nil {
		return true
	}
	return false
}

func (e intElement) Type() Type {
	return Int
}

func (e intElement) Val() ElementValue {
	if e.IsNA() {
		return nil
	}
	return *e.e
}

func (e intElement) String() string {
	if e.e == nil {
		return "NaN"
	}
	return fmt.Sprint(*e.e)
}

func (e intElement) Int() (int, error) {
	if e.IsNA() {
		return 0, fmt.Errorf("can't convert NaN to int")
	}
	return *e.e, nil
}

func (e intElement) Float() float64 {
	if e.IsNA() {
		return math.NaN()
	}
	return float64(*e.e)
}

func (e intElement) Bool() (bool, error) {
	if e.IsNA() {
		return false, fmt.Errorf("can't convert NaN to bool")
	}
	switch *e.e {
	case 1:
		return true, nil
	case 0:
		return false, nil
	}
	return false, fmt.Errorf("can't convert Int \"%v\" to bool", *e.e)
}

func (e intElement) Addr() string {
	return fmt.Sprint(e.e)
}

func (e intElement) Eq(elem elementInterface) bool {
	i, err := elem.Int()
	if err != nil || e.IsNA() {
		return false
	}
	return *e.e == i
}

func (e intElement) Neq(elem elementInterface) bool {
	i, err := elem.Int()
	if err != nil || e.IsNA() {
		return false
	}
	return *e.e != i
}

func (e intElement) Less(elem elementInterface) bool {
	i, err := elem.Int()
	if err != nil || e.IsNA() {
		return false
	}
	return *e.e < i
}

func (e intElement) LessEq(elem elementInterface) bool {
	i, err := elem.Int()
	if err != nil || e.IsNA() {
		return false
	}
	return *e.e <= i
}

func (e intElement) Greater(elem elementInterface) bool {
	i, err := elem.Int()
	if err != nil || e.IsNA() {
		return false
	}
	return *e.e > i
}

func (e intElement) GreaterEq(elem elementInterface) bool {
	i, err := elem.Int()
	if err != nil || e.IsNA() {
		return false
	}
	return *e.e >= i
}
