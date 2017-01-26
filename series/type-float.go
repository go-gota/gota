package series

import (
	"fmt"
	"math"
	"strconv"
	"time"
)

type floatElement struct {
	e   float64
	nan bool
}

func (e *floatElement) Set(value interface{}) {
	e.nan = false
	switch value.(type) {
	case string:
		if value.(string) == "NaN" {
			e.nan = true
			return
		}
		f, err := strconv.ParseFloat(value.(string), 64)
		if err != nil {
			e.nan = true
			return
		}
		e.e = f
	case int:
		e.e = float64(value.(int))
	case float64:
		e.e = float64(value.(float64))
	case bool:
		b := value.(bool)
		if b {
			e.e = 1
		} else {
			e.e = 0
		}
	case Element:
		e.e = value.(Element).Float()
	default:
		e.nan = true
		return
	}
	return
}

func (e floatElement) Copy() Element {
	if e.nan {
		return &floatElement{0.0, true}
	}
	return &floatElement{e.e, false}
}

func (e floatElement) IsNA() bool {
	if e.nan || math.IsNaN(e.e) {
		return true
	}
	return false
}

func (e floatElement) Type() Type {
	return Float
}

func (e floatElement) Val() ElementValue {
	if e.IsNA() {
		return nil
	}
	return float64(e.e)
}

func (e floatElement) String() string {
	if e.nan {
		return "NaN"
	}
	return fmt.Sprintf("%f", e.e)
}

func (e floatElement) Int() (int, error) {
	if e.IsNA() {
		return 0, createErr("can't convert NaN to int", "floatElement.Int()")
	}
	f := e.e
	if math.IsInf(f, 1) || math.IsInf(f, -1) {
		return 0, createErr("can't convert Inf to int", "floatElement.Int()")
	}
	if math.IsNaN(f) {
		return 0, createErr("can't convert NaN to int", "floatElement.Int()")
	}
	return int(f), nil
}

func (e floatElement) Float() float64 {
	if e.IsNA() {
		return math.NaN()
	}
	return float64(e.e)
}

func (e floatElement) Bool() (bool, error) {
	if e.IsNA() {
		return false, createErr("can't convert NaN to bool", "floatElement.Bool()")
	}
	switch e.e {
	case 1:
		return true, nil
	case 0:
		return false, nil
	}
	return false, createErr("can't convert Float \"%v\" to bool", "floatElement.Bool()", e.e)
}

func (e floatElement) Time() (time.Time, error) {
	return time.Date(1, 1, 1, 0, 0, 0, 0, time.Local), createErr("can't convert float to time.Time", "floatElement.Time()")
}

func (e floatElement) Addr() string {
	return fmt.Sprint(&e)
}

func (e floatElement) Eq(elem Element) bool {
	f := elem.Float()
	if e.IsNA() || math.IsNaN(f) {
		return false
	}
	return e.e == f
}

func (e floatElement) Neq(elem Element) bool {
	f := elem.Float()
	if e.IsNA() || math.IsNaN(f) {
		return false
	}
	return e.e != f
}

func (e floatElement) Less(elem Element) bool {
	f := elem.Float()
	if e.IsNA() || math.IsNaN(f) {
		return false
	}
	return e.e < f
}

func (e floatElement) LessEq(elem Element) bool {
	f := elem.Float()
	if e.IsNA() || math.IsNaN(f) {
		return false
	}
	return e.e <= f
}

func (e floatElement) Greater(elem Element) bool {
	f := elem.Float()
	if e.IsNA() || math.IsNaN(f) {
		return false
	}
	return e.e > f
}

func (e floatElement) GreaterEq(elem Element) bool {
	f := elem.Float()
	if e.IsNA() || math.IsNaN(f) {
		return false
	}
	return e.e >= f
}
