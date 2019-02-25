package series

import (
	"fmt"
	"math"
	"strconv"
)

type float32Element struct {
	e   float32
	nan bool
}

func (e *float32Element) Set(value interface{}) {
	e.nan = false
	switch value.(type) {
	case string:
		if value.(string) == "NaN" {
			e.nan = true
			return
		}
		f, err := strconv.ParseFloat(value.(string), 32)
		if err != nil {
			e.nan = true
			return
		}
		e.e = float32(f)
	case int:
		e.e = float32(value.(int))
	case float64:
		e.e = float32(value.(float64))
	case float32:
		e.e = value.(float32)
	case bool:
		b := value.(bool)
		if b {
			e.e = 1
		} else {
			e.e = 0
		}
	case Element:
		e.e = float32(value.(Element).Float())
	default:
		e.nan = true
		return
	}
	return
}

func (e float32Element) Copy() Element {
	if e.IsNA() {
		return &float32Element{0.0, true}
	}
	return &float32Element{e.e, false}
}

func (e float32Element) IsNA() bool {
	if e.nan || math32.IsNaN(e.e) {
		return true
	}
	return false
}

func (e float32Element) Type() Type {
	return Float32
}

func (e float32Element) Val() ElementValue {
	if e.IsNA() {
		return nil
	}
	return float64(e.e)
}

func (e float32Element) String() string {
	if e.IsNA() {
		return "NaN"
	}
	return fmt.Sprintf("%f", e.e)
}

func (e float32Element) Int() (int, error) {
	if e.IsNA() {
		return 0, fmt.Errorf("can't convert NaN to int")
	}
	f := e.e
	if math32.IsInf(f, 1) || math32.IsInf(f, -1) {
		return 0, fmt.Errorf("can't convert Inf to int")
	}
	if math32.IsNaN(f) {
		return 0, fmt.Errorf("can't convert NaN to int")
	}
	return int(f), nil
}

func (e float32Element) Float() float64 {
	if e.IsNA() {
		return math.NaN()
	}
	return float64(e.e)
}

func (e float32Element) Float32() float32 {
	if e.IsNA() {
		return math32.NaN()
	}
	return float32(e.e)
}

func (e float32Element) Bool() (bool, error) {
	if e.IsNA() {
		return false, fmt.Errorf("can't convert NaN to bool")
	}
	switch e.e {
	case 1:
		return true, nil
	case 0:
		return false, nil
	}
	return false, fmt.Errorf("can't convert Float \"%v\" to bool", e.e)
}

func (e float32Element) Eq(elem Element) bool {
	f := elem.Float32()
	if e.IsNA() || math32.IsNaN(f) {
		return false
	}
	return e.e == f
}

func (e float32Element) Neq(elem Element) bool {
	f := elem.Float32()
	if e.IsNA() || math32.IsNaN(f) {
		return false
	}
	return e.e != f
}

func (e float32Element) Less(elem Element) bool {
	f := elem.Float32()
	if e.IsNA() || math32.IsNaN(f) {
		return false
	}
	return e.e < f
}

func (e float32Element) LessEq(elem Element) bool {
	f := elem.Float32()
	if e.IsNA() || math32.IsNaN(f) {
		return false
	}
	return e.e <= f
}

func (e float32Element) Greater(elem Element) bool {
	f := elem.Float32()
	if e.IsNA() || math32.IsNaN(f) {
		return false
	}
	return e.e > f
}

func (e float32Element) GreaterEq(elem Element) bool {
	f := elem.Float32()
	if e.IsNA() || math32.IsNaN(f) {
		return false
	}
	return e.e >= f
}
