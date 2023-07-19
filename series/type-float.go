package series

import (
	"fmt"
	"math"
	"strconv"
)

type floatElement struct {
	e   float64
	nan bool
}

// force floatElement struct to implement Element interface
var _ Element = (*floatElement)(nil)

func (e *floatElement) Set(value interface{}) {
	switch val := value.(type) {
	case string:
		e.SetString(val)
	case int:
		e.SetInt(val)
	case float64:
		e.SetFloat(val)
	case bool:
		e.SetBool(val)
	case Element:
		e.SetElement(val)
	case FloatValuer:
		e.e = val.Float()
		e.nan = math.IsNaN(e.e)
	default:
		e.nan = true
	}
}

func (e *floatElement) SetElement(val Element) {
	e.nan = val.IsNA()
	e.e = val.Float()
}
func (e *floatElement) SetBool(val bool) {
	e.nan = false
	if val {
		e.e = 1
	} else {
		e.e = 0
	}
}
func (e *floatElement) SetFloat(val float64) {
	e.e = val
	if math.IsNaN(val) {
		e.nan = true
	} else {
		e.nan = false
	}
}
func (e *floatElement) SetInt(val int) {
	e.nan = false
	e.e = float64(val)
}
func (e *floatElement) SetString(val string) {
	e.nan = false
	if val == NaN {
		e.nan = true
		return
	}
	f, err := strconv.ParseFloat(val, 64)
	if err != nil {
		e.nan = true
		return
	}
	e.e = f
}

func (e floatElement) Copy() Element {
	if e.IsNA() {
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
	if e.IsNA() {
		return NaN
	}
	return fmt.Sprintf("%f", e.e)
}

func (e floatElement) Int() (int, error) {
	if e.IsNA() {
		return 0, fmt.Errorf("can't convert NaN to int")
	}
	f := e.e
	if math.IsInf(f, 1) || math.IsInf(f, -1) {
		return 0, fmt.Errorf("can't convert Inf to int")
	}
	if math.IsNaN(f) {
		return 0, fmt.Errorf("can't convert NaN to int")
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

// FloatValuer is the interface providing the Float method.
//
// Types implementing FloatValuer interface are able to convert
// themselves to a float Value.
type FloatValuer interface {
	// Float returns a float64 value.
	Float() float64
}
