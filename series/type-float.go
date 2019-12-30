package series

import (
	"fmt"
	"math"
	"reflect"
	"strconv"
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
		v := value.(Element).ConvertTo(Float)
		if v.Type() != Float {
			e.nan = true
		} else {
			e.e = v.Float()
		}
	default:
		e.nan = true
		return
	}
	return
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

func (e floatElement) Type() reflect.Type {
	return reflect.TypeOf(e.e)
}

func (e floatElement) Val() ElementValue {
	if e.IsNA() {
		return nil
	}
	return float64(e.e)
}

func (e floatElement) String() string {
	if e.IsNA() {
		return "NaN"
	}
	return fmt.Sprintf("%f", e.e)
}

func (e floatElement) Value() reflect.Value {
	return reflect.ValueOf(e.e)
}

func (e floatElement) ConvertTo(ty reflect.Type) reflect.Value {
	if e.IsNA() {
		return reflect.ValueOf(fmt.Errorf("can't convert NaN to %v", ty))
	}

	switch ty {
	case Int:
		f := e.e
		if math.IsInf(f, 1) || math.IsInf(f, -1) {
			return reflect.ValueOf(fmt.Errorf("can't convert Inf to int"))
		}
		if math.IsNaN(f) {
			return reflect.ValueOf(fmt.Errorf("can't convert NaN to int"))
		}
		return reflect.ValueOf(int(f))
	case Bool:
		switch e.e {
		case 0:
			return reflect.ValueOf(false)
		case 1:
			return reflect.ValueOf(true)
		default:
			return reflect.ValueOf(fmt.Errorf("can't convert Float \"%v\" to bool", e.e))
		}
	case Float:
		return reflect.ValueOf(float64(e.e))
	case String:
		return reflect.ValueOf(e.String())
	default:
		return reflect.ValueOf(fmt.Errorf("unsupported type: %s", ty.String()))
	}
}
