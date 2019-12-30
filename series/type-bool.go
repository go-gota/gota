package series

import (
	"fmt"
	"math"
	"reflect"
	"strings"
)

type boolElement struct {
	e   bool
	nan bool
}

func (e *boolElement) Set(value interface{}) {
	e.nan = false
	switch value.(type) {
	case string:
		if value.(string) == "NaN" {
			e.nan = true
			return
		}
		switch strings.ToLower(value.(string)) {
		case "true", "t", "1":
			e.e = true
		case "false", "f", "0":
			e.e = false
		default:
			e.nan = true
			return
		}
	case int:
		switch value.(int) {
		case 1:
			e.e = true
		case 0:
			e.e = false
		default:
			e.nan = true
			return
		}
	case float64:
		switch value.(float64) {
		case 1:
			e.e = true
		case 0:
			e.e = false
		default:
			e.nan = true
			return
		}
	case bool:
		e.e = value.(bool)
	case Element:
		if value.(Element).Value().Type().Kind() == reflect.Bool {
			e.e = value.(Element).Value().Bool()
		} else {
			v := value.(Element).ConvertTo(Bool)
			if v.Type() != Bool {
				e.nan = true
			} else {
				e.e = v.Bool()
			}
		}
	default:
		e.nan = true
		return
	}
	return
}

func (e boolElement) Copy() Element {
	if e.IsNA() {
		return &boolElement{false, true}
	}
	return &boolElement{e.e, false}
}

func (e boolElement) IsNA() bool {
	if e.nan {
		return true
	}
	return false
}

func (e boolElement) Type() reflect.Type {
	return reflect.TypeOf(e.e)
}

func (e boolElement) Val() ElementValue {
	if e.IsNA() {
		return nil
	}
	return bool(e.e)
}

func (e boolElement) String() string {
	if e.IsNA() {
		return "NaN"
	}
	if e.e {
		return "true"
	}
	return "false"
}

func (e boolElement) Value() reflect.Value {
	return reflect.ValueOf(e.e)
}

func (e boolElement) ConvertTo(ty reflect.Type) reflect.Value {
	switch ty {
	case Bool:
		if e.IsNA() {
			return reflect.ValueOf(fmt.Errorf("can't convert NaN to bool"))
		}
		return reflect.ValueOf(e.e)
	case Int:
		if e.IsNA() {
			return reflect.ValueOf(fmt.Errorf("can't convert NaN to int"))
		}
		if e.e == true {
			return reflect.ValueOf(1)
		}
		return reflect.ValueOf(0)
	case Float:
		if e.IsNA() {
			return reflect.ValueOf(math.NaN())
		}
		if e.e {
			return reflect.ValueOf(1.0)
		}
		return reflect.ValueOf(0.0)
	case String:
		return reflect.ValueOf(e.String())
	default:
		return reflect.ValueOf(fmt.Errorf("unsupported type: %s", ty.String()))
	}
}
