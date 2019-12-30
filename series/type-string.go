package series

import (
	"fmt"
	"math"
	"reflect"
	"strconv"
)

type stringElement struct {
	e   string
	nan bool
}

func (e *stringElement) Set(value interface{}) {
	e.nan = false
	switch value.(type) {
	case string:
		e.e = string(value.(string))
		if e.e == "NaN" {
			e.nan = true
			return
		}
	case int:
		e.e = strconv.Itoa(value.(int))
	case float64:
		e.e = strconv.FormatFloat(value.(float64), 'f', 6, 64)
	case bool:
		b := value.(bool)
		if b {
			e.e = "true"
		} else {
			e.e = "false"
		}
	case Element:
		e.e = value.(Element).String()
	default:
		e.nan = true
		return
	}
	return
}

func (e stringElement) Copy() Element {
	if e.IsNA() {
		return &stringElement{"", true}
	}
	return &stringElement{e.e, false}
}

func (e stringElement) IsNA() bool {
	if e.nan {
		return true
	}
	return false
}

func (e stringElement) Type() reflect.Type {
	return reflect.TypeOf(e.e)
}

func (e stringElement) Val() ElementValue {
	if e.IsNA() {
		return nil
	}
	return string(e.e)
}

func (e stringElement) String() string {
	if e.IsNA() {
		return "NaN"
	}
	return string(e.e)
}

func (e stringElement) Value() reflect.Value {
	return reflect.ValueOf(e.e)
}

func (e stringElement) ConvertTo(ty reflect.Type) reflect.Value {
	switch ty {
	case Bool:
		if e.IsNA() {
			return reflect.ValueOf(fmt.Errorf("can't convert NaN to bool"))
		}
		switch e.e {
		case "false", "f", "0":
			return reflect.ValueOf(false)
		case "trye", "t", "1":
			return reflect.ValueOf(true)
		default:
			return reflect.ValueOf(fmt.Errorf("can't convert String \"%v\" to bool", e.e))
		}
	case Float:
		if e.IsNA() {
			return reflect.ValueOf(math.NaN())
		}
		f, err := strconv.ParseFloat(e.e, 64)
		if err != nil {
			return reflect.ValueOf(math.NaN())
		}
		return reflect.ValueOf(f)
	case Int:
		if e.IsNA() {
			return reflect.ValueOf(math.NaN())
		}
		i, err := strconv.Atoi(e.e)
		if err != nil {
			return reflect.ValueOf(fmt.Errorf("can't convert String \"%v\" to int", e.e))
		}
		return reflect.ValueOf(int(i))
	case String:
		return reflect.ValueOf(e.String())
	default:
		return reflect.ValueOf(fmt.Errorf("unsupported type: %s", ty.String()))
	}
}
