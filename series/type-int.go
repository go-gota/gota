package series

import (
	"fmt"
	"math"
	"reflect"
	"strconv"
)

type intElement struct {
	e   int
	nan bool
}

func (e *intElement) Set(value interface{}) {
	e.nan = false
	switch value.(type) {
	case string:
		if value.(string) == "NaN" {
			e.nan = true
			return
		}
		i, err := strconv.Atoi(value.(string))
		if err != nil {
			e.nan = true
			return
		}
		e.e = i
	case int:
		e.e = int(value.(int))
	case float64:
		f := value.(float64)
		if math.IsNaN(f) ||
			math.IsInf(f, 0) ||
			math.IsInf(f, 1) {
			e.nan = true
			return
		}
		e.e = int(f)
	case bool:
		b := value.(bool)
		if b {
			e.e = 1
		} else {
			e.e = 0
		}
	case Element:
		v := value.(Element).ConvertTo(Int)
		if v.Type() != Int {
			e.nan = true
		} else {
			e.e = int(v.Int())
		}
	default:
		e.nan = true
		return
	}
	return
}

func (e intElement) Copy() Element {
	if e.IsNA() {
		return &intElement{0, true}
	}
	return &intElement{e.e, false}
}

func (e intElement) IsNA() bool {
	if e.nan {
		return true
	}
	return false
}

func (e intElement) Type() reflect.Type {
	return reflect.TypeOf(e.e)
}

func (e intElement) Val() ElementValue {
	if e.IsNA() {
		return nil
	}
	return int(e.e)
}

func (e intElement) String() string {
	if e.IsNA() {
		return "NaN"
	}
	return fmt.Sprint(e.e)
}

func (e intElement) Value() reflect.Value {
	return reflect.ValueOf(e.e)
}

func (e intElement) ConvertTo(ty reflect.Type) reflect.Value {
	switch ty {
	case Bool:
		if e.IsNA() {
			return reflect.ValueOf(fmt.Errorf("can't convert NaN to %v", ty))
		}
		switch e.e {
		case 0:
			return reflect.ValueOf(false)
		case 1:
			return reflect.ValueOf(true)
		default:
			return reflect.ValueOf(fmt.Errorf("can't convert Int \"%v\" to bool", e.e))
		}
	case Int:
		if e.IsNA() {
			return reflect.ValueOf(fmt.Errorf("can't convert NaN to %v", ty))
		}
		return reflect.ValueOf(e.e)
	case Float:
		if e.IsNA() {
			return reflect.ValueOf(math.NaN())
		}
		return reflect.ValueOf(float64(e.e))
	case String:
		return reflect.ValueOf(e.String())
	default:
		return reflect.ValueOf(fmt.Errorf("unsupported type: %s", ty.String()))
	}
}
