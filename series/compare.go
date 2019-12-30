package series

import (
	"reflect"
)

var (
	Eq        Comparator = Comparator{Op: opEq}
	Neq       Comparator = Comparator{Op: opNeq}
	Less      Comparator = Comparator{Op: opLess}
	LessEq    Comparator = Comparator{Op: opLessEq}
	Greater   Comparator = Comparator{Op: opGreater}
	GreaterEq Comparator = Comparator{Op: opGreaterEq}
	In        Comparator = Comparator{Mode: OneToMany, Op: opIn}
)

func opEq(a Element, b interface{}) bool {
	if t, ok := b.(Element); ok {
		b = t.Value().Interface()
	}

	v := a.ConvertTo(reflect.TypeOf(b))
	switch b.(type) {
	case bool:
		return v.Bool() == reflect.ValueOf(b).Bool()
	case int:
		return v.Int() == reflect.ValueOf(b).Int()
	case float64:
		return v.Float() == reflect.ValueOf(b).Float()
	case string:
		return v.String() == reflect.ValueOf(b).String()
	default:
		return false
	}
}

func opNeq(a Element, b interface{}) bool {
	return !opEq(a, b)
}

func opLess(a Element, b interface{}) bool {
	if t, ok := b.(Element); ok {
		b = t.Value().Interface()
	}
	v := a.ConvertTo(reflect.TypeOf(b))
	switch b.(type) {
	case bool:
		return !v.Bool() && reflect.ValueOf(b).Bool()
	case int:
		return v.Int() < reflect.ValueOf(b).Int()
	case float64:
		return v.Float() < reflect.ValueOf(b).Float()
	case string:
		return v.String() < reflect.ValueOf(b).String()
	default:
		return false
	}
}

func opLessEq(a Element, b interface{}) bool {
	if t, ok := b.(Element); ok {
		b = t.Value().Interface()
	}
	v := a.ConvertTo(reflect.TypeOf(b))
	switch b.(type) {
	case bool:
		return !v.Bool() || reflect.ValueOf(b).Bool()
	case int:
		return v.Int() <= reflect.ValueOf(b).Int()
	case float64:
		return v.Float() <= reflect.ValueOf(b).Float()
	case string:
		return v.String() <= reflect.ValueOf(b).String()
	default:
		return false
	}
}

func opGreater(a Element, b interface{}) bool {
	return !opLessEq(a, b)
}

func opGreaterEq(a Element, b interface{}) bool {
	return !opLess(a, b)
}

func opIn(a Element, b interface{}) bool {
	if s, ok := b.(Series); ok {
		if s.Len() == 0 {
			return false
		}
		for i := 0; i < s.Len(); i++ {
			if opEq(a, s.Elem(i)) {
				return true
			}
		}
		return false
	}

	if e, ok := b.(Element); ok {
		return opEq(a, e)
	}

	switch reflect.TypeOf(b).Kind() {
	case reflect.Slice:
		for i := 0; i < reflect.ValueOf(b).Len(); i++ {
			if opEq(a, b) {
				return true
			}
		}
		return false
	default:
		return opEq(a, b)
	}
}
