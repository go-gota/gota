package series

import (
	"fmt"
	"math"
	"time"
)

type timeElement struct {
	e *time.Time
}

func (e timeElement) Addr() string {
	return fmt.Sprint(e.e)
}

func (e timeElement) Set(value interface{}) Element {
	var val time.Time
	var err error
	switch value.(type) {
	case string:
		if value.(string) == "NaN" {
			e.e = nil
			return e
		}
		val, err = ParseDateTime(value.(string))
		if err != nil {
			e.e = nil
			return e
		}
	case float64:
		val = time.Unix(0, int64(value.(float64)))
	case int:
		val = time.Unix(0, int64(value.(int)))
	case int64:
		val = time.Unix(0, value.(int64))
	case time.Time:
		val = value.(time.Time)
	case Element:
		val, err = value.(Element).Time()
		if err != nil {
			e.e = nil
			return e
		}

	default:
		e.e = nil
		return e
	}

	e.e = &val
	return e
}

func (e timeElement) Type() Type {
	return Time
}

func (e timeElement) IsNA() bool {
	if e.e == nil {
		return true
	}
	return false
}

func (e timeElement) Val() ElementValue {
	if e.IsNA() {
		return nil
	}
	return time.Time(*e.e)
}

func (e timeElement) Copy() Element {
	if e.e == nil {
		return timeElement{nil}
	}
	copy := time.Time(*e.e)
	return timeElement{&copy}
}

func (e timeElement) Bool() (bool, error) {
	return !e.IsNA(), nil
}

func (e timeElement) Int() (int, error) {
	if e.IsNA() {
		return 0, fmt.Errorf("can't convert NaN to int")
	}

	return int(e.e.UnixNano()), nil
}

func (e timeElement) Float() float64 {
	if e.e == nil {
		return math.NaN()
	}
	return float64(e.e.UnixNano())
}

func (e timeElement) String() string {
	if e.e == nil {
		return "NaN"
	}
	return time.Time(*e.e).String()
}

func (e timeElement) Time() (time.Time, error) {
	if e.IsNA() {
		return time.Time{}, fmt.Errorf("value is NaN")
	}
	return *e.e, nil
}

func (e timeElement) Eq(elem Element) bool {
	t, err := elem.Time()
	if e.IsNA() || err != nil {
		return false
	}

	return e.e.Equal(t)
}

func (e timeElement) Neq(elem Element) bool {
	t, err := elem.Time()
	if e.IsNA() || err != nil {
		return false
	}

	return !e.e.Equal(t)
}

func (e timeElement) Less(elem Element) bool {
	t, err := elem.Time()
	if e.IsNA() || err != nil {
		return false
	}
	return (*e.e).Before(t)
}

func (e timeElement) LessEq(elem Element) bool {
	t, err := elem.Time()
	if e.IsNA() || err != nil {
		return false
	}
	return (*e.e).Before(t) || e.Eq(elem)
}

func (e timeElement) Greater(elem Element) bool {
	t, err := elem.Time()
	if e.IsNA() || err != nil {
		return false
	}
	return (*e.e).After(t)
}

func (e timeElement) GreaterEq(elem Element) bool {
	t, err := elem.Time()
	if e.IsNA() || err != nil {
		return false
	}
	return (*e.e).After(t) || e.Eq(elem)
}
