package series

import (
	"fmt"
	"math"
	"time"
)

type timeElement struct {
	e   time.Time
	nan bool
}

func (e *timeElement) Set(value interface{}) {
	e.nan = false
	switch value.(type) {
	case string:
		v := string(value.(string))
		var err error
		e.e, err = time.Parse("01/02/2006", v)
		if err != nil {
			e.e = time.Date(1, 1, 1, 0, 0, 0, 0, time.Local)
		}
		return
	case int, float64, bool:
		e.e = time.Date(1, 1, 1, 0, 0, 0, 0, nil)
	case time.Time:
		e.e = value.(time.Time)
	case Element:
		e.e, _ = value.(Element).Time()
	default:
		e.nan = true
		return
	}
	return
}

func (e timeElement) Copy() Element {
	if e.nan {
		return &timeElement{time.Date(1, 1, 1, 0, 0, 0, 0, time.Local), true}
	}
	return &timeElement{e.e, false}
}

func (e timeElement) IsNA() bool {
	if e.nan {
		return true
	}
	return false
}

func (e timeElement) Type() Type {
	return String
}

func (e timeElement) Val() ElementValue {
	if e.IsNA() {
		return nil
	}
	return e.e
}

func (e timeElement) String() string {
	if e.nan {
		return "NaN"
	}
	return e.e.Format("01/02/2006")
}

func (e timeElement) Int() (int, error) {
	if e.IsNA() {
		return 0, createErr("can't convert NaN to int", "timeElement.Int()")
	}
	return 0, createErr("can't convert Time to int", "timeElement.Int()")
}

func (e timeElement) Float() float64 {
	return math.NaN()
}

func (e timeElement) Bool() (bool, error) {
	if e.IsNA() {
		return false, createErr("can't convert NaN to bool", "timeElement.Bool()")
	}
	return false, createErr("can't convert Time to bool", "timeElement.Bool()")
}

func (e timeElement) Time() (time.Time, error) {
	return e.e, nil
}

func (e timeElement) Addr() string {
	return fmt.Sprint(&e)
}

func (e timeElement) Eq(elem Element) bool {
	if e.IsNA() || elem.IsNA() {
		return false
	}
	t, _ := elem.Time()
	return e.e.Equal(t)
}

func (e timeElement) Neq(elem Element) bool {
	if e.IsNA() || elem.IsNA() {
		return false
	}
	t, _ := elem.Time()
	return !e.e.Equal(t)
}

func (e timeElement) Less(elem Element) bool {
	if e.IsNA() || elem.IsNA() {
		return false
	}
	t, _ := elem.Time()
	return e.e.Before(t)
}

func (e timeElement) LessEq(elem Element) bool {
	if e.IsNA() || elem.IsNA() {
		return false
	}
	t, _ := elem.Time()
	if e.e.Equal(t) || e.e.Before(t) {
		return true
	}
	return false
}

func (e timeElement) Greater(elem Element) bool {
	if e.IsNA() || elem.IsNA() {
		return false
	}
	t, _ := elem.Time()
	return e.e.After(t)
}

func (e timeElement) GreaterEq(elem Element) bool {
	if e.IsNA() || elem.IsNA() {
		return false
	}
	t, _ := elem.Time()
	if e.e.Equal(t) || e.e.After(t) {
		return true
	}
	return false
}
