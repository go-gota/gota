package series

import (
	"fmt"
	"time"
)

// timeElement is the concrete implementation of the Element interface for
// time.Time. If the stored time.Time is zero, it will be considered as a NaN
// element.
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
		e.e, err = time.Parse(timeformat, v)
		if err != nil {
			e.e = time.Time{}
		}
		return
	case int, float64, bool:
		e.e = time.Time{}
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
		return &timeElement{time.Time{}, true}
	}
	return &timeElement{e.e, false}
}

func (e timeElement) IsNA() bool {
	return e.e.IsZero()
}

func (e timeElement) Type() Type {
	return Time
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
	return e.e.Format(timeformat)
}

func (e timeElement) Int() (int, error) {
	if e.IsNA() {
		return 0, fmt.Errorf("timeElement.Int(): can't convert NaN to int")
	}
	return int(e.e.Unix()), nil
}

func (e timeElement) Float() float64 {
	return float64(e.e.Unix())
}

func (e timeElement) Bool() (bool, error) {
	if e.IsNA() {
		return false, fmt.Errorf("timeElement.Bool(): can't convert NaN to bool")
	}
	return false, fmt.Errorf("timeElement.Bool(): can't convert Time to bool")
}

func (e timeElement) Time() (time.Time, error) {
	if e.IsNA() {
		return time.Time{}, fmt.Errorf("timeElement.Time(): can't convert NaN to time.Time")
	}
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
