package series

import (
	"fmt"
	"math"
	"strconv"
	"time"
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
	case time.Time:
		e.e = value.(time.Time).Format("01/02/2006")
	case Element:
		e.e = value.(Element).String()
	default:
		e.nan = true
		return
	}
	return
}

func (e stringElement) Copy() Element {
	if e.nan {
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

func (e stringElement) Type() Type {
	return String
}

func (e stringElement) Val() ElementValue {
	if e.IsNA() {
		return nil
	}
	return string(e.e)
}

func (e stringElement) String() string {
	if e.nan {
		return "NaN"
	}
	return string(e.e)
}

func (e stringElement) Int() (int, error) {
	if e.IsNA() {
		return 0, createErr("can't convert NaN to int", "stringElement.Int()")
	}
	return strconv.Atoi(e.e)
}

func (e stringElement) Float() float64 {
	if e.IsNA() {
		return math.NaN()
	}
	f, err := strconv.ParseFloat(e.e, 64)
	if err != nil {
		return math.NaN()
	}
	return f
}

func (e stringElement) Bool() (bool, error) {
	if e.IsNA() {
		return false, createErr("can't convert NaN to bool", "stringElement.Bool()")
	}
	return strconv.ParseBool(e.e)
}

func (e stringElement) Time() (time.Time, error) {
	t, err := time.Parse("01/02/2006", e.e)
	if err != nil {
		return time.Date(1, 1, 1, 0, 0, 0, 0, time.Local), createErr("%s", "stringElement.Time()", err)
	}
	return t, nil
}

func (e stringElement) Addr() string {
	return fmt.Sprint(&e)
}

func (e stringElement) Eq(elem Element) bool {
	if e.IsNA() || elem.IsNA() {
		return false
	}
	return e.e == elem.String()
}

func (e stringElement) Neq(elem Element) bool {
	if e.IsNA() || elem.IsNA() {
		return false
	}
	return e.e != elem.String()
}

func (e stringElement) Less(elem Element) bool {
	if e.IsNA() || elem.IsNA() {
		return false
	}
	return e.e < elem.String()
}

func (e stringElement) LessEq(elem Element) bool {
	if e.IsNA() || elem.IsNA() {
		return false
	}
	return e.e <= elem.String()
}

func (e stringElement) Greater(elem Element) bool {
	if e.IsNA() || elem.IsNA() {
		return false
	}
	return e.e > elem.String()
}

func (e stringElement) GreaterEq(elem Element) bool {
	if e.IsNA() || elem.IsNA() {
		return false
	}
	return e.e >= elem.String()
}
