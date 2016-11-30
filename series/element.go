package series

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// String is an alias for string to be able to implement custom methods
type stringElement struct {
	e *string
}

// Int is an alias for int to be able to implement custom methods
type intElement struct {
	e *int
}

// Float is an alias for float64 to be able to implement custom methods
type floatElement struct {
	e *float64
}

// Bool is an alias for string to be able to implement custom methods
type boolElement struct {
	e *bool
}

type elementInterface interface {
	Eq(elementInterface) bool
	Neq(elementInterface) bool
	Less(elementInterface) bool
	LessEq(elementInterface) bool
	Greater(elementInterface) bool
	GreaterEq(elementInterface) bool
	IsNA() bool
	Val() elementValue
	Set(interface{}) elementInterface
	Copy() elementInterface
	Type() Type
	Addr() string
	String() string
	Int() (int, error)
	Float() float64
	Bool() (bool, error)
}

type elementValue interface{}

func (e stringElement) Float() float64 {
	if e.IsNA() {
		return math.NaN()
	}
	f, _ := strconv.ParseFloat(*e.e, 64)
	return f
}
func (e intElement) Float() float64 {
	if e.IsNA() {
		return math.NaN()
	}
	return float64(*e.e)
}
func (e floatElement) Float() float64 {
	if e.IsNA() {
		return math.NaN()
	}
	return *e.e
}
func (e boolElement) Float() float64 {
	if e.IsNA() {
		return math.NaN()
	}
	if *e.e {
		return 1
	}
	return 0
}

func (e stringElement) Int() (int, error) {
	if e.IsNA() {
		return 0, fmt.Errorf("can't convert NaN to int")
	}
	return strconv.Atoi(*e.e)
}
func (e intElement) Int() (int, error) {
	if e.IsNA() {
		return 0, fmt.Errorf("can't convert NaN to int")
	}
	return *e.e, nil
}
func (e floatElement) Int() (int, error) {
	if e.IsNA() {
		return 0, fmt.Errorf("can't convert NaN to int")
	}
	f := *e.e
	if math.IsInf(f, 1) || math.IsInf(f, -1) {
		return 0, fmt.Errorf("can't convert Inf to int")
	}
	if math.IsNaN(f) {
		return 0, fmt.Errorf("can't convert NaN to int")
	}
	return int(f), nil
}
func (e boolElement) Int() (int, error) {
	if e.IsNA() {
		return 0, fmt.Errorf("can't convert NaN to int")
	}
	if *e.e == true {
		return 1, nil
	}
	return 0, nil
}

func (e stringElement) Bool() (bool, error) {
	if e.IsNA() {
		return false, fmt.Errorf("can't convert NaN to bool")
	}
	switch strings.ToLower(*e.e) {
	case "true", "t", "1":
		return true, nil
	case "false", "f", "0":
		return false, nil
	}
	return false, fmt.Errorf("can't convert String \"%v\" to bool", *e.e)
}
func (e intElement) Bool() (bool, error) {
	if e.IsNA() {
		return false, fmt.Errorf("can't convert NaN to bool")
	}
	switch *e.e {
	case 1:
		return true, nil
	case 0:
		return false, nil
	}
	return false, fmt.Errorf("can't convert Int \"%v\" to bool", *e.e)
}
func (e floatElement) Bool() (bool, error) {
	if e.IsNA() {
		return false, fmt.Errorf("can't convert NaN to bool")
	}
	switch *e.e {
	case 1:
		return true, nil
	case 0:
		return false, nil
	}
	return false, fmt.Errorf("can't convert Float \"%v\" to bool", *e.e)
}
func (e boolElement) Bool() (bool, error) {
	if e.IsNA() {
		return false, fmt.Errorf("can't convert NaN to bool")
	}
	return *e.e, nil
}

func (e stringElement) Addr() string {
	return fmt.Sprint(e.e)
}
func (e intElement) Addr() string {
	return fmt.Sprint(e.e)
}
func (e floatElement) Addr() string {
	return fmt.Sprint(e.e)
}
func (e boolElement) Addr() string {
	return fmt.Sprint(e.e)
}
func (e stringElement) Type() Type {
	return String
}
func (e intElement) Type() Type {
	return Int
}
func (e floatElement) Type() Type {
	return Float
}
func (e boolElement) Type() Type {
	return Bool
}

func (e stringElement) Set(value interface{}) elementInterface {
	var val string
	switch value.(type) {
	case string:
		val = value.(string)
	case int:
		val = strconv.Itoa(value.(int))
	case float64:
		val = strconv.FormatFloat(value.(float64), 'f', 6, 64)
	case bool:
		b := value.(bool)
		if b {
			val = "true"
		} else {
			val = "false"
		}
	case elementInterface:
		val = value.(elementInterface).String()
	default:
		e.e = nil
		return e
	}
	e.e = &val
	return e
}

func (e intElement) Set(value interface{}) elementInterface {
	var val int
	switch value.(type) {
	case string:
		i, err := strconv.Atoi(value.(string))
		if err != nil {
			e.e = nil
			return e
		}
		val = i
	case int:
		val = value.(int)
	case float64:
		f := value.(float64)
		if math.IsNaN(f) ||
			math.IsInf(f, 0) ||
			math.IsInf(f, 1) {
			e.e = nil
			return e
		}
		val = int(f)
	case bool:
		b := value.(bool)
		if b {
			val = 1
		} else {
			val = 0
		}
	case elementInterface:
		v, err := value.(elementInterface).Int()
		if err != nil {
			e.e = nil
			return e
		}
		val = v
	default:
		e.e = nil
		return e
	}
	e.e = &val
	return e
}

func (e floatElement) Set(value interface{}) elementInterface {
	var val float64
	switch value.(type) {
	case string:
		f, err := strconv.ParseFloat(value.(string), 64)
		if err != nil {
			e.e = nil
			return e
		}
		val = f
	case int:
		val = float64(value.(int))
	case float64:
		val = value.(float64)
	case bool:
		b := value.(bool)
		if b {
			val = 1
		} else {
			val = 0
		}
	case elementInterface:
		val = value.(elementInterface).Float()
	default:
		e.e = nil
		return e
	}
	e.e = &val
	return e
}

func (e boolElement) Set(value interface{}) elementInterface {
	var val bool
	switch value.(type) {
	case string:
		switch strings.ToLower(value.(string)) {
		case "true", "t", "1":
			val = true
		case "false", "f", "0":
			val = false
		default:
			e.e = nil
			return e
		}
	case int:
		switch value.(int) {
		case 1:
			val = true
		case 0:
			val = false
		default:
			e.e = nil
			return e
		}
	case float64:
		switch value.(float64) {
		case 1:
			val = true
		case 0:
			val = false
		default:
			e.e = nil
			return e
		}
	case bool:
		val = value.(bool)
	case elementInterface:
		b, err := value.(elementInterface).Bool()
		if err != nil {
			e.e = nil
			return e
		}
		val = b
	default:
		e.e = nil
		return e
	}
	e.e = &val
	return e
}

func (e stringElement) Val() elementValue {
	if e.IsNA() {
		return nil
	}
	return *e.e
}
func (e intElement) Val() elementValue {
	if e.IsNA() {
		return nil
	}
	return *e.e
}
func (e floatElement) Val() elementValue {
	if e.IsNA() {
		return nil
	}
	return *e.e
}
func (e boolElement) Val() elementValue {
	if e.IsNA() {
		return nil
	}
	return *e.e
}

func (e stringElement) LessEq(elem elementInterface) bool {
	if e.IsNA() || elem.IsNA() {
		return false
	}
	return *e.e <= elem.String()
}
func (e intElement) LessEq(elem elementInterface) bool {
	i, err := elem.Int()
	if err != nil || e.IsNA() {
		return false
	}
	return *e.e <= i
}
func (e floatElement) LessEq(elem elementInterface) bool {
	f := elem.Float()
	if e.IsNA() || math.IsNaN(f) {
		return false
	}
	return *e.e <= f
}
func (e boolElement) LessEq(elem elementInterface) bool {
	b, err := elem.Bool()
	if err != nil || e.IsNA() {
		return false
	}
	return !*e.e || b
}

func (e stringElement) Less(elem elementInterface) bool {
	if e.IsNA() || elem.IsNA() {
		return false
	}
	return *e.e < elem.String()
}
func (e intElement) Less(elem elementInterface) bool {
	i, err := elem.Int()
	if err != nil || e.IsNA() {
		return false
	}
	return *e.e < i
}
func (e floatElement) Less(elem elementInterface) bool {
	f := elem.Float()
	if e.IsNA() || math.IsNaN(f) {
		return false
	}
	return *e.e < f
}
func (e boolElement) Less(elem elementInterface) bool {
	b, err := elem.Bool()
	if err != nil || e.IsNA() {
		return false
	}
	return !*e.e && b
}

func (e stringElement) GreaterEq(elem elementInterface) bool {
	if e.IsNA() || elem.IsNA() {
		return false
	}
	return *e.e >= elem.String()
}
func (e intElement) GreaterEq(elem elementInterface) bool {
	i, err := elem.Int()
	if err != nil || e.IsNA() {
		return false
	}
	return *e.e >= i
}
func (e floatElement) GreaterEq(elem elementInterface) bool {
	f := elem.Float()
	if e.IsNA() || math.IsNaN(f) {
		return false
	}
	return *e.e >= f
}
func (e boolElement) GreaterEq(elem elementInterface) bool {
	b, err := elem.Bool()
	if err != nil || e.IsNA() {
		return false
	}
	return *e.e || !b
}

func (e stringElement) Greater(elem elementInterface) bool {
	if e.IsNA() || elem.IsNA() {
		return false
	}
	return *e.e > elem.String()
}
func (e intElement) Greater(elem elementInterface) bool {
	i, err := elem.Int()
	if err != nil || e.IsNA() {
		return false
	}
	return *e.e > i
}
func (e floatElement) Greater(elem elementInterface) bool {
	f := elem.Float()
	if e.IsNA() || math.IsNaN(f) {
		return false
	}
	return *e.e > f
}
func (e boolElement) Greater(elem elementInterface) bool {
	b, err := elem.Bool()
	if err != nil || e.IsNA() {
		return false
	}
	return *e.e && !b
}

func (e stringElement) Neq(elem elementInterface) bool {
	if e.IsNA() || elem.IsNA() {
		return false
	}
	return *e.e != elem.String()
}

func (e intElement) Neq(elem elementInterface) bool {
	i, err := elem.Int()
	if err != nil || e.IsNA() {
		return false
	}
	return *e.e != i
}

func (e floatElement) Neq(elem elementInterface) bool {
	f := elem.Float()
	if e.IsNA() || math.IsNaN(f) {
		return false
	}
	return *e.e != f
}

func (e boolElement) Neq(elem elementInterface) bool {
	b, err := elem.Bool()
	if err != nil || e.IsNA() {
		return false
	}
	return *e.e != b
}

func (e stringElement) Eq(elem elementInterface) bool {
	if e.IsNA() || elem.IsNA() {
		return false
	}
	return *e.e == elem.String()
}

func (e intElement) Eq(elem elementInterface) bool {
	i, err := elem.Int()
	if err != nil || e.IsNA() {
		return false
	}
	return *e.e == i
}

func (e floatElement) Eq(elem elementInterface) bool {
	f := elem.Float()
	if e.IsNA() || math.IsNaN(f) {
		return false
	}
	return *e.e == f
}

func (e boolElement) Eq(elem elementInterface) bool {
	b, err := elem.Bool()
	if err != nil || e.IsNA() {
		return false
	}
	return *e.e == b
}

func (e stringElement) String() string {
	if e.e == nil {
		return "NaN"
	}
	return *e.e
}
func (e intElement) String() string {
	if e.e == nil {
		return "NaN"
	}
	return fmt.Sprint(*e.e)
}
func (e floatElement) String() string {
	if e.e == nil {
		return "NaN"
	}
	return fmt.Sprintf("%f", *e.e)
}
func (e boolElement) String() string {
	if e.e == nil {
		return "NaN"
	}
	if *e.e {
		return "true"
	}
	return "false"
}

func (e stringElement) Copy() elementInterface {
	if e.e == nil {
		return stringElement{nil}
	}
	copy := *e.e
	return stringElement{&copy}
}

func (e intElement) Copy() elementInterface {
	if e.e == nil {
		return intElement{nil}
	}
	copy := *e.e
	return intElement{&copy}
}

func (e floatElement) Copy() elementInterface {
	if e.e == nil {
		return floatElement{nil}
	}
	copy := *e.e
	return floatElement{&copy}
}

func (e boolElement) Copy() elementInterface {
	if e.e == nil {
		return boolElement{nil}
	}
	copy := *e.e
	return boolElement{&copy}
}

// IsNA returns true if the element is empty and viceversa
func (e stringElement) IsNA() bool {
	if e.e == nil {
		return true
	}
	return false
}

// IsNA returns true if the element is empty and viceversa
func (e intElement) IsNA() bool {
	if e.e == nil {
		return true
	}
	return false
}

// IsNA returns true if the element is empty and viceversa
func (e floatElement) IsNA() bool {
	if e.e == nil {
		return true
	}
	return false
}

// IsNA returns true if the element is empty and viceversa
func (e boolElement) IsNA() bool {
	if e.e == nil {
		return true
	}
	return false
}
