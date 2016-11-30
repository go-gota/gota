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
	ToString() stringElement
	ToInt() intElement
	ToFloat() floatElement
	ToBool() boolElement
	IsNA() bool
	Val() elementValue
	Set(interface{}) elementInterface
	Copy() elementInterface
	Type() Type
	Addr() string
	String() string
}

type elementValue interface{}

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
		return value.(elementInterface).ToString()
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
		return value.(elementInterface).ToInt()
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
		return value.(elementInterface).ToFloat()
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
		return value.(elementInterface).ToBool()
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

func (e stringElement) ToString() stringElement {
	return e.Copy().(stringElement)
}
func (e intElement) ToString() stringElement {
	if e.IsNA() {
		return stringElement{nil}
	}
	s := e.String()
	return stringElement{&s}
}
func (e floatElement) ToString() stringElement {
	if e.IsNA() {
		return stringElement{nil}
	}
	s := e.String()
	return stringElement{&s}
}
func (e boolElement) ToString() stringElement {
	if e.IsNA() {
		return stringElement{nil}
	}
	s := e.String()
	return stringElement{&s}
}

func (e stringElement) ToInt() intElement {
	if e.e == nil {
		return intElement{nil}
	}
	i, err := strconv.Atoi(*e.e)
	if err != nil {
		return intElement{nil}
	}
	if e.IsNA() {
		return intElement{nil}
	}
	return intElement{&i}
}
func (e intElement) ToInt() intElement {
	return e.Copy().(intElement)
}
func (e floatElement) ToInt() intElement {
	if e.e != nil {
		i := int(*e.e)
		return intElement{&i}
	}
	return intElement{nil}
}
func (e boolElement) ToInt() intElement {
	if e.e == nil {
		return intElement{nil}
	}
	var i int
	if *e.e {
		i = 1
	} else {
		i = 0
	}
	return intElement{&i}
}

func (e stringElement) ToFloat() floatElement {
	if e.e == nil {
		return floatElement{nil}
	}
	f, err := strconv.ParseFloat(*e.e, 64)
	if err != nil {
		return floatElement{nil}
	}
	return floatElement{&f}
}
func (e floatElement) ToFloat() floatElement {
	return e.Copy().(floatElement)
}
func (e intElement) ToFloat() floatElement {
	if e.e != nil {
		f := float64(*e.e)
		return floatElement{&f}
	}
	return floatElement{nil}
}
func (e boolElement) ToFloat() floatElement {
	if e.e == nil {
		return floatElement{nil}
	}
	var f float64
	if *e.e {
		f = 1.0
	} else {
		f = 0.0
	}
	return floatElement{&f}
}

func (e stringElement) ToBool() boolElement {
	if e.e == nil {
		return boolElement{nil}
	}
	var b bool
	switch strings.ToLower(*e.e) {
	case "true", "t", "1":
		b = true
	case "false", "f", "0":
		b = false
	default:
		return boolElement{nil}
	}
	return boolElement{&b}
}
func (e intElement) ToBool() boolElement {
	if e.e == nil {
		return boolElement{nil}
	}
	var b bool
	if *e.e == 1 {
		b = true
	}
	if *e.e == 0 {
		b = false
	}
	return boolElement{&b}
}
func (e floatElement) ToBool() boolElement {
	if e.e == nil {
		return boolElement{nil}
	}
	var b bool
	if *e.e == 1.0 {
		b = true
	} else if *e.e == 0.0 {
		b = false
	} else {
		return boolElement{nil}
	}
	return boolElement{&b}
}
func (e boolElement) ToBool() boolElement {
	return e.Copy().(boolElement)
}

func (e stringElement) LessEq(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e2 := elem.ToString()
	if e.IsNA() || e2.IsNA() {
		return false
	}
	return *e.e <= *e2.e
}
func (e intElement) LessEq(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e2 := elem.ToInt()
	if e.IsNA() || e2.IsNA() {
		return false
	}
	return *e.e <= *e2.e
}
func (e floatElement) LessEq(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e2 := elem.ToFloat()
	if e.IsNA() || e2.IsNA() {
		return false
	}
	return *e.e <= *e2.e
}
func (e boolElement) LessEq(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e2 := elem.ToBool()
	if e.IsNA() || e2.IsNA() {
		return false
	}
	if *e.e && !*e2.e {
		return false
	}
	return true
}

func (e stringElement) Less(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e2 := elem.ToString()
	if e.IsNA() || e2.IsNA() {
		return false
	}
	return *e.e < *e2.e
}
func (e intElement) Less(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e2 := elem.ToInt()
	if e.IsNA() || e2.IsNA() {
		return false
	}
	return *e.e < *e2.e
}
func (e floatElement) Less(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e2 := elem.ToFloat()
	if e.IsNA() || e2.IsNA() {
		return false
	}
	return *e.e < *e2.e
}
func (e boolElement) Less(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e2 := elem.ToBool()
	if e.IsNA() || e2.IsNA() {
		return false
	}
	if *e.e {
		return false
	}
	if *e2.e {
		return true
	}
	return false
}

func (e stringElement) GreaterEq(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e2 := elem.ToString()
	if e.IsNA() || e2.IsNA() {
		return false
	}
	return *e.e >= *e2.e
}
func (e intElement) GreaterEq(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e2 := elem.ToInt()
	if e.IsNA() || e2.IsNA() {
		return false
	}
	return *e.e >= *e2.e
}
func (e floatElement) GreaterEq(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e2 := elem.ToFloat()
	if e.IsNA() || e2.IsNA() {
		return false
	}
	return *e.e >= *e2.e
}
func (e boolElement) GreaterEq(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e2 := elem.ToBool()
	if e.IsNA() || e2.IsNA() {
		return false
	}
	if *e.e {
		return true
	}
	if *e2.e {
		return false
	}
	return true
}

func (e stringElement) Greater(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e2 := elem.ToString()
	if e.IsNA() || e2.IsNA() {
		return false
	}
	return *e.e > *e2.e
}
func (e intElement) Greater(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e2 := elem.ToInt()
	if e.IsNA() || e2.IsNA() {
		return false
	}
	return *e.e > *e2.e
}
func (e floatElement) Greater(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e2 := elem.ToFloat()
	if e.IsNA() || e2.IsNA() {
		return false
	}
	return *e.e > *e2.e
}
func (e boolElement) Greater(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e2 := elem.ToBool()
	if e.IsNA() || e2.IsNA() {
		return false
	}
	if *e.e && !*e2.e {
		return true
	}
	return false
}

func (e stringElement) Neq(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e2 := elem.ToString()
	if e.IsNA() || e2.IsNA() {
		return false
	}
	return *e.e != *e2.e
}

func (e intElement) Neq(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e2 := elem.ToInt()
	if e.IsNA() || e2.IsNA() {
		return false
	}
	return *e.e != *e2.e
}

func (e floatElement) Neq(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e2 := elem.ToFloat()
	if e.IsNA() || e2.IsNA() {
		return false
	}
	return *e.e != *e2.e
}

func (e boolElement) Neq(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e2 := elem.ToBool()
	if e.IsNA() || e2.IsNA() {
		return false
	}
	return *e.e != *e2.e
}

func (e stringElement) Eq(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e2 := elem.ToString()
	if e.IsNA() || e2.IsNA() {
		return false
	}
	return *e.e == *e2.e
}

func (e intElement) Eq(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e2 := elem.ToInt()
	if e.IsNA() || e2.IsNA() {
		return false
	}
	return *e.e == *e2.e
}

func (e floatElement) Eq(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e2 := elem.ToFloat()
	if e.IsNA() || e2.IsNA() {
		return false
	}
	return *e.e == *e2.e
}

func (e boolElement) Eq(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e2 := elem.ToBool()
	if e.IsNA() || e2.IsNA() {
		return false
	}
	return *e.e == *e2.e
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
