package df

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// String is an alias for string to be able to implement custom methods
type stringElement struct {
	s *string
}

// Int is an alias for int to be able to implement custom methods
type intElement struct {
	i *int
}

// Float is an alias for float64 to be able to implement custom methods
type floatElement struct {
	f *float64
}

// Bool is an alias for string to be able to implement custom methods
type boolElement struct {
	b *bool
}

type elementInterface interface {
	Eq(elementInterface) bool
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
}

type elementValue interface{}

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
		e.s = nil
		return e
	}
	e.s = &val
	return e
}

func (e intElement) Set(value interface{}) elementInterface {
	var val int
	switch value.(type) {
	case string:
		i, err := strconv.Atoi(value.(string))
		if err != nil {
			e.i = nil
			return e
		} else {
			val = i
		}
	case int:
		val = value.(int)
	case float64:
		f := value.(float64)
		if math.IsNaN(f) ||
			math.IsInf(f, 0) ||
			math.IsInf(f, 1) {
			e.i = nil
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
		e.i = nil
		return e
	}
	e.i = &val
	return e
}

func (e floatElement) Set(value interface{}) elementInterface {
	var val float64
	switch value.(type) {
	case string:
		f, err := strconv.ParseFloat(value.(string), 64)
		if err != nil {
			e.f = nil
			return e
		} else {
			val = f
		}
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
		e.f = nil
		return e
	}
	e.f = &val
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
			e.b = nil
			return e
		}
	case int:
		switch value.(int) {
		case 1:
			val = true
		case 0:
			val = false
		default:
			e.b = nil
			return e
		}
	case float64:
		switch value.(float64) {
		case 1:
			val = true
		case 0:
			val = false
		default:
			e.b = nil
			return e
		}
	case bool:
		val = value.(bool)
	case elementInterface:
		return value.(elementInterface).ToBool()
	default:
		e.b = nil
		return e
	}
	e.b = &val
	return e
}

func (e stringElement) Val() elementValue {
	if e.IsNA() {
		return nil
	}
	return *e.s
}
func (e intElement) Val() elementValue {
	if e.IsNA() {
		return nil
	}
	return *e.i
}
func (e floatElement) Val() elementValue {
	if e.IsNA() {
		return nil
	}
	return *e.f
}
func (e boolElement) Val() elementValue {
	if e.IsNA() {
		return nil
	}
	return *e.b
}

func (s stringElement) ToString() stringElement {
	return s.Copy().(stringElement)
}
func (i intElement) ToString() stringElement {
	if i.IsNA() {
		return stringElement{nil}
	}
	s := i.String()
	return stringElement{&s}
}
func (f floatElement) ToString() stringElement {
	if f.IsNA() {
		return stringElement{nil}
	}
	s := f.String()
	return stringElement{&s}
}
func (b boolElement) ToString() stringElement {
	if b.IsNA() {
		return stringElement{nil}
	}
	s := b.String()
	return stringElement{&s}
}

func (s stringElement) ToInt() intElement {
	if s.s == nil {
		return intElement{nil}
	}
	i, err := strconv.Atoi(*s.s)
	if err != nil {
		return intElement{nil}
	}
	if s.IsNA() {
		return intElement{nil}
	}
	return intElement{&i}
}
func (i intElement) ToInt() intElement {
	return i.Copy().(intElement)
}
func (f floatElement) ToInt() intElement {
	if f.f != nil {
		i := int(*f.f)
		return intElement{&i}
	}
	return intElement{nil}
}
func (b boolElement) ToInt() intElement {
	if b.b == nil {
		return intElement{nil}
	}
	var i int
	if *b.b {
		i = 1
	} else {
		i = 0
	}
	return intElement{&i}
}

func (s stringElement) ToFloat() floatElement {
	if s.s == nil {
		return floatElement{nil}
	}
	f, err := strconv.ParseFloat(*s.s, 64)
	if err != nil {
		return floatElement{nil}
	}
	return floatElement{&f}
}
func (i floatElement) ToFloat() floatElement {
	return i.Copy().(floatElement)
}
func (i intElement) ToFloat() floatElement {
	if i.i != nil {
		f := float64(*i.i)
		return floatElement{&f}
	}
	return floatElement{nil}
}
func (b boolElement) ToFloat() floatElement {
	if b.b == nil {
		return floatElement{nil}
	}
	var f float64
	if *b.b {
		f = 1.0
	} else {
		f = 0.0
	}
	return floatElement{&f}
}

func (s stringElement) ToBool() boolElement {
	if s.s == nil {
		return boolElement{nil}
	}
	var b bool
	switch strings.ToLower(*s.s) {
	case "true", "t", "1":
		b = true
	case "false", "f", "0":
		b = false
	default:
		return boolElement{nil}
	}
	return boolElement{&b}
}
func (i intElement) ToBool() boolElement {
	if i.i == nil {
		return boolElement{nil}
	}
	var b bool
	if *i.i == 1 {
		b = true
	}
	if *i.i == 0 {
		b = false
	}
	return boolElement{&b}
}
func (f floatElement) ToBool() boolElement {
	if f.f == nil {
		return boolElement{nil}
	}
	var b bool
	if *f.f == 1.0 {
		b = true
	} else if *f.f == 0.0 {
		b = false
	} else {
		return boolElement{nil}
	}
	return boolElement{&b}
}
func (i boolElement) ToBool() boolElement {
	return i.Copy().(boolElement)
}

func (s stringElement) LessEq(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e := elem.ToString()
	if s.IsNA() || e.IsNA() {
		return false
	}
	return *s.s <= *e.s
}
func (s intElement) LessEq(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e := elem.ToInt()
	if s.IsNA() || e.IsNA() {
		return false
	}
	return *s.i <= *e.i
}
func (s floatElement) LessEq(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e := elem.ToFloat()
	if s.IsNA() || e.IsNA() {
		return false
	}
	return *s.f <= *e.f
}
func (s boolElement) LessEq(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e := elem.ToBool()
	if s.IsNA() || e.IsNA() {
		return false
	}
	if *s.b && !*e.b {
		return false
	}
	return true
}

func (s stringElement) Less(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e := elem.ToString()
	if s.IsNA() || e.IsNA() {
		return false
	}
	return *s.s < *e.s
}
func (s intElement) Less(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e := elem.ToInt()
	if s.IsNA() || e.IsNA() {
		return false
	}
	return *s.i < *e.i
}
func (s floatElement) Less(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e := elem.ToFloat()
	if s.IsNA() || e.IsNA() {
		return false
	}
	return *s.f < *e.f
}
func (s boolElement) Less(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e := elem.ToBool()
	if s.IsNA() || e.IsNA() {
		return false
	}
	if *s.b {
		return false
	}
	if *e.b {
		return true
	}
	return false
}

func (s stringElement) GreaterEq(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e := elem.ToString()
	if s.IsNA() || e.IsNA() {
		return false
	}
	return *s.s >= *e.s
}
func (s intElement) GreaterEq(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e := elem.ToInt()
	if s.IsNA() || e.IsNA() {
		return false
	}
	return *s.i >= *e.i
}
func (s floatElement) GreaterEq(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e := elem.ToFloat()
	if s.IsNA() || e.IsNA() {
		return false
	}
	return *s.f >= *e.f
}
func (s boolElement) GreaterEq(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e := elem.ToBool()
	if s.IsNA() || e.IsNA() {
		return false
	}
	if *s.b {
		return true
	}
	if *e.b {
		return false
	}
	return true
}

func (s stringElement) Greater(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e := elem.ToString()
	if s.IsNA() || e.IsNA() {
		return false
	}
	return *s.s > *e.s
}
func (s intElement) Greater(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e := elem.ToInt()
	if s.IsNA() || e.IsNA() {
		return false
	}
	return *s.i > *e.i
}
func (s floatElement) Greater(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e := elem.ToFloat()
	if s.IsNA() || e.IsNA() {
		return false
	}
	return *s.f > *e.f
}
func (s boolElement) Greater(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e := elem.ToBool()
	if s.IsNA() || e.IsNA() {
		return false
	}
	if *s.b && !*e.b {
		return true
	}
	return false
}

func (s stringElement) Eq(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e := elem.ToString()
	if s.IsNA() || e.IsNA() {
		return false
	}
	return *s.s == *e.s
}

func (s intElement) Eq(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e := elem.ToInt()
	if s.IsNA() || e.IsNA() {
		return false
	}
	return *s.i == *e.i
}

func (s floatElement) Eq(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e := elem.ToFloat()
	if s.IsNA() || e.IsNA() {
		return false
	}
	return *s.f == *e.f
}

func (s boolElement) Eq(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e := elem.ToBool()
	if s.IsNA() || e.IsNA() {
		return false
	}
	return *s.b == *e.b
}

func (s stringElement) String() string {
	if s.s == nil {
		return "NaN"
	}
	return *s.s
}
func (i intElement) String() string {
	if i.i == nil {
		return "NaN"
	}
	return fmt.Sprint(*i.i)
}
func (f floatElement) String() string {
	if f.f == nil {
		return "NaN"
	}
	return fmt.Sprintf("%f", *f.f)
}
func (b boolElement) String() string {
	if b.b == nil {
		return "NaN"
	}
	if *b.b {
		return "true"
	}
	return "false"
}

func (s stringElement) Copy() elementInterface {
	if s.s == nil {
		return stringElement{nil}
	}
	copy := *s.s
	return stringElement{&copy}
}

func (i intElement) Copy() elementInterface {
	if i.i == nil {
		return intElement{nil}
	}
	copy := *i.i
	return intElement{&copy}
}

func (f floatElement) Copy() elementInterface {
	if f.f == nil {
		return floatElement{nil}
	}
	copy := *f.f
	return floatElement{&copy}
}

func (b boolElement) Copy() elementInterface {
	if b.b == nil {
		return boolElement{nil}
	}
	copy := *b.b
	return boolElement{&copy}
}

// IsNA returns true if the element is empty and viceversa
func (s stringElement) IsNA() bool {
	if s.s == nil {
		return true
	}
	return false
}

// IsNA returns true if the element is empty and viceversa
func (i intElement) IsNA() bool {
	if i.i == nil {
		return true
	}
	return false
}

// IsNA returns true if the element is empty and viceversa
func (f floatElement) IsNA() bool {
	if f.f == nil {
		return true
	}
	return false
}

// IsNA returns true if the element is empty and viceversa
func (b boolElement) IsNA() bool {
	if b.b == nil {
		return true
	}
	return false
}
