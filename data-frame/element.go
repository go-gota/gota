package df

import (
	"fmt"
	"strconv"
)

// String is an alias for string to be able to implement custom methods
type String struct {
	s *string
}

// Int is an alias for int to be able to implement custom methods
type Int struct {
	i *int
}

// Float is an alias for float64 to be able to implement custom methods
type Float struct {
	f *float64
}

// Bool is an alias for string to be able to implement custom methods
type Bool struct {
	b *bool
}

type Element interface {
	Eq(Element) bool
	ToString() String
	ToInt() Int
	ToFloat() Float
	ToBool() Bool
	IsNA() bool
	Val() ElementValue
}

type ElementValue interface{}

func (e String) Val() ElementValue {
	if e.IsNA() {
		return nil
	}
	return *e.s
}
func (e Int) Val() ElementValue {
	if e.IsNA() {
		return nil
	}
	return *e.i
}
func (e Float) Val() ElementValue {
	if e.IsNA() {
		return nil
	}
	return *e.f
}
func (e Bool) Val() ElementValue {
	if e.IsNA() {
		return nil
	}
	return *e.b
}

func (s String) ToString() String {
	return s.Copy()
}
func (i Int) ToString() String {
	if i.IsNA() {
		return String{nil}
	}
	s := i.String()
	return String{&s}
}
func (f Float) ToString() String {
	if f.IsNA() {
		return String{nil}
	}
	s := f.String()
	return String{&s}
}
func (b Bool) ToString() String {
	if b.IsNA() {
		return String{nil}
	}
	s := b.String()
	return String{&s}
}

func (s String) ToInt() Int {
	if s.s == nil {
		return Int{nil}
	}
	i, err := strconv.Atoi(*s.s)
	if err != nil {
		return Int{nil}
	}
	if s.IsNA() {
		return Int{nil}
	}
	return Int{&i}
}
func (i Int) ToInt() Int {
	return i.Copy()
}
func (f Float) ToInt() Int {
	if f.f != nil {
		i := int(*f.f)
		return Int{&i}
	}
	return Int{nil}
}
func (b Bool) ToInt() Int {
	if b.b == nil {
		return Int{nil}
	}
	var i int
	if *b.b {
		i = 1
	} else {
		i = 0
	}
	return Int{&i}
}

func (s String) ToFloat() Float {
	if s.s == nil {
		return Float{nil}
	}
	f, err := strconv.ParseFloat(*s.s, 64)
	if err != nil {
		return Float{nil}
	}
	return Float{&f}
}
func (i Float) ToFloat() Float {
	return i.Copy()
}
func (i Int) ToFloat() Float {
	if i.i != nil {
		f := float64(*i.i)
		return Float{&f}
	}
	return Float{nil}
}
func (b Bool) ToFloat() Float {
	if b.b == nil {
		return Float{nil}
	}
	var f float64
	if *b.b {
		f = 1.0
	} else {
		f = 0.0
	}
	return Float{&f}
}

func (s String) ToBool() Bool {
	if s.s == nil {
		return Bool{nil}
	}
	var b bool
	if *s.s == "false" {
		b = false
	}
	if *s.s == "true" {
		b = true
	}
	return Bool{&b}
}
func (i Int) ToBool() Bool {
	if i.i == nil {
		return Bool{nil}
	}
	var b bool
	if *i.i == 1 {
		b = true
	}
	if *i.i == 0 {
		b = false
	}
	return Bool{&b}
}
func (f Float) ToBool() Bool {
	if f.f == nil {
		return Bool{nil}
	}
	var b bool
	if *f.f == 1.0 {
		b = true
	} else if *f.f == 0.0 {
		b = false
	} else {
		return Bool{nil}
	}
	return Bool{&b}
}
func (i Bool) ToBool() Bool {
	return i.Copy()
}

func (s String) Eq(elem Element) bool {
	if elem == nil {
		return false
	}
	e := elem.ToString()
	if s.IsNA() || e.IsNA() {
		return false
	}
	return *s.s == *e.s
}

func (s Int) Eq(elem Element) bool {
	if elem == nil {
		return false
	}
	e := elem.ToInt()
	if s.IsNA() || e.IsNA() {
		return false
	}
	return *s.i == *e.i
}

func (s Float) Eq(elem Element) bool {
	if elem == nil {
		return false
	}
	e := elem.ToFloat()
	if s.IsNA() || e.IsNA() {
		return false
	}
	return *s.f == *e.f
}

func (s Bool) Eq(elem Element) bool {
	if elem == nil {
		return false
	}
	e := elem.ToBool()
	if s.IsNA() || e.IsNA() {
		return false
	}
	return *s.b == *e.b
}

func (s String) String() string {
	if s.s == nil {
		return "NA"
	}
	return *s.s
}
func (i Int) String() string {
	if i.i == nil {
		return "NA"
	}
	return fmt.Sprint(*i.i)
}
func (f Float) String() string {
	if f.f == nil {
		return "NA"
	}
	return fmt.Sprint(*f.f)
}
func (b Bool) String() string {
	if b.b == nil {
		return "NA"
	}
	if *b.b {
		return "true"
	}
	return "false"
}

func (s String) Copy() String {
	if s.s == nil {
		return String{nil}
	}
	copy := *s.s
	return String{&copy}
}

func (i Int) Copy() Int {
	if i.i == nil {
		return Int{nil}
	}
	copy := *i.i
	return Int{&copy}
}

func (f Float) Copy() Float {
	if f.f == nil {
		return Float{nil}
	}
	copy := *f.f
	return Float{&copy}
}

func (b Bool) Copy() Bool {
	if b.b == nil {
		return Bool{nil}
	}
	copy := *b.b
	return Bool{&copy}
}

// IsNA returns true if the element is empty and viceversa
func (s String) IsNA() bool {
	if s.s == nil {
		return true
	}
	return false
}

// IsNA returns true if the element is empty and viceversa
func (i Int) IsNA() bool {
	if i.i == nil {
		return true
	}
	return false
}

// IsNA returns true if the element is empty and viceversa
func (f Float) IsNA() bool {
	if f.f == nil {
		return true
	}
	return false
}

// IsNA returns true if the element is empty and viceversa
func (b Bool) IsNA() bool {
	if b.b == nil {
		return true
	}
	return false
}
