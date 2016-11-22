package df

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
)

// Series is the main structure for a series of elements of the same type. It is
// the primary building block of a DataFrame.
type Series struct {
	Name     string             // The name of the series
	elements []elementInterface // The values of the elements
	t        Type               // The type of the series
	err      error
}

// Comparator is a comparator that can be used for filtering Series and DataFrames
type Comparator string

// Alias for Comparator operations
const (
	Eq        Comparator = "==" // Equal
	Neq                  = "!=" // Non equal
	Greater              = ">"  // Greater than
	GreaterEq            = ">=" // Greater or equal than
	Less                 = "<"  // Lesser than
	LessEq               = "<=" // Lesser or equal than
	In                   = "in" // Inside
)

// Type represents the type of the elements that can be stored on Series
type Type string

// Alias for the supported types of Series
const (
	String Type = "string"
	Int         = "int"
	Float       = "float"
	Bool        = "bool"
)

// NewSeries is the generic Series constructor
func NewSeries(values interface{}, t Type) Series {
	var elements []elementInterface
	ret := Series{
		Name:     "",
		elements: elements,
		t:        t,
	}
	ret.Append(values)
	return ret
}

// Strings is a constructor for a String series
func Strings(values interface{}) Series {
	return NewSeries(values, String)
}

// Ints is a constructor for an Int series
func Ints(values interface{}) Series {
	return NewSeries(values, Int)
}

// Floats is a constructor for a Float series
func Floats(values interface{}) Series {
	return NewSeries(values, Float)
}

// Bools is a constructor for a bools series
func Bools(values interface{}) Series {
	return NewSeries(values, Bool)
}

//// NamedStrings is a constructor for a named String series
//func NamedStrings(name string, args ...interface{}) Series {
//s := Strings(args...)
//s.Name = name
//return s
//}
//// NamedInts is a constructor for a named Int series
//func NamedInts(name string, args ...interface{}) Series {
//s := Ints(args...)
//s.Name = name
//return s
//}
//// NamedFloats is a constructor for a named Float series
//func NamedFloats(name string, args ...interface{}) Series {
//s := Floats(args...)
//s.Name = name
//return s
//}
//// NamedBools is a constructor for a named Bool series
//func NamedBools(name string, args ...interface{}) Series {
//s := Bools(args...)
//s.Name = name
//return s
//}

// Empty returns an empty Series of the same type
func (s Series) Empty() Series {
	var elements []elementInterface
	return Series{
		Name:     s.Name,
		t:        s.t,
		elements: elements,
	}
}

//func (s Series) set(i int, val elementValue) Series {
//if s.Err() != nil {
//return s
//}
//if i >= s.Len() || i < 0 {
//return Series{err: errors.New("Couldn't set element. Index out of bounds")}
//}
//elems, err := s.elements.Set(i, val)
//if err != nil {
//return Series{err: errors.New("Couldn't set element: " + err.Error())}
//}
//s.elements = elems
//return s
//}

//func (s Series) elem(i int) elementInterface {
//if i >= s.Len() || i < 0 {
//return nil
//}
//return s.elements.Elem(i)
//}

//// Val returns the value of a series for the given index
//func (s Series) Val(i int) (interface{}, error) {
//if i >= s.Len() || i < 0 {
//return nil, errors.New("index out of bounds")
//}
//elem := s.elements.Elem(i).Val()
//return elem, nil
//}

// Append appends elements to the end of the Series. The Series is modified in situ
func (s *Series) Append(values interface{}) {
	appendElements := func(val interface{}) error {
		var newelem elementInterface
		switch s.t {
		case String:
			newelem = stringElement{}
		case Int:
			newelem = intElement{}
		case Float:
			newelem = floatElement{}
		case Bool:
			newelem = boolElement{}
		default:
			return errors.New("can't create series, unknown type")
		}
		s.elements = append(s.elements, newelem.Set(val))
		return nil
	}
	if values == nil {
		appendElements(values)
	} else {
		switch reflect.TypeOf(values).Kind() {
		case reflect.Slice:
			v := reflect.ValueOf(values)
			for i := 0; i < v.Len(); i++ {
				val := v.Index(i).Interface()
				err := appendElements(val)
				if err != nil {
					s.err = err
					return
				}
			}
		default:
			v := reflect.ValueOf(values)
			val := v.Interface()
			switch val.(type) {
			case Series:
				for _, v := range val.(Series).elements {
					err := appendElements(v)
					if err != nil {
						s.err = err
						return
					}
				}
			default:
				err := appendElements(val)
				if err != nil {
					s.err = err
					return
				}
			}
		}
	}
}

// Concat concatenates two series together. It will return a new Series with the
// combined elements of both Series.
func (s Series) Concat(x Series) Series {
	y := s.Copy()
	y.Append(x)
	return y
}

func subsetIndexParse(l int, indexes interface{}) ([]int, error) {
	var idx []int
	switch indexes.(type) {
	case []int:
		idx = indexes.([]int)
	case int:
		idx = []int{indexes.(int)}
	case []bool:
		bools := indexes.([]bool)
		if len(bools) != l {
			return nil, errors.New("subsetting error: index dimensions mismatch")
		}
		for i, b := range bools {
			if b {
				idx = append(idx, i)
			}
		}
	case Series:
		s := indexes.(Series)
		if s.HasNaN() {
			return nil, errors.New("subsetting error: indexes contain NaN")
		}
		switch s.t {
		case Int:
			return s.Int()
		case Bool:
			bools, err := s.Bool()
			if err != nil {
				return nil, fmt.Errorf("subsetting error: %v", err)
			}
			return subsetIndexParse(l, bools)
		default:
			return nil, errors.New("subsetting error: unknown indexing mode")
		}
	default:
		return nil, errors.New("subsetting error: unknown indexing mode")
	}
	return idx, nil
}

// Subset returns a subset of the series based on the given indexes. Currently
// supports numeric indexes in the form of []int or int, boolean []bool and the
// respective Series of types Int/Bool.
func (s Series) Subset(indexes interface{}) Series {
	if s.Err() != nil {
		return s
	}

	idx, err := subsetIndexParse(s.Len(), indexes)
	if err != nil {
		s.err = err
		return s
	}

	var elements []elementInterface
	for _, i := range idx {
		if i < 0 || i >= s.Len() {
			s.err = errors.New("subsetting error: index out of range")
			return s
		}
		elements = append(elements, s.elements[i].Copy())
	}
	return Series{
		Name:     s.Name,
		t:        s.t,
		elements: elements,
	}
}

// HasNaN checks whether the Series contain NaN elements
func (s Series) HasNaN() bool {
	for _, e := range s.elements {
		if e.IsNA() {
			return true
		}
	}
	return false
}

//// Compare compares the values of a Series with other series, scalars, text, etc
//func (s Series) Compare(comparator Comparator, comparando interface{}) ([]bool, error) {
//var comp Series
//switch s.t {
//case String:
//comp = Strings(comparando)
//case Int:
//comp = Ints(comparando)
//case Float:
//comp = Floats(comparando)
//case Bool:
//comp = Bools(comparando)
//default:
//return nil, errors.New("Unknown Series type")
//}
//ret := []bool{}
//switch comparator {
//case Eq:
//if comp.Len() == 1 {
//for i := 0; i < s.Len(); i++ {
//ret = append(ret, s.elements.Elem(i).Eq(comp.elements.Elem(0)))
//}
//return ret, nil
//}
//if s.Len() != comp.Len() {
//return nil, errors.New("Can't compare Series: Different dimensions")
//}
//for i := 0; i < s.Len(); i++ {
//ret = append(ret, s.elements.Elem(i).Eq(comp.elements.Elem(i)))
//}
//case Neq:
//if comp.Len() == 1 {
//for i := 0; i < s.Len(); i++ {
//ret = append(ret, !s.elements.Elem(i).Eq(comp.elements.Elem(0)))
//}
//return ret, nil
//}
//if s.Len() != comp.Len() {
//return nil, errors.New("Can't compare Series: Different dimensions")
//}
//for i := 0; i < s.Len(); i++ {
//ret = append(ret, !s.elements.Elem(i).Eq(comp.elements.Elem(i)))
//}
//case Greater:
//if comp.Len() == 1 {
//for i := 0; i < s.Len(); i++ {
//ret = append(ret, s.elements.Elem(i).Greater(comp.elements.Elem(0)))
//}
//return ret, nil
//}
//if s.Len() != comp.Len() {
//return nil, errors.New("Can't compare Series: Different dimensions")
//}
//for i := 0; i < s.Len(); i++ {
//ret = append(ret, s.elements.Elem(i).Greater(comp.elements.Elem(i)))
//}
//case GreaterEq:
//if comp.Len() == 1 {
//for i := 0; i < s.Len(); i++ {
//ret = append(ret, s.elements.Elem(i).GreaterEq(comp.elements.Elem(0)))
//}
//return ret, nil
//}
//if s.Len() != comp.Len() {
//return nil, errors.New("Can't compare Series: Different dimensions")
//}
//for i := 0; i < s.Len(); i++ {
//ret = append(ret, s.elements.Elem(i).GreaterEq(comp.elements.Elem(i)))
//}
//case Less:
//if comp.Len() == 1 {
//for i := 0; i < s.Len(); i++ {
//ret = append(ret, s.elements.Elem(i).Less(comp.elements.Elem(0)))
//}
//return ret, nil
//}
//if s.Len() != comp.Len() {
//return nil, errors.New("Can't compare Series: Different dimensions")
//}
//for i := 0; i < s.Len(); i++ {
//ret = append(ret, s.elements.Elem(i).Less(comp.elements.Elem(i)))
//}
//case LessEq:
//if comp.Len() == 1 {
//for i := 0; i < s.Len(); i++ {
//ret = append(ret, s.elements.Elem(i).LessEq(comp.elements.Elem(0)))
//}
//return ret, nil
//}
//if s.Len() != comp.Len() {
//return nil, errors.New("Can't compare Series: Different dimensions")
//}
//for i := 0; i < s.Len(); i++ {
//ret = append(ret, s.elements.Elem(i).LessEq(comp.elements.Elem(i)))
//}
//case In:
//for i := 0; i < s.Len(); i++ {
//found := false
//for j := 0; j < comp.Len(); j++ {
//if s.elements.Elem(i).Eq(comp.elements.Elem(j)) {
//found = true
//break
//}
//}
//ret = append(ret, found)
//}
//default:
//return nil, errors.New("Unknown comparator")
//}
//return ret, nil
//}

// Copy wil copy the values of a given Series
func (s Series) Copy() Series {
	name := s.Name
	t := s.t
	var elements []elementInterface
	for _, e := range s.elements {
		elements = append(elements, e.Copy())
	}
	ret := Series{
		Name:     name,
		t:        t,
		elements: elements,
	}
	return ret
}

// Records returns the elements of a Series in a []string
func (s Series) Records() []string {
	var ret []string
	for _, e := range s.elements {
		ret = append(ret, e.String())
	}
	return ret
}

// Float returns the elements of a Series in a []float64. If the elements can not
// be converted to float64 or contains a NaN returns the float representation of
// NaN.
func (s Series) Float() []float64 {
	var ret []float64
	for _, e := range s.elements {
		val := e.ToFloat().Val()
		if val == nil {
			ret = append(ret, math.NaN())
		} else {
			ret = append(ret, val.(float64))
		}
	}
	return ret
}

// Int returns the elements of a Series in a []int or an error if NaN or can't be
// converted.
func (s Series) Int() ([]int, error) {
	var ret []int
	for _, e := range s.elements {
		val := e.ToInt().Val()
		if val == nil {
			return nil, errors.New("can't convert NaN to int")
		}
		ret = append(ret, val.(int))
	}
	return ret, nil
}

// Bool returns the elements of a Series in a []bool or an error if NaN or can't be
// converted.
func (s Series) Bool() ([]bool, error) {
	var ret []bool
	for _, e := range s.elements {
		val := e.ToBool().Val()
		if val == nil {
			return nil, errors.New("can't convert NaN to bool")
		}
		ret = append(ret, val.(bool))
	}
	return ret, nil
}

// Type returns the type of a given series
func (s Series) Type() Type {
	return s.t
}

// Len returns the length of a given Series
func (s Series) Len() int {
	return len(s.elements)
}

// String implements the Stringer interface for Series
func (s Series) String() string {
	return fmt.Sprint(s.elements)
}

// Str prints some extra information about a given series
func (s Series) Str() string {
	var ret []string
	// If name exists print name
	if s.Name != "" {
		ret = append(ret, "Name: "+s.Name)
	}
	ret = append(ret, "Type: "+fmt.Sprint(s.t))
	ret = append(ret, "Length: "+fmt.Sprint(s.Len()))
	if s.Len() != 0 {
		ret = append(ret, "Values: "+fmt.Sprint(s))
	}
	return strings.Join(ret, "\n")
}

// Err returns the error contained in the series
func (s Series) Err() error {
	return s.err
}

func addr(s Series) []string {
	var ret []string
	for _, e := range s.elements {
		ret = append(ret, e.Addr())
	}
	return ret
}
