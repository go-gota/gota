package df

import (
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

// Indexes represent the elements that can be used for selecting a subset of
// indexes. Currently supported are: []int, int, []bool, Series (Int/Bool)
type Indexes interface{}

// NewSeries is the generic Series constructor
func NewSeries(values interface{}, t Type, name string) Series {
	var elements []elementInterface
	ret := Series{
		Name:     name,
		elements: elements,
		t:        t,
	}
	ret.Append(values)
	return ret
}

// Strings is a constructor for a String series
func Strings(values interface{}) Series {
	return NewSeries(values, String, "")
}

// Ints is a constructor for an Int series
func Ints(values interface{}) Series {
	return NewSeries(values, Int, "")
}

// Floats is a constructor for a Float series
func Floats(values interface{}) Series {
	return NewSeries(values, Float, "")
}

// Bools is a constructor for a bools series
func Bools(values interface{}) Series {
	return NewSeries(values, Bool, "")
}

// Empty returns an empty Series of the same type
func (s Series) Empty() Series {
	var elements []elementInterface
	return Series{
		Name:     s.Name,
		t:        s.t,
		elements: elements,
	}
}

// FIXME: NOT NEEDED ANYMORE
func (s Series) elem(i int) elementInterface {
	if i >= s.Len() || i < 0 {
		return nil
	}
	return s.elements[i]
}

// FIXME: SHOULD NOT BE ALLOWED
// Val returns the value of a series for the given index
func (s Series) Val(i int) (interface{}, error) {
	if i >= s.Len() || i < 0 {
		return nil, fmt.Errorf("index out of bounds")
	}
	return s.elements[i].Val(), nil
}

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
			return fmt.Errorf("can't create series, unknown type")
		}
		s.elements = append(s.elements, newelem.Set(val))
		return nil
	}
	if values == nil {
		err := appendElements(values)
		if err != nil {
			s.err = err
		}
		return
	}
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

// Concat concatenates two series together. It will return a new Series with the
// combined elements of both Series.
func (s Series) Concat(x Series) Series {
	if err := s.Err(); err != nil {
		return s
	}
	if err := x.Err(); err != nil {
		s.err = fmt.Errorf("concat error: argument has errors: %v", err)
		return s
	}
	y := s.Copy()
	y.Append(x)
	return y
}

// Subset returns a subset of the series based on the given indexes. Currently
// supports numeric indexes in the form of []int or int, boolean []bool and the
// respective Series of types Int/Bool.
func (s Series) Subset(indexes Indexes) Series {
	if err := s.Err(); err != nil {
		return s
	}
	idx, err := parseIndexes(s.Len(), indexes)
	if err != nil {
		s.err = err
		return s
	}
	var elements []elementInterface
	for _, i := range idx {
		if i < 0 || i >= s.Len() {
			s.err = fmt.Errorf("subsetting error: index out of range")
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

// Set sets the values on the indexes of a Series and returns a new one with these
// modifications. The original Series does not change.
func (s Series) Set(indexes Indexes, newvalues Series) Series {
	if err := s.Err(); err != nil {
		return s
	}
	if err := newvalues.Err(); err != nil {
		s.err = fmt.Errorf("set error: argument has errors: %v", err)
		return s
	}
	idx, err := parseIndexes(s.Len(), indexes)
	if err != nil {
		s.err = err
		return s
	}
	if len(idx) != newvalues.Len() {
		s.err = fmt.Errorf("set error: dimensions mismatch")
		return s
	}
	ret := s.Copy()
	for k, i := range idx {
		if i < 0 || i >= s.Len() {
			s.err = fmt.Errorf("set error: index out of range")
			return s
		}
		ret.elements[i] = ret.elements[i].Set(newvalues.elements[k])
	}
	return ret
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

// Compare compares the values of a Series with other series, scalars, text, etc
func (s Series) Compare(comparator Comparator, comparando interface{}) Series {
	if err := s.Err(); err != nil {
		return s
	}
	compareElements := func(a, b elementInterface, c Comparator) (bool, error) {
		var ret bool
		switch c {
		case Eq:
			ret = a.Eq(b)
		case Neq:
			ret = a.Neq(b)
		case Greater:
			ret = a.Greater(b)
		case GreaterEq:
			ret = a.GreaterEq(b)
		case Less:
			ret = a.Less(b)
		case LessEq:
			ret = a.LessEq(b)
		default:
			return false, fmt.Errorf("unknown comparator: %v", c)
		}
		return ret, nil
	}

	comp := NewSeries(comparando, s.t, "")
	// In comparator comparation
	if comparator == In {
		var bools []bool
		for _, e := range s.elements {
			b := false
			for _, m := range comp.elements {
				c, err := compareElements(e, m, Eq)
				if err != nil {
					s = s.Empty()
					s.err = err
					return s
				}
				if c {
					b = true
					break
				}
			}
			bools = append(bools, b)
		}
		return Bools(bools)
	}

	// Single element comparation
	var bools []bool
	if comp.Len() == 1 {
		for _, e := range s.elements {
			c, err := compareElements(e, comp.elements[0], comparator)
			if err != nil {
				s = s.Empty()
				s.err = err
				return s
			}
			bools = append(bools, c)
		}
		return Bools(bools)
	}

	// Multiple element comparation
	if s.Len() != comp.Len() {
		s := s.Empty()
		s.err = fmt.Errorf("can't compare: length mismatch")
		return s
	}
	for k, e := range s.elements {
		c, err := compareElements(e, comp.elements[k], comparator)
		if err != nil {
			s = s.Empty()
			s.err = err
			return s
		}
		bools = append(bools, c)
	}
	return Bools(bools)
}

// Copy wil copy the values of a given Series
func (s Series) Copy() Series {
	name := s.Name
	t := s.t
	err := s.err
	var elements []elementInterface
	for _, e := range s.elements {
		elements = append(elements, e.Copy())
	}
	ret := Series{
		Name:     name,
		t:        t,
		elements: elements,
		err:      err,
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
			return nil, fmt.Errorf("can't convert NaN to int")
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
			return nil, fmt.Errorf("can't convert NaN to bool")
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

func parseIndexes(l int, indexes interface{}) ([]int, error) {
	var idx []int
	switch indexes.(type) {
	case []int:
		idx = indexes.([]int)
	case int:
		idx = []int{indexes.(int)}
	case []bool:
		bools := indexes.([]bool)
		if len(bools) != l {
			return nil, fmt.Errorf("indexing error: index dimensions mismatch")
		}
		for i, b := range bools {
			if b {
				idx = append(idx, i)
			}
		}
	case Series:
		s := indexes.(Series)
		if err := s.Err(); err != nil {
			return nil, fmt.Errorf("indexing error: new values has errors: %v", err)
		}
		if s.HasNaN() {
			return nil, fmt.Errorf("indexing error: indexes contain NaN")
		}
		switch s.t {
		case Int:
			return s.Int()
		case Bool:
			bools, err := s.Bool()
			if err != nil {
				return nil, fmt.Errorf("indexing error: %v", err)
			}
			return parseIndexes(l, bools)
		default:
			return nil, fmt.Errorf("indexing error: unknown indexing mode")
		}
	default:
		return nil, fmt.Errorf("indexing error: unknown indexing mode")
	}
	return idx, nil
}

func (s Series) addr() []string {
	var ret []string
	for _, e := range s.elements {
		ret = append(ret, e.Addr())
	}
	return ret
}
