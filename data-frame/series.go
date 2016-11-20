package df

import (
	"errors"
	"fmt"
	"math"
	"strings"
)

// Series is the main structure for a series of elements of the same type. It is
// the primary building block of a DataFrame.
type Series struct {
	Name     string         // The name of the series
	elements seriesElements // The values of the elements
	t        Type           // The type of the series
	err      error
}

// Comparator is a comparator that can be used for filtering Series and DataFrames
type Comparator string

const (
	Eq        Comparator = "=="
	Neq                  = "!="
	Greater              = ">"
	GreaterEq            = ">="
	Less                 = "<"
	LessEq               = "<="
	In                   = "in"
)

// Type represents the type of the elements that can be stored on Series
type Type string

const (
	String Type = "string"
	Int         = "int"
	Float       = "float"
	Bool        = "bool"
)

// NewSeries is the generic Series constructor
func NewSeries(elements interface{}, t Type) Series {
	ret := Series{}
	switch t {
	case String:
		ret = Strings(elements)
	case Int:
		ret = Ints(elements)
	case Float:
		ret = Floats(elements)
	case Bool:
		ret = Bools(elements)
	default:
		return Series{err: errors.New("unknown type")}
	}
	return ret
}

// Empty returns an empty Series of the same type
func (s Series) Empty() Series {
	ret := Series{Name: s.Name, t: s.t}
	switch ret.t {
	case String:
		ret.elements = stringElements{}
	case Int:
		ret.elements = intElements{}
	case Float:
		ret.elements = floatElements{}
	case Bool:
		ret.elements = boolElements{}
	}
	return ret
}

// Err returns the error contained in the series
func (s Series) Err() error {
	return s.err
}

func (s Series) set(i int, val elementValue) Series {
	if s.Err() != nil {
		return s
	}
	if i >= s.Len() || i < 0 {
		return Series{err: errors.New("Couldn't set element. Index out of bounds")}
	}
	elems, err := s.elements.Set(i, val)
	if err != nil {
		return Series{err: errors.New("Couldn't set element: " + err.Error())}
	}
	s.elements = elems
	return s
}

func (s Series) elem(i int) elementInterface {
	if i >= s.Len() || i < 0 {
		return nil
	}
	return s.elements.Elem(i)
}

// Val returns the value of a series for the given index
func (s Series) Val(i int) (interface{}, error) {
	if i >= s.Len() || i < 0 {
		return nil, errors.New("index out of bounds")
	}
	elem := s.elements.Elem(i).Val()
	return elem, nil
}

// Append adds elements to the end of the Series
func (s *Series) Append(x interface{}) {
	s.elements = s.elements.Append(x)
}

// Concat concatenates two series together
func (s Series) Concat(x Series) Series {
	var y Series
	switch s.t {
	case String:
		y = NamedStrings(s.Name, s, x)
	case Int:
		y = NamedInts(s.Name, s, x)
	case Float:
		y = NamedFloats(s.Name, s, x)
	case Bool:
		y = NamedBools(s.Name, s, x)
	default:
		return Series{err: errors.New("Unknown Series type")}
	}
	return y
}

// Subset returns a subset of the series based on the given indexes
func (s Series) Subset(indexes interface{}) Series {
	// TODO: This could use some work
	var series Series
	switch s.t {
	case String:
		elements := s.elements.(stringElements)
		switch indexes.(type) {
		case []int:
			elems := stringElements{}
			for _, v := range indexes.([]int) {
				if v >= len(elements) || v < 0 {
					return Series{err: errors.New("Index out of range")}
				}
				elems = append(elems, elements[v])
			}
			series = NamedStrings(s.Name, elems)
		case []bool:
			idx := indexes.([]bool)
			if len(idx) != s.Len() {
				return Series{err: errors.New("Dimensions mismatch")}
			}
			var elems stringElements
			for k, v := range idx {
				if v {
					elems = append(elems, elements[k])
				}
			}
			series = NamedStrings(s.Name, elems)
		case Series:
			idx := indexes.(Series)
			switch idx.t {
			case String:
				return Series{err: errors.New("Wrong Series type for subsetting")}
			case Bool:
				if idx.Len() != s.Len() {
					return Series{err: errors.New("Dimensions mismatch")}
				}
				boolElems := idx.elements.(boolElements)
				var elems stringElements
				for k, v := range boolElems {
					b := v.ToBool().Val()
					if b == nil {
						return Series{err: errors.New("Can't subset over NA elements")}
					}
					if b.(bool) {
						elems = append(elems, elements[k])
					}
				}
				series = NamedStrings(s.Name, elems)
			case Int:
				elems := stringElements{}
				intElems := idx.elements.(intElements)
				for _, v := range intElems {
					i := v.ToInt().Val()
					if i == nil {
						return Series{err: errors.New("Can't subset over NA elements")}
					}
					if i.(int) >= len(elements) || i.(int) < 0 {
						return Series{err: errors.New("Index out of range")}
					}
					elems = append(elems, elements[i.(int)])
				}
				series = NamedStrings(s.Name, elems)
			case Float:
				elems := stringElements{}
				intElems := Ints(idx).elements.(intElements)
				for _, v := range intElems {
					i := v.ToInt().Val()
					if i == nil {
						return Series{err: errors.New("Can't subset over NA elements")}
					}
					if i.(int) >= len(elements) || i.(int) < 0 {
						return Series{err: errors.New("Index out of range")}
					}
					elems = append(elems, elements[i.(int)])
				}
				series = NamedStrings(s.Name, elems)
			}
		default:
			return Series{err: errors.New("Unknown indexing mode")}
		}
	case Int:
		elements := s.elements.(intElements)
		switch indexes.(type) {
		case []int:
			elems := intElements{}
			for _, v := range indexes.([]int) {
				if v >= len(elements) || v < 0 {
					return Series{err: errors.New("Index out of range")}
				}
				elems = append(elems, elements[v])
			}
			series = NamedInts(s.Name, elems)
		case []bool:
			idx := indexes.([]bool)
			if len(idx) != s.Len() {
				return Series{err: errors.New("Dimensions mismatch")}
			}
			var elems intElements
			for k, v := range idx {
				if v {
					elems = append(elems, elements[k])
				}
			}
			series = NamedInts(s.Name, elems)
		case Series:
			idx := indexes.(Series)
			switch idx.t {
			case String:
				return Series{err: errors.New("Wrong Series type for subsetting")}
			case Bool:
				if idx.Len() != s.Len() {
					return Series{err: errors.New("Dimensions mismatch")}
				}
				boolElems := idx.elements.(boolElements)
				var elems intElements
				for k, v := range boolElems {
					b := v.ToBool().Val()
					if b == nil {
						return Series{err: errors.New("Can't subset over NA elements")}
					}
					if b.(bool) {
						elems = append(elems, elements[k])
					}
				}
				series = NamedInts(s.Name, elems)
			case Int:
				elems := intElements{}
				intElems := idx.elements.(intElements)
				for _, v := range intElems {
					i := v.ToInt().Val()
					if i == nil {
						return Series{err: errors.New("Can't subset over NA elements")}
					}
					if i.(int) >= len(elements) || i.(int) < 0 {
						return Series{err: errors.New("Index out of range")}
					}
					elems = append(elems, elements[i.(int)])
				}
				series = NamedInts(s.Name, elems)
			case Float:
				elems := intElements{}
				intElems := Ints(idx).elements.(intElements)
				for _, v := range intElems {
					i := v.ToInt().Val()
					if i == nil {
						return Series{err: errors.New("Can't subset over NA elements")}
					}
					if i.(int) >= len(elements) || i.(int) < 0 {
						return Series{err: errors.New("Index out of range")}
					}
					elems = append(elems, elements[i.(int)])
				}
				series = NamedInts(s.Name, elems)
			}
		default:
			return Series{err: errors.New("Unknown indexing mode")}
		}
	case Float:
		elements := s.elements.(floatElements)
		switch indexes.(type) {
		case []int:
			elems := floatElements{}
			for _, v := range indexes.([]int) {
				if v >= len(elements) || v < 0 {
					return Series{err: errors.New("Index out of range")}
				}
				elems = append(elems, elements[v])
			}
			series = NamedFloats(s.Name, elems)
		case []bool:
			idx := indexes.([]bool)
			if len(idx) != s.Len() {
				return Series{err: errors.New("Dimensions mismatch")}
			}
			var elems floatElements
			for k, v := range idx {
				if v {
					elems = append(elems, elements[k])
				}
			}
			series = NamedFloats(s.Name, elems)
		case Series:
			idx := indexes.(Series)
			switch idx.t {
			case String:
				return Series{err: errors.New("Wrong Series type for subsetting")}
			case Bool:
				if idx.Len() != s.Len() {
					return Series{err: errors.New("Dimensions mismatch")}
				}
				boolElems := idx.elements.(boolElements)
				var elems floatElements
				for k, v := range boolElems {
					b := v.ToBool().Val()
					if b == nil {
						return Series{err: errors.New("Can't subset over NA elements")}
					}
					if b.(bool) {
						elems = append(elems, elements[k])
					}
				}
				series = NamedFloats(s.Name, elems)
			case Int:
				elems := floatElements{}
				intElems := idx.elements.(intElements)
				for _, v := range intElems {
					i := v.ToInt().Val()
					if i == nil {
						return Series{err: errors.New("Can't subset over NA elements")}
					}
					if i.(int) >= len(elements) || i.(int) < 0 {
						return Series{err: errors.New("Index out of range")}
					}
					elems = append(elems, elements[i.(int)])
				}
				series = NamedFloats(s.Name, elems)
			case Float:
				elems := floatElements{}
				intElems := Ints(idx).elements.(intElements)
				for _, v := range intElems {
					i := v.ToInt().Val()
					if i == nil {
						return Series{err: errors.New("Can't subset over NA elements")}
					}
					if i.(int) >= len(elements) || i.(int) < 0 {
						return Series{err: errors.New("Index out of range")}
					}
					elems = append(elems, elements[i.(int)])
				}
				series = NamedFloats(s.Name, elems)
			}
		default:
			return Series{err: errors.New("Unknown indexing mode")}
		}
	case Bool:
		elements := s.elements.(boolElements)
		switch indexes.(type) {
		case []int:
			elems := boolElements{}
			for _, v := range indexes.([]int) {
				if v >= len(elements) || v < 0 {
					return Series{err: errors.New("Index out of range")}
				}
				elems = append(elems, elements[v])
			}
			series = NamedBools(s.Name, elems)
		case []bool:
			idx := indexes.([]bool)
			if len(idx) != s.Len() {
				return Series{err: errors.New("Dimensions mismatch")}
			}
			var elems boolElements
			for k, v := range idx {
				if v {
					elems = append(elems, elements[k])
				}
			}
			series = NamedBools(s.Name, elems)
		case Series:
			idx := indexes.(Series)
			switch idx.t {
			case String:
				return Series{err: errors.New("Wrong Series type for subsetting")}
			case Bool:
				if idx.Len() != s.Len() {
					return Series{err: errors.New("Dimensions mismatch")}
				}
				boolElems := idx.elements.(boolElements)
				var elems boolElements
				for k, v := range boolElems {
					b := v.ToBool().Val()
					if b == nil {
						return Series{err: errors.New("Can't subset over NA elements")}
					}
					if b.(bool) {
						elems = append(elems, elements[k])
					}
				}
				series = NamedBools(s.Name, elems)
			case Int:
				elems := boolElements{}
				intElems := idx.elements.(intElements)
				for _, v := range intElems {
					i := v.ToInt().Val()
					if i == nil {
						return Series{err: errors.New("Can't subset over NA elements")}
					}
					if i.(int) >= len(elements) || i.(int) < 0 {
						return Series{err: errors.New("Index out of range")}
					}
					elems = append(elems, elements[i.(int)])
				}
				series = NamedBools(s.Name, elems)
			case Float:
				elems := boolElements{}
				intElems := Ints(idx).elements.(intElements)
				for _, v := range intElems {
					i := v.ToInt().Val()
					if i == nil {
						return Series{err: errors.New("Can't subset over NA elements")}
					}
					if i.(int) >= len(elements) || i.(int) < 0 {
						return Series{err: errors.New("Index out of range")}
					}
					elems = append(elems, elements[i.(int)])
				}
				series = NamedBools(s.Name, elems)
			}
		default:
			return Series{err: errors.New("Unknown indexing mode")}
		}
	}
	return series
}

// Compare compares the values of a Series with other series, scalars, text, etc
func (s Series) Compare(comparator Comparator, comparando interface{}) ([]bool, error) {
	var comp Series
	switch s.t {
	case String:
		comp = Strings(comparando)
	case Int:
		comp = Ints(comparando)
	case Float:
		comp = Floats(comparando)
	case Bool:
		comp = Bools(comparando)
	default:
		return nil, errors.New("Unknown Series type")
	}
	ret := []bool{}
	switch comparator {
	case Eq:
		if comp.Len() == 1 {
			for i := 0; i < s.Len(); i++ {
				ret = append(ret, s.elements.Elem(i).Eq(comp.elements.Elem(0)))
			}
			return ret, nil
		}
		if s.Len() != comp.Len() {
			return nil, errors.New("Can't compare Series: Different dimensions")
		}
		for i := 0; i < s.Len(); i++ {
			ret = append(ret, s.elements.Elem(i).Eq(comp.elements.Elem(i)))
		}
	case Neq:
		if comp.Len() == 1 {
			for i := 0; i < s.Len(); i++ {
				ret = append(ret, !s.elements.Elem(i).Eq(comp.elements.Elem(0)))
			}
			return ret, nil
		}
		if s.Len() != comp.Len() {
			return nil, errors.New("Can't compare Series: Different dimensions")
		}
		for i := 0; i < s.Len(); i++ {
			ret = append(ret, !s.elements.Elem(i).Eq(comp.elements.Elem(i)))
		}
	case Greater:
		if comp.Len() == 1 {
			for i := 0; i < s.Len(); i++ {
				ret = append(ret, s.elements.Elem(i).Greater(comp.elements.Elem(0)))
			}
			return ret, nil
		}
		if s.Len() != comp.Len() {
			return nil, errors.New("Can't compare Series: Different dimensions")
		}
		for i := 0; i < s.Len(); i++ {
			ret = append(ret, s.elements.Elem(i).Greater(comp.elements.Elem(i)))
		}
	case GreaterEq:
		if comp.Len() == 1 {
			for i := 0; i < s.Len(); i++ {
				ret = append(ret, s.elements.Elem(i).GreaterEq(comp.elements.Elem(0)))
			}
			return ret, nil
		}
		if s.Len() != comp.Len() {
			return nil, errors.New("Can't compare Series: Different dimensions")
		}
		for i := 0; i < s.Len(); i++ {
			ret = append(ret, s.elements.Elem(i).GreaterEq(comp.elements.Elem(i)))
		}
	case Less:
		if comp.Len() == 1 {
			for i := 0; i < s.Len(); i++ {
				ret = append(ret, s.elements.Elem(i).Less(comp.elements.Elem(0)))
			}
			return ret, nil
		}
		if s.Len() != comp.Len() {
			return nil, errors.New("Can't compare Series: Different dimensions")
		}
		for i := 0; i < s.Len(); i++ {
			ret = append(ret, s.elements.Elem(i).Less(comp.elements.Elem(i)))
		}
	case LessEq:
		if comp.Len() == 1 {
			for i := 0; i < s.Len(); i++ {
				ret = append(ret, s.elements.Elem(i).LessEq(comp.elements.Elem(0)))
			}
			return ret, nil
		}
		if s.Len() != comp.Len() {
			return nil, errors.New("Can't compare Series: Different dimensions")
		}
		for i := 0; i < s.Len(); i++ {
			ret = append(ret, s.elements.Elem(i).LessEq(comp.elements.Elem(i)))
		}
	case In:
		for i := 0; i < s.Len(); i++ {
			found := false
			for j := 0; j < comp.Len(); j++ {
				if s.elements.Elem(i).Eq(comp.elements.Elem(j)) {
					found = true
					break
				}
			}
			ret = append(ret, found)
		}
	default:
		return nil, errors.New("Unknown comparator")
	}
	return ret, nil
}

// Records returns the elements of a Series in a []string
func (s Series) Records() []string {
	return s.elements.Records()
}

// String implements the Stringer interface for Series
func (s Series) String() string {
	return fmt.Sprint(s.elements)
}

// Copy wil copy the values of a given Series
func (s Series) Copy() Series {
	copy := Series{}
	copy.Name = s.Name
	copy.t = s.t
	copy.elements = s.elements.Copy()
	return copy
}

// NamedStrings is a constructor for a named String series
func NamedStrings(name string, args ...interface{}) Series {
	s := Strings(args...)
	s.Name = name
	return s
}

// NamedInts is a constructor for a named Int series
func NamedInts(name string, args ...interface{}) Series {
	s := Ints(args...)
	s.Name = name
	return s
}

// NamedFloats is a constructor for a named Float series
func NamedFloats(name string, args ...interface{}) Series {
	s := Floats(args...)
	s.Name = name
	return s
}

// NamedBools is a constructor for a named Bool series
func NamedBools(name string, args ...interface{}) Series {
	s := Bools(args...)
	s.Name = name
	return s
}

// Strings is a constructor for a String series
func Strings(args ...interface{}) Series {
	var elements seriesElements = make(stringElements, 0)
	elements = elements.Append(args...)
	ret := Series{
		Name:     "",
		elements: elements,
		t:        String,
	}
	return ret
}

// Ints is a constructor for an Int series
func Ints(args ...interface{}) Series {
	var elements seriesElements = make(intElements, 0)
	elements = elements.Append(args...)
	ret := Series{
		Name:     "",
		elements: elements,
		t:        Int,
	}
	return ret
}

// Floats is a constructor for a Float series
func Floats(args ...interface{}) Series {
	var elements seriesElements = make(floatElements, 0)
	elements = elements.Append(args...)
	ret := Series{
		Name:     "",
		elements: elements,
		t:        Float,
	}
	return ret
}

// Bools is a constructor for a bools series
func Bools(args ...interface{}) Series {
	var elements seriesElements = make(boolElements, 0)
	elements = elements.Append(args...)
	ret := Series{
		Name:     "",
		elements: elements,
		t:        Bool,
	}
	return ret
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

// Len returns the length of a given Series
func (s Series) Len() int {
	return s.elements.Len()
}

func (s Series) Float() ([]float64, error) {
	var ret []float64
	switch s.t {
	case String:
		elems := s.elements.(stringElements)
		for _, elem := range elems {
			val := elem.ToFloat().Val()
			if val == nil {
				ret = append(ret, math.NaN())
			} else {
				ret = append(ret, val.(float64))
			}
		}
		return ret, nil
	case Int:
		elems := s.elements.(intElements)
		for _, elem := range elems {
			val := elem.ToFloat().Val()
			if val == nil {
				ret = append(ret, math.NaN())
			} else {
				ret = append(ret, val.(float64))
			}
		}
		return ret, nil
	case Float:
		elems := s.elements.(floatElements)
		for _, elem := range elems {
			val := elem.ToFloat().Val()
			if val == nil {
				ret = append(ret, math.NaN())
			} else {
				ret = append(ret, val.(float64))
			}
		}
		return ret, nil
	case Bool:
		elems := s.elements.(boolElements)
		for _, elem := range elems {
			val := elem.ToFloat().Val()
			if val == nil {
				ret = append(ret, math.NaN())
			} else {
				ret = append(ret, val.(float64))
			}
		}
		return ret, nil
	}
	return nil, errors.New("Couldn't convert to []float64")
}

// Type returns the type of a given series
func (s Series) Type() Type {
	return s.t
}

func addr(s Series) []string {
	var ret []string
	switch s.t {
	case String:
		elems := s.elements.(stringElements)
		for _, elem := range elems {
			ret = append(ret, fmt.Sprint(elem.s))
		}
	case Int:
		elems := s.elements.(intElements)
		for _, elem := range elems {
			ret = append(ret, fmt.Sprint(elem.i))
		}
	case Float:
		elems := s.elements.(floatElements)
		for _, elem := range elems {
			ret = append(ret, fmt.Sprint(elem.f))
		}
	case Bool:
		elems := s.elements.(boolElements)
		for _, elem := range elems {
			ret = append(ret, fmt.Sprint(elem.b))
		}
	}
	return ret
}
