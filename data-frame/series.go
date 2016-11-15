package df

import (
	"errors"
	"fmt"
	"strings"
)

// Series is the main structure for a series of elements of the same type. It is
// the primary building block of a DataFrame.
type Series struct {
	Name     string         // The name of the series
	elements seriesElements // The values of the elements
	t        string         // The type of the series
	err      error
}

// Empty returns an empty Series of the same type
func (s Series) Empty() Series {
	ret := Series{Name: s.Name, t: s.t}
	switch ret.t {
	// FIXME: Use SeriesType instead
	case "string":
		ret.elements = stringElements{}
	case "int":
		ret.elements = intElements{}
	case "float":
		ret.elements = floatElements{}
	case "bool":
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
	if i >= Len(s) || i < 0 {
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
	if i >= Len(s) || i < 0 {
		return nil
	}
	return s.elements.Elem(i)
}

// Val returns the value of a series for the given index or nil if NA or out of bounds
func (s Series) Val(i int) interface{} {
	// FIXME: This is probably not the right way to handle out of bounds/NA errors...
	if i >= Len(s) || i < 0 {
		return nil
	}
	elem := s.elements.Elem(i)
	if elem.IsNA() {
		return nil
	}
	return elem.Val()
}

// Append adds elements to the end of the Series
func (s *Series) Append(x interface{}) {
	s.elements = s.elements.Append(x)
}

// Concat concatenates two series together
func (s Series) Concat(x Series) Series {
	var y Series
	switch s.t {
	// FIXME: Use SeriesType instead
	case "string":
		y = NamedStrings(s.Name, s, x)
	case "int":
		y = NamedInts(s.Name, s, x)
	case "float":
		y = NamedFloats(s.Name, s, x)
	case "bool":
		y = NamedBools(s.Name, s, x)
	default:
		return Series{err: errors.New("Unknown Series type")}
	}
	return y
}

// Subset returns a subset of the series based on the given indexes
func (s Series) Subset(indexes interface{}) Series {
	var series Series
	switch s.t {
	// FIXME: Use SeriesType instead
	case "string":
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
			if len(idx) != Len(s) {
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
			case "string":
				return Series{err: errors.New("Wrong Series type for subsetting")}
			case "bool":
				if Len(idx) != Len(s) {
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
			case "int":
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
			case "float":
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
	case "int":
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
			if len(idx) != Len(s) {
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
			case "string":
				return Series{err: errors.New("Wrong Series type for subsetting")}
			case "bool":
				if Len(idx) != Len(s) {
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
			case "int":
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
			case "float":
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
	case "float":
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
			if len(idx) != Len(s) {
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
			case "string":
				return Series{err: errors.New("Wrong Series type for subsetting")}
			case "bool":
				if Len(idx) != Len(s) {
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
			case "int":
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
			case "float":
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
	case "bool":
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
			if len(idx) != Len(s) {
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
			case "string":
				return Series{err: errors.New("Wrong Series type for subsetting")}
			case "bool":
				if Len(idx) != Len(s) {
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
			case "int":
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
			case "float":
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
func (s Series) Compare(comparator string, comparando interface{}) ([]bool, error) {
	var comp Series
	switch s.t {
	// FIXME: Use SeriesType instead
	case "string":
		comp = Strings(comparando)
	case "int":
		comp = Ints(comparando)
	case "float":
		comp = Floats(comparando)
	case "bool":
		comp = Bools(comparando)
	default:
		return nil, errors.New("Unknown Series type")
	}
	ret := []bool{}
	switch comparator {
	// FIXME: Use ComparatorType instead
	case "==":
		if Len(comp) == 1 {
			for i := 0; i < Len(s); i++ {
				ret = append(ret, s.elements.Elem(i).Eq(comp.elements.Elem(0)))
			}
			return ret, nil
		}
		if Len(s) != Len(comp) {
			return nil, errors.New("Can't compare Series: Different dimensions")
		}
		for i := 0; i < Len(s); i++ {
			ret = append(ret, s.elements.Elem(i).Eq(comp.elements.Elem(i)))
		}
	case "!=":
		if Len(comp) == 1 {
			for i := 0; i < Len(s); i++ {
				ret = append(ret, !s.elements.Elem(i).Eq(comp.elements.Elem(0)))
			}
			return ret, nil
		}
		if Len(s) != Len(comp) {
			return nil, errors.New("Can't compare Series: Different dimensions")
		}
		for i := 0; i < Len(s); i++ {
			ret = append(ret, !s.elements.Elem(i).Eq(comp.elements.Elem(i)))
		}
	case ">":
		if Len(comp) == 1 {
			for i := 0; i < Len(s); i++ {
				ret = append(ret, s.elements.Elem(i).Greater(comp.elements.Elem(0)))
			}
			return ret, nil
		}
		if Len(s) != Len(comp) {
			return nil, errors.New("Can't compare Series: Different dimensions")
		}
		for i := 0; i < Len(s); i++ {
			ret = append(ret, s.elements.Elem(i).Greater(comp.elements.Elem(i)))
		}
	case ">=":
		if Len(comp) == 1 {
			for i := 0; i < Len(s); i++ {
				ret = append(ret, s.elements.Elem(i).GreaterEq(comp.elements.Elem(0)))
			}
			return ret, nil
		}
		if Len(s) != Len(comp) {
			return nil, errors.New("Can't compare Series: Different dimensions")
		}
		for i := 0; i < Len(s); i++ {
			ret = append(ret, s.elements.Elem(i).GreaterEq(comp.elements.Elem(i)))
		}
	case "<":
		if Len(comp) == 1 {
			for i := 0; i < Len(s); i++ {
				ret = append(ret, s.elements.Elem(i).Less(comp.elements.Elem(0)))
			}
			return ret, nil
		}
		if Len(s) != Len(comp) {
			return nil, errors.New("Can't compare Series: Different dimensions")
		}
		for i := 0; i < Len(s); i++ {
			ret = append(ret, s.elements.Elem(i).Less(comp.elements.Elem(i)))
		}
	case "<=":
		if Len(comp) == 1 {
			for i := 0; i < Len(s); i++ {
				ret = append(ret, s.elements.Elem(i).LessEq(comp.elements.Elem(0)))
			}
			return ret, nil
		}
		if Len(s) != Len(comp) {
			return nil, errors.New("Can't compare Series: Different dimensions")
		}
		for i := 0; i < Len(s); i++ {
			ret = append(ret, s.elements.Elem(i).LessEq(comp.elements.Elem(i)))
		}
	case "in":
		for i := 0; i < Len(s); i++ {
			found := false
			for j := 0; j < Len(comp); j++ {
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
	var copy Series
	switch s.t {
	// FIXME: Use SeriesType instead
	case "string":
		copy = Strings(s)
		n := s.Name
		copy.Name = n
	case "int":
		copy = Ints(s)
		n := s.Name
		copy.Name = n
	case "float":
		copy = Floats(s)
		n := s.Name
		copy.Name = n
	case "bool":
		copy = Bools(s)
		n := s.Name
		copy.Name = n
	}
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
		t:        "string",
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
		t:        "int",
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
		t:        "float",
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
		t:        "bool",
	}
	return ret
}

// Str prints some extra information about a given series
func Str(s Series) string {
	var ret []string
	// If name exists print name
	if s.Name != "" {
		ret = append(ret, "Name: "+s.Name)
	}
	ret = append(ret, "Type: "+s.t)
	ret = append(ret, "Length: "+fmt.Sprint(Len(s)))
	if Len(s) != 0 {
		ret = append(ret, "Values: "+fmt.Sprint(s))
	}
	return strings.Join(ret, "\n")
}

// Len returns the length of a given Series
func Len(s Series) int {
	switch s.t {
	// FIXME: Use SeriesType instead
	case "string":
		elems := s.elements.(stringElements)
		return (len(elems))
	case "int":
		elems := s.elements.(intElements)
		return (len(elems))
	case "float":
		elems := s.elements.(floatElements)
		return (len(elems))
	case "bool":
		elems := s.elements.(boolElements)
		return (len(elems))
	}
	return -1
}

// Type returns the type of a given series
func (s Series) Type() string {
	return s.t
}

func addr(s Series) []string {
	var ret []string
	switch s.t {
	// FIXME: Use SeriesType instead
	case "string":
		elems := s.elements.(stringElements)
		for _, elem := range elems {
			ret = append(ret, fmt.Sprint(elem.s))
		}
	case "int":
		elems := s.elements.(intElements)
		for _, elem := range elems {
			ret = append(ret, fmt.Sprint(elem.i))
		}
	case "float":
		elems := s.elements.(floatElements)
		for _, elem := range elems {
			ret = append(ret, fmt.Sprint(elem.f))
		}
	case "bool":
		elems := s.elements.(boolElements)
		for _, elem := range elems {
			ret = append(ret, fmt.Sprint(elem.b))
		}
	}
	return ret
}
