package df

// TODO: Improve package documentation and include code examples

import (
	"errors"
	"fmt"
	"strings"
)

// TODO: Refactor error returns
type Series struct {
	Name     string   // The name of the series
	elements Elements // The values of the elements
	t        string   // The type of the series
	err      error
}

func (s Series) Empty() Series {
	ret := Series{Name: s.Name, t: s.t}
	switch ret.t {
	case "string":
		ret.elements = StringElements{}
	case "int":
		ret.elements = IntElements{}
	case "float":
		ret.elements = FloatElements{}
	case "bool":
		ret.elements = BoolElements{}
	}
	return ret
}

func (s Series) Err() error {
	return s.err
}

func (s Series) Set(i int, val ElementValue) Series {
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

func (s Series) Elem(i int) Element {
	if i >= Len(s) || i < 0 {
		return nil
	}
	return s.elements.Elem(i)
}

func (s Series) Val(i int) interface{} {
	if i >= Len(s) || i < 0 {
		return nil
	}
	elem := s.elements.Elem(i)
	if elem.IsNA() {
		return nil
	}
	return elem.Val()
}

func (s *Series) Append(x interface{}) {
	s.elements = s.elements.Append(x)
}

func (s Series) Concat(x Series) Series {
	var y Series
	switch s.t {
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

func (s Series) Subset(indexes interface{}) Series {
	var series Series
	switch s.t {
	case "string":
		elements := s.elements.(StringElements)
		switch indexes.(type) {
		case []int:
			elems := StringElements{}
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
			var elems StringElements
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
				boolElems := idx.elements.(BoolElements)
				var elems StringElements
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
				elems := StringElements{}
				intElems := idx.elements.(IntElements)
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
				elems := StringElements{}
				intElems := Ints(idx).elements.(IntElements)
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
		elements := s.elements.(IntElements)
		switch indexes.(type) {
		case []int:
			elems := IntElements{}
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
			var elems IntElements
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
				boolElems := idx.elements.(BoolElements)
				var elems IntElements
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
				elems := IntElements{}
				intElems := idx.elements.(IntElements)
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
				elems := IntElements{}
				intElems := Ints(idx).elements.(IntElements)
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
		elements := s.elements.(FloatElements)
		switch indexes.(type) {
		case []int:
			elems := FloatElements{}
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
			var elems FloatElements
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
				boolElems := idx.elements.(BoolElements)
				var elems FloatElements
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
				elems := FloatElements{}
				intElems := idx.elements.(IntElements)
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
				elems := FloatElements{}
				intElems := Ints(idx).elements.(IntElements)
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
		elements := s.elements.(BoolElements)
		switch indexes.(type) {
		case []int:
			elems := BoolElements{}
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
			var elems BoolElements
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
				boolElems := idx.elements.(BoolElements)
				var elems BoolElements
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
				elems := BoolElements{}
				intElems := idx.elements.(IntElements)
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
				elems := BoolElements{}
				intElems := Ints(idx).elements.(IntElements)
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

// TODO: Return a Bools Series instead of []bool?
func (s Series) Compare(comparator string, comparando interface{}) ([]bool, error) {
	var comp Series
	switch s.t {
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

func (s Series) Records() []string {
	return s.elements.Records()
}

func (s Series) String() string {
	return fmt.Sprint(s.elements)
}

func (s Series) Copy() Series {
	var copy Series
	switch s.t {
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
	var elements Elements = make(StringElements, 0)
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
	var elements Elements = make(IntElements, 0)
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
	var elements Elements = make(FloatElements, 0)
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
	var elements Elements = make(BoolElements, 0)
	elements = elements.Append(args...)
	ret := Series{
		Name:     "",
		elements: elements,
		t:        "bool",
	}
	return ret
}

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

func Len(s Series) int {
	switch s.t {
	case "string":
		elems := s.elements.(StringElements)
		return (len(elems))
	case "int":
		elems := s.elements.(IntElements)
		return (len(elems))
	case "float":
		elems := s.elements.(FloatElements)
		return (len(elems))
	case "bool":
		elems := s.elements.(BoolElements)
		return (len(elems))
	}
	return -1
}

func Type(s Series) string {
	return s.t
}

func Addr(s Series) []string {
	var ret []string
	switch s.t {
	case "string":
		elems := s.elements.(StringElements)
		for _, elem := range elems {
			ret = append(ret, fmt.Sprint(elem.s))
		}
	case "int":
		elems := s.elements.(IntElements)
		for _, elem := range elems {
			ret = append(ret, fmt.Sprint(elem.i))
		}
	case "float":
		elems := s.elements.(FloatElements)
		for _, elem := range elems {
			ret = append(ret, fmt.Sprint(elem.f))
		}
	case "bool":
		elems := s.elements.(BoolElements)
		for _, elem := range elems {
			ret = append(ret, fmt.Sprint(elem.b))
		}
	}
	return ret
}
