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

func (s Series) Err() error {
	return s.err
}

func (s Series) Elem(i int) Element {
	if i >= Len(s) || i < 0 {
		return nil
	}
	return s.Elem(i)
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

func (s Series) Append(x interface{}) {
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
	// TODO: What to do in case of NAs?
	switch s.t {
	case "string":
		elements := s.elements.(StringElements)
		ret := []bool{}
		comparando := Strings(comparando)
		compElements := comparando.elements.(StringElements)
		switch comparator {
		case "==":
			if Len(comparando) == 1 {
				for _, v := range elements {
					ret = append(ret, v.Eq(compElements[0]))
				}
				return ret, nil
			}
			if Len(s) != Len(comparando) {
				return nil, errors.New("Can't compare Series: Different dimensions")
			}
			for i := 0; i < Len(s); i++ {
				ret = append(ret, elements[i].Eq(compElements[i]))
			}
			return ret, nil
		case "!=":
			if Len(comparando) == 1 {
				for _, v := range elements {
					ret = append(ret, !v.Eq(compElements[0]))
				}
				return ret, nil
			}
			if Len(s) != Len(comparando) {
				return nil, errors.New("Can't compare Series: Different dimensions")
			}
			for i := 0; i < Len(s); i++ {
				ret = append(ret, !elements[i].Eq(compElements[i]))
			}
			return ret, nil
		case ">":
			if Len(comparando) == 1 {
				for _, v := range elements {
					ret = append(ret, v.String() > compElements[0].String())
				}
				return ret, nil
			}
			if Len(s) != Len(comparando) {
				return nil, errors.New("Can't compare Series: Different dimensions")
			}
			for i := 0; i < Len(s); i++ {
				ret = append(ret, elements[i].String() > compElements[i].String())
			}
			return ret, nil
		case ">=":
			if Len(comparando) == 1 {
				for _, v := range elements {
					ret = append(ret, v.String() >= compElements[0].String())
				}
				return ret, nil
			}
			if Len(s) != Len(comparando) {
				return nil, errors.New("Can't compare Series: Different dimensions")
			}
			for i := 0; i < Len(s); i++ {
				ret = append(ret, elements[i].String() >= compElements[i].String())
			}
			return ret, nil
		case "<":
			if Len(comparando) == 1 {
				for _, v := range elements {
					ret = append(ret, v.String() < compElements[0].String())
				}
				return ret, nil
			}
			if Len(s) != Len(comparando) {
				return nil, errors.New("Can't compare Series: Different dimensions")
			}
			for i := 0; i < Len(s); i++ {
				ret = append(ret, elements[i].String() < compElements[i].String())
			}
			return ret, nil
		case "<=":
			if Len(comparando) == 1 {
				for _, v := range elements {
					ret = append(ret, v.String() <= compElements[0].String())
				}
				return ret, nil
			}
			if Len(s) != Len(comparando) {
				return nil, errors.New("Can't compare Series: Different dimensions")
			}
			for i := 0; i < Len(s); i++ {
				ret = append(ret, elements[i].String() <= compElements[i].String())
			}
			return ret, nil
		case "in":
			for _, v := range elements {
				found := false
				for _, w := range compElements {
					if v.String() == w.String() {
						found = true
						break
					}
				}
				ret = append(ret, found)
			}
			return ret, nil
		default:
			return nil, errors.New("Unknown comparator")
		}

	case "int":
		elements := s.elements.(IntElements)
		ret := []bool{}
		comparando := Ints(comparando)
		compElements := comparando.elements.(IntElements)
		switch comparator {
		case "==":
			if Len(comparando) == 1 {
				compInt := compElements[0]
				for _, v := range elements {
					ret = append(ret, v.Eq(compInt))
				}
				return ret, nil
			}
			if Len(s) != Len(comparando) {
				return nil, errors.New("Can't compare Series: Different dimensions")
			}
			for i := 0; i < Len(s); i++ {
				ret = append(ret, elements[i].Eq(compElements[i]))
			}
			return ret, nil
		case "!=":
			if Len(comparando) == 1 {
				for _, v := range elements {
					ret = append(ret, !v.Eq(compElements[0]))
				}
				return ret, nil
			}
			if Len(s) != Len(comparando) {
				return nil, errors.New("Can't compare Series: Different dimensions")
			}
			for i := 0; i < Len(s); i++ {
				ret = append(ret, !elements[i].Eq(compElements[i]))
			}
			return ret, nil
		case ">":
			if Len(comparando) == 1 {
				compInt := compElements[0].ToInt().Val()
				for _, v := range elements {
					sInt := v.ToInt().Val()
					if sInt == nil || compInt == nil {
						ret = append(ret, false)
						continue
					}
					ret = append(ret, sInt.(int) > compInt.(int))
				}
				return ret, nil
			}
			if Len(s) != Len(comparando) {
				return nil, errors.New("Can't compare Series: Different dimensions")
			}
			for i := 0; i < Len(s); i++ {
				sInt := elements[i].ToInt().Val()
				compInt := compElements[i].ToInt().Val()
				if sInt == nil || compInt == nil {
					ret = append(ret, false)
					continue
				}
				ret = append(ret, sInt.(int) > compInt.(int))
			}
			return ret, nil
		case ">=":
			if Len(comparando) == 1 {
				compInt := compElements[0].ToInt().Val()
				for _, v := range elements {
					sInt := v.ToInt().Val()
					if sInt == nil || compInt == nil {
						ret = append(ret, false)
						continue
					}
					ret = append(ret, sInt.(int) >= compInt.(int))
				}
				return ret, nil
			}
			if Len(s) != Len(comparando) {
				return nil, errors.New("Can't compare Series: Different dimensions")
			}
			for i := 0; i < Len(s); i++ {
				sInt := elements[i].ToInt().Val()
				compInt := compElements[i].ToInt().Val()
				if sInt == nil || compInt == nil {
					ret = append(ret, false)
					continue
				}
				ret = append(ret, sInt.(int) >= compInt.(int))
			}
			return ret, nil
		case "<":
			if Len(comparando) == 1 {
				compInt := compElements[0].ToInt().Val()
				for _, v := range elements {
					sInt := v.ToInt().Val()
					if sInt == nil || compInt == nil {
						ret = append(ret, false)
						continue
					}
					ret = append(ret, sInt.(int) < compInt.(int))
				}
				return ret, nil
			}
			if Len(s) != Len(comparando) {
				return nil, errors.New("Can't compare Series: Different dimensions")
			}
			for i := 0; i < Len(s); i++ {
				sInt := elements[i].ToInt().Val()
				compInt := compElements[i].ToInt().Val()
				if sInt == nil || compInt == nil {
					ret = append(ret, false)
					continue
				}
				ret = append(ret, sInt.(int) < compInt.(int))
			}
			return ret, nil
		case "<=":
			if Len(comparando) == 1 {
				compInt := compElements[0].ToInt().Val()
				for _, v := range elements {
					sInt := v.ToInt().Val()
					if sInt == nil || compInt == nil {
						ret = append(ret, false)
						continue
					}
					ret = append(ret, sInt.(int) <= compInt.(int))
				}
				return ret, nil
			}
			if Len(s) != Len(comparando) {
				return nil, errors.New("Can't compare Series: Different dimensions")
			}
			for i := 0; i < Len(s); i++ {
				sInt := elements[i].ToInt().Val()
				compInt := compElements[i].ToInt().Val()
				if sInt == nil || compInt == nil {
					ret = append(ret, false)
					continue
				}
				ret = append(ret, sInt.(int) <= compInt.(int))
			}
			return ret, nil
		case "in":
			for _, v := range elements {
				sInt := v.ToInt().Val()
				found := false
				for _, w := range compElements {
					compInt := w.ToInt().Val()
					if sInt == nil || compInt == nil {
						continue
					}
					if sInt.(int) == compInt {
						found = true
						break
					}
				}
				ret = append(ret, found)
			}
			return ret, nil
		default:
			return nil, errors.New("Unknown comparator")
		}

	case "float":
		elements := s.elements.(FloatElements)
		ret := []bool{}
		comparando := Floats(comparando)
		compElements := comparando.elements.(FloatElements)
		switch comparator {
		case "==":
			if Len(comparando) == 1 {
				for _, v := range elements {
					ret = append(ret, v.Eq(compElements[0]))
				}
				return ret, nil
			}
			if Len(s) != Len(comparando) {
				return nil, errors.New("Can't compare Series: Different dimensions")
			}
			for i := 0; i < Len(s); i++ {
				ret = append(ret, elements[i].Eq(compElements[i]))
			}
			return ret, nil
		case "!=":
			if Len(comparando) == 1 {
				for _, v := range elements {
					ret = append(ret, !v.Eq(compElements[0]))
				}
				return ret, nil
			}
			if Len(s) != Len(comparando) {
				return nil, errors.New("Can't compare Series: Different dimensions")
			}
			for i := 0; i < Len(s); i++ {
				ret = append(ret, !elements[i].Eq(compElements[i]))
			}
			return ret, nil
		case ">":
			if Len(comparando) == 1 {
				compFloat := compElements[0].ToFloat().Val()
				for _, v := range elements {
					sFloat := v.ToFloat().Val()
					if sFloat == nil || compFloat == nil {
						ret = append(ret, false)
						continue
					}
					ret = append(ret, sFloat.(float64) > compFloat.(float64))
				}
				return ret, nil
			}
			if Len(s) != Len(comparando) {
				return nil, errors.New("Can't compare Series: Different dimensions")
			}
			for i := 0; i < Len(s); i++ {
				sFloat := elements[i].ToFloat().Val()
				compFloat := compElements[i].ToFloat().Val()
				if sFloat == nil || compFloat == nil {
					ret = append(ret, false)
					continue
				}
				ret = append(ret, sFloat.(float64) > compFloat.(float64))
			}
			return ret, nil
		case ">=":
			if Len(comparando) == 1 {
				compFloat := compElements[0].ToFloat().Val()
				for _, v := range elements {
					sFloat := v.ToFloat().Val()
					if sFloat == nil || compFloat == nil {
						ret = append(ret, false)
						continue
					}
					ret = append(ret, sFloat.(float64) >= compFloat.(float64))
				}
				return ret, nil
			}
			if Len(s) != Len(comparando) {
				return nil, errors.New("Can't compare Series: Different dimensions")
			}
			for i := 0; i < Len(s); i++ {
				sFloat := elements[i].ToFloat().Val()
				compFloat := compElements[i].ToFloat().Val()
				if sFloat == nil || compFloat == nil {
					ret = append(ret, false)
					continue
				}
				ret = append(ret, sFloat.(float64) >= compFloat.(float64))
			}
			return ret, nil
		case "<":
			if Len(comparando) == 1 {
				compFloat := compElements[0].ToFloat().Val()
				for _, v := range elements {
					sFloat := v.ToFloat().Val()
					if sFloat == nil || compFloat == nil {
						ret = append(ret, false)
						continue
					}
					ret = append(ret, sFloat.(float64) < compFloat.(float64))
				}
				return ret, nil
			}
			if Len(s) != Len(comparando) {
				return nil, errors.New("Can't compare Series: Different dimensions")
			}
			for i := 0; i < Len(s); i++ {
				sFloat := elements[i].ToFloat().Val()
				compFloat := compElements[i].ToFloat().Val()
				if sFloat == nil || compFloat == nil {
					ret = append(ret, false)
					continue
				}
				ret = append(ret, sFloat.(float64) < compFloat.(float64))
			}
			return ret, nil
		case "<=":
			if Len(comparando) == 1 {
				compFloat := compElements[0].ToFloat().Val()
				for _, v := range elements {
					sFloat := v.ToFloat().Val()
					if sFloat == nil || compFloat == nil {
						ret = append(ret, false)
						continue
					}
					ret = append(ret, sFloat.(float64) <= compFloat.(float64))
				}
				return ret, nil
			}
			if Len(s) != Len(comparando) {
				return nil, errors.New("Can't compare Series: Different dimensions")
			}
			for i := 0; i < Len(s); i++ {
				sFloat := elements[i].ToFloat().Val()
				compFloat := compElements[i].ToFloat().Val()
				if sFloat == nil || compFloat == nil {
					ret = append(ret, false)
					continue
				}
				ret = append(ret, sFloat.(float64) <= compFloat.(float64))
			}
			return ret, nil
		case "in":
			for _, v := range elements {
				sFloat := v.ToFloat().Val()
				found := false
				for _, w := range compElements {
					compFloat := w.ToFloat().Val()
					if sFloat == nil || compFloat == nil {
						continue
					}
					if sFloat.(float64) == compFloat.(float64) {
						found = true
						break
					}
				}
				ret = append(ret, found)
			}
			return ret, nil
		default:
			return nil, errors.New("Unknown comparator")
		}

	case "bool":
		elements := s.elements.(BoolElements)
		ret := []bool{}
		comparando := Bools(comparando)
		compElements := comparando.elements.(BoolElements)
		switch comparator {
		case "==":
			if Len(comparando) == 1 {
				for _, v := range elements {
					ret = append(ret, v.Eq(compElements[0]))
				}
				return ret, nil
			}
			if Len(s) != Len(comparando) {
				return nil, errors.New("Can't compare Series: Different dimensions")
			}
			for i := 0; i < Len(s); i++ {
				ret = append(ret, elements[i].Eq(compElements[i]))
			}
			return ret, nil
		case "!=":
			if Len(comparando) == 1 {
				for _, v := range elements {
					ret = append(ret, !v.Eq(compElements[0]))
				}
				return ret, nil
			}
			if Len(s) != Len(comparando) {
				return nil, errors.New("Can't compare Series: Different dimensions")
			}
			for i := 0; i < Len(s); i++ {
				ret = append(ret, !elements[i].Eq(compElements[i]))
			}
			return ret, nil
		case ">":
			if Len(comparando) == 1 {
				compBool := compElements[0].ToInt().Val()
				for _, v := range elements {
					sBool := v.ToInt().Val()
					if sBool == nil || compBool == nil {
						ret = append(ret, false)
						continue
					}
					ret = append(ret, sBool.(int) > compBool.(int))
				}
				return ret, nil
			}
			if Len(s) != Len(comparando) {
				return nil, errors.New("Can't compare Series: Different dimensions")
			}
			for i := 0; i < Len(s); i++ {
				sBool := elements[i].ToInt().Val()
				compBool := compElements[i].ToInt().Val()
				if sBool == nil || compBool == nil {
					ret = append(ret, false)
					continue
				}
				ret = append(ret, sBool.(int) > compBool.(int))
			}
			return ret, nil
		case ">=":
			if Len(comparando) == 1 {
				compBool := compElements[0].ToInt().Val()
				for _, v := range elements {
					sBool := v.ToInt().Val()
					if sBool == nil || compBool == nil {
						ret = append(ret, false)
						continue
					}
					ret = append(ret, sBool.(int) >= compBool.(int))
				}
				return ret, nil
			}
			if Len(s) != Len(comparando) {
				return nil, errors.New("Can't compare Series: Different dimensions")
			}
			for i := 0; i < Len(s); i++ {
				sBool := elements[i].ToInt().Val()
				compBool := compElements[i].ToInt().Val()
				if sBool == nil || compBool == nil {
					ret = append(ret, false)
					continue
				}
				ret = append(ret, sBool.(int) >= compBool.(int))
			}
			return ret, nil
		case "<":
			if Len(comparando) == 1 {
				compBool := compElements[0].ToInt().Val()
				for _, v := range elements {
					sBool := v.ToInt().Val()
					if sBool == nil || compBool == nil {
						ret = append(ret, false)
						continue
					}
					ret = append(ret, sBool.(int) < compBool.(int))
				}
				return ret, nil
			}
			if Len(s) != Len(comparando) {
				return nil, errors.New("Can't compare Series: Different dimensions")
			}
			for i := 0; i < Len(s); i++ {
				sBool := elements[i].ToInt().Val()
				compBool := compElements[i].ToInt().Val()
				if sBool == nil || compBool == nil {
					ret = append(ret, false)
					continue
				}
				ret = append(ret, sBool.(int) < compBool.(int))
			}
			return ret, nil
		case "<=":
			if Len(comparando) == 1 {
				compBool := compElements[0].ToInt().Val()
				for _, v := range elements {
					sBool := v.ToInt().Val()
					if sBool == nil || compBool == nil {
						ret = append(ret, false)
						continue
					}
					ret = append(ret, sBool.(int) <= compBool.(int))
				}
				return ret, nil
			}
			if Len(s) != Len(comparando) {
				return nil, errors.New("Can't compare Series: Different dimensions")
			}
			for i := 0; i < Len(s); i++ {
				sBool := elements[i].ToInt().Val()
				compBool := compElements[i].ToInt().Val()
				if sBool == nil || compBool == nil {
					ret = append(ret, false)
					continue
				}
				ret = append(ret, sBool.(int) <= compBool.(int))
			}
			return ret, nil
		case "in":
			for _, v := range elements {
				sBool := v.ToBool().Val()
				found := false
				for _, w := range compElements {
					compBool := w.ToBool().Val()
					if sBool == nil || compBool == nil {
						continue
					}
					if sBool.(bool) == compBool.(bool) {
						found = true
						break
					}
				}
				ret = append(ret, found)
			}
			return ret, nil
		default:
			return nil, errors.New("Unknown comparator")
		}

	}
	return nil, nil
}

// All custom type definitions:
// ============================

// All Eq() methods
// ====================

// All Records() methods
// ====================

func (s Series) Records() []string {
	return s.elements.Records()
}

// All String() methods
// ====================

func (s Series) String() string {
	return fmt.Sprint(s.elements)
}

// All Int() methods
// ====================

// All Float() methods
// ====================

// All Bool() methods
// ====================
// All Copy() methods
// ====================

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

// All IsNA() methods
// ====================
// TODO: IsNA for a Series will return a boolean Series indicating which of the given elements is NA

// Constructors
// ============

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

// Extra Series functions
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
