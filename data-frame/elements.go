package df

import (
	"errors"
	"reflect"
	"strconv"
	"strings"
)

type seriesElements interface {
	Copy() seriesElements
	Records() []string
	Elem(int) elementInterface
	Append(...interface{}) seriesElements
	Set(int, elementValue) (seriesElements, error)
}

type stringElements []String
type intElements []Int
type floatElements []Float
type boolElements []Bool

func (s stringElements) Set(i int, val elementValue) (seriesElements, error) {
	if i >= len(s) || i < 0 {
		return nil, errors.New("Index out of bounds")
	}
	switch val.(type) {
	case int:
		v := strconv.Itoa(val.(int))
		s[i] = String{&v}
	case float64:
		v := strconv.FormatFloat(val.(float64), 'f', 6, 64)
		s[i] = String{&v}
	case string:
		v := val.(string)
		s[i] = String{&v}
	case bool:
		b := val.(bool)
		if b {
			v := "true"
			s[i] = String{&v}
		} else {
			v := "false"
			s[i] = String{&v}
		}
	case nil:
		s[i] = String{nil}
	case Series:
		series := val.(Series)
		if Len(series) != 1 {
			return nil, errors.New("Non unit Series")
		}
		v := series.elem(0).ToString()
		s[i] = v
	default:
		ifElem := reflect.TypeOf((*elementInterface)(nil)).Elem()
		rv := reflect.ValueOf(val)
		switch reflect.TypeOf(val).Kind() {
		case reflect.Slice:
			if rv.Len() != 1 {
				return nil, errors.New("Non unit slice")
			}
			if rv.Index(0).Type().Implements(ifElem) {
				m := rv.Index(0).MethodByName("ToString").
					Call([]reflect.Value{})
				j := m[0].Interface().(String)
				s[i] = j
			} else {
				s[i] = String{nil}
			}
		default:
			if rv.Type().Implements(ifElem) {
				m := rv.MethodByName("ToString").Call([]reflect.Value{})
				j := m[0].Interface().(String)
				s[i] = j
			} else {
				s[i] = String{nil}
			}
		}
	}
	return s, nil
}
func (s intElements) Set(i int, val elementValue) (seriesElements, error) {
	if i >= len(s) || i < 0 {
		return nil, errors.New("Index out of bounds")
	}
	switch val.(type) {
	case int:
		v := val.(int)
		s[i] = Int{&v}
	case float64:
		v := int(val.(float64))
		s[i] = Int{&v}
	case string:
		v, err := strconv.Atoi(val.(string))
		if err != nil {
			return nil, err
		} else {
			s[i] = Int{&v}
		}
	case bool:
		b := val.(bool)
		var v int
		if b {
			v = 1
		} else {
			v = 0
		}
		s[i] = Int{&v}
	case nil:
		s[i] = Int{nil}
	case Series:
		series := val.(Series)
		if Len(series) != 1 {
			return nil, errors.New("Non unit Series")
		}
		v := series.elem(0).ToInt()
		s[i] = v
	default:
		rv := reflect.ValueOf(val)
		ifElem := reflect.TypeOf((*elementInterface)(nil)).Elem()
		switch reflect.TypeOf(val).Kind() {
		case reflect.Slice:
			if rv.Len() != 1 {
				return nil, errors.New("Non unit slice")
			}
			if rv.Index(0).Type().Implements(ifElem) {
				m := rv.Index(0).MethodByName("ToInt").
					Call([]reflect.Value{})
				j := m[0].Interface().(Int)
				s[i] = j
			} else {
				s[i] = Int{nil}
			}
		default:
			if rv.Type().Implements(ifElem) {
				m := rv.MethodByName("ToInt").Call([]reflect.Value{})
				j := m[0].Interface().(Int)
				s[i] = j
			} else {
				s[i] = Int{nil}
			}
		}
	}
	return s, nil
}
func (s floatElements) Set(i int, val elementValue) (seriesElements, error) {
	if i >= len(s) || i < 0 {
		return nil, errors.New("Index out of bounds")
	}
	switch val.(type) {
	case int:
		v := float64(val.(int))
		s[i] = Float{&v}
	case float64:
		v := val.(float64)
		s[i] = Float{&v}
	case string:
		v, err := strconv.ParseFloat(val.(string), 64)
		if err != nil {
			return nil, errors.New(err.Error())
		}
		s[i] = Float{&v}
	case bool:
		b := val.(bool)
		var v float64
		if b {
			v = 1.0
		} else {
			v = 0.0
		}
		s[i] = Float{&v}
	case nil:
		s[i] = Float{nil}
	case Series:
		series := val.(Series)
		if Len(series) != 1 {
			return nil, errors.New("Non unit Series")
		}
		v := series.elem(0).ToFloat()
		s[i] = v
	default:
		rv := reflect.ValueOf(val)
		ifElem := reflect.TypeOf((*elementInterface)(nil)).Elem()
		switch reflect.TypeOf(val).Kind() {
		case reflect.Slice:
			if rv.Len() != 1 {
				return nil, errors.New("Non unit slice")
			}
			if rv.Index(0).Type().Implements(ifElem) {
				m := rv.Index(0).MethodByName("ToFloat").
					Call([]reflect.Value{})
				j := m[0].Interface().(Float)
				s[i] = j
			} else {
				s[i] = Float{nil}
			}
		default:
			if rv.Type().Implements(ifElem) {
				m := rv.MethodByName("ToFloat").Call([]reflect.Value{})
				j := m[0].Interface().(Float)
				s[i] = j
			} else {
				s[i] = Float{nil}
			}
		}
	}
	return s, nil
}
func (s boolElements) Set(i int, val elementValue) (seriesElements, error) {
	if i >= len(s) || i < 0 {
		return nil, errors.New("Index out of bounds")
	}
	switch val.(type) {
	case int:
		v := val.(int)
		var b bool
		if v > 0 {
			b = true
		}
		s[i] = Bool{&b}
	case float64:
		v := val.(float64)
		var b bool
		if v > 0 {
			b = true
		}
		s[i] = Bool{&b}
	case string:
		v := val.(string)
		var b bool
		if strings.ToLower(v) == "true" ||
			strings.ToLower(v) == "t" {
			b = true
		} else if strings.ToLower(v) == "false" ||
			strings.ToLower(v) == "f" {
			b = false
		} else {
			s[i] = Bool{nil}
			return s, nil
		}
		s[i] = Bool{&b}
	case bool:
		v := val.(bool)
		s[i] = Bool{&v}
	case nil:
		s[i] = Bool{nil}
	case Series:
		series := val.(Series)
		if Len(series) != 1 {
			return nil, errors.New("Non unit Series")
		}
		v := series.elem(0).ToBool()
		s[i] = v
	default:
		rv := reflect.ValueOf(val)
		ifElem := reflect.TypeOf((*elementInterface)(nil)).Elem()
		switch reflect.TypeOf(val).Kind() {
		case reflect.Slice:
			if rv.Len() != 1 {
				return nil, errors.New("Non unit Slice")
			}
			if rv.Index(0).Type().Implements(ifElem) {
				m := rv.Index(0).MethodByName("ToBool").
					Call([]reflect.Value{})
				j := m[0].Interface().(Bool)
				s[i] = j
			} else {
				s[i] = Bool{nil}
			}
		default:
			if rv.Type().Implements(ifElem) {
				m := rv.MethodByName("ToBool").Call([]reflect.Value{})
				j := m[0].Interface().(Bool)
				s[i] = j
			} else {
				s[i] = Bool{nil}
			}
		}
	}
	return s, nil
}

// Records return the elements as a slice of strings
func (s stringElements) Records() []string {
	arr := []string{}
	for _, v := range s {
		arr = append(arr, v.String())
	}
	return arr
}
func (s intElements) Records() []string {
	arr := []string{}
	for _, v := range s {
		arr = append(arr, v.String())
	}
	return arr
}
func (s floatElements) Records() []string {
	arr := []string{}
	for _, v := range s {
		arr = append(arr, v.String())
	}
	return arr
}
func (s boolElements) Records() []string {
	arr := []string{}
	for _, v := range s {
		arr = append(arr, v.String())
	}
	return arr
}

// Elem returns the Element at index i
func (s stringElements) Elem(i int) elementInterface {
	if i >= len(s) || i < 0 {
		return nil
	}
	return s[i]
}
func (s intElements) Elem(i int) elementInterface {
	if i >= len(s) || i < 0 {
		return nil
	}
	return s[i]
}
func (s floatElements) Elem(i int) elementInterface {
	if i >= len(s) || i < 0 {
		return nil
	}
	return s[i]
}
func (s boolElements) Elem(i int) elementInterface {
	if i >= len(s) || i < 0 {
		return nil
	}
	return s[i]
}

func (elements stringElements) Append(args ...interface{}) seriesElements {
	for _, v := range args {
		switch v.(type) {
		case []int:
			varr := v.([]int)
			for k := range varr {
				s := strconv.Itoa(varr[k])
				elements = append(elements, String{&s})
			}
		case int:
			s := strconv.Itoa(v.(int))
			elements = append(elements, String{&s})
		case []float64:
			varr := v.([]float64)
			for k := range varr {
				s := strconv.FormatFloat(varr[k], 'f', 6, 64)
				elements = append(elements, String{&s})
			}
		case float64:
			s := strconv.FormatFloat(v.(float64), 'f', 6, 64)
			elements = append(elements, String{&s})
		case []string:
			varr := v.([]string)
			for k := range varr {
				s := varr[k]
				elements = append(elements, String{&s})
			}
		case string:
			s := v.(string)
			elements = append(elements, String{&s})
		case []bool:
			varr := v.([]bool)
			for k := range varr {
				b := varr[k]
				if b {
					s := "true"
					elements = append(elements, String{&s})
				} else {
					s := "false"
					elements = append(elements, String{&s})
				}
			}
		case bool:
			b := v.(bool)
			if b {
				s := "true"
				elements = append(elements, String{&s})
			} else {
				s := "false"
				elements = append(elements, String{&s})
			}
		case nil:
			elements = append(elements, String{nil})
		case Series:
			s := v.(Series)
			elems := s.elements.Copy()
			elements = elements.Append(elems).(stringElements)
		default:
			// This should only happen if v (or its elements in case of a slice)
			// implements Stringer.
			ifElem := reflect.TypeOf((*elementInterface)(nil)).Elem()
			s := reflect.ValueOf(v)
			switch reflect.TypeOf(v).Kind() {
			case reflect.Slice:
				if s.Len() > 0 {
					for i := 0; i < s.Len(); i++ {
						if s.Index(i).Type().Implements(ifElem) {
							m := s.Index(i).MethodByName("ToString").
								Call([]reflect.Value{})
							j := m[0].Interface().(String)
							elements = append(elements, j)
						} else {
							elements = append(elements, String{nil})
						}
					}
				}
			default:
				if s.Type().Implements(ifElem) {
					m := s.MethodByName("ToString").Call([]reflect.Value{})
					j := m[0].Interface().(String)
					elements = append(elements, j)
				} else {
					elements = append(elements, String{nil})
				}
			}
		}
	}
	return elements
}
func (elements intElements) Append(args ...interface{}) seriesElements {
	for _, v := range args {
		switch v.(type) {
		case []int:
			varr := v.([]int)
			for k := range varr {
				elements = append(elements, Int{&varr[k]})
			}
		case int:
			i := v.(int)
			elements = append(elements, Int{&i})
		case []float64:
			varr := v.([]float64)
			for k := range varr {
				f := varr[k]
				i := int(f)
				elements = append(elements, Int{&i})
			}
		case float64:
			f := v.(float64)
			i := int(f)
			elements = append(elements, Int{&i})
		case []string:
			varr := v.([]string)
			for k := range varr {
				s := varr[k]
				i, err := strconv.Atoi(s)
				if err != nil {
					elements = append(elements, Int{nil})
				} else {
					elements = append(elements, Int{&i})
				}
			}
		case string:
			i, err := strconv.Atoi(v.(string))
			if err != nil {
				elements = append(elements, Int{nil})
			} else {
				elements = append(elements, Int{&i})
			}
		case []bool:
			varr := v.([]bool)
			for k := range varr {
				b := varr[k]
				if b {
					i := 1
					elements = append(elements, Int{&i})
				} else {
					i := 0
					elements = append(elements, Int{&i})
				}
			}
		case bool:
			b := v.(bool)
			if b {
				i := 1
				elements = append(elements, Int{&i})
			} else {
				i := 0
				elements = append(elements, Int{&i})
			}
		case nil:
			elements = append(elements, Int{nil})
		case Series:
			s := v.(Series)
			elems := s.elements.Copy()
			elements = elements.Append(elems).(intElements)
		default:
			s := reflect.ValueOf(v)
			ifElem := reflect.TypeOf((*elementInterface)(nil)).Elem()
			switch reflect.TypeOf(v).Kind() {
			case reflect.Slice:
				if s.Len() > 0 {
					for i := 0; i < s.Len(); i++ {
						if s.Index(i).Type().Implements(ifElem) {
							m := s.Index(i).MethodByName("ToInt").
								Call([]reflect.Value{})
							j := m[0].Interface().(Int)
							elements = append(elements, j)
						} else {
							elements = append(elements, Int{nil})
						}
					}
				}
			default:
				if s.Type().Implements(ifElem) {
					m := s.MethodByName("ToInt").Call([]reflect.Value{})
					j := m[0].Interface().(Int)
					elements = append(elements, j)
				} else {
					elements = append(elements, Int{nil})
				}
			}
		}
	}
	return elements
}
func (elements floatElements) Append(args ...interface{}) seriesElements {
	for _, v := range args {
		switch v.(type) {
		case []int:
			varr := v.([]int)
			for k := range varr {
				i := varr[k]
				f := float64(i)
				elements = append(elements, Float{&f})
			}
		case int:
			i := v.(int)
			f := float64(i)
			elements = append(elements, Float{&f})
		case []float64:
			varr := v.([]float64)
			for k := range varr {
				f := varr[k]
				elements = append(elements, Float{&f})
			}
		case float64:
			f := v.(float64)
			elements = append(elements, Float{&f})
		case []string:
			varr := v.([]string)
			for k := range varr {
				s := varr[k]
				f, err := strconv.ParseFloat(s, 64)
				if err != nil {
					elements = append(elements, Float{nil})
				} else {
					elements = append(elements, Float{&f})
				}
			}
		case string:
			f, err := strconv.ParseFloat(v.(string), 64)
			if err != nil {
				elements = append(elements, Float{nil})
			} else {
				elements = append(elements, Float{&f})
			}
		case []bool:
			varr := v.([]bool)
			for k := range varr {
				b := varr[k]
				if b {
					i := 1.0
					elements = append(elements, Float{&i})
				} else {
					i := 0.0
					elements = append(elements, Float{&i})
				}
			}
		case bool:
			b := v.(bool)
			if b {
				i := 1.0
				elements = append(elements, Float{&i})
			} else {
				i := 0.0
				elements = append(elements, Float{&i})
			}
		case nil:
			elements = append(elements, Float{nil})
		case Series:
			s := v.(Series)
			elems := s.elements.Copy()
			elements = elements.Append(elems).(floatElements)
		default:
			s := reflect.ValueOf(v)
			ifElem := reflect.TypeOf((*elementInterface)(nil)).Elem()
			switch reflect.TypeOf(v).Kind() {
			case reflect.Slice:
				if s.Len() > 0 {
					for i := 0; i < s.Len(); i++ {
						if s.Index(i).Type().Implements(ifElem) {
							m := s.Index(i).MethodByName("ToFloat").
								Call([]reflect.Value{})
							j := m[0].Interface().(Float)
							elements = append(elements, j)
						} else {
							elements = append(elements, Float{nil})
						}
					}
				}
			default:
				if s.Type().Implements(ifElem) {
					m := s.MethodByName("ToFloat").Call([]reflect.Value{})
					j := m[0].Interface().(Float)
					elements = append(elements, j)
				} else {
					elements = append(elements, Float{nil})
				}
			}
		}
	}
	return elements
}
func (elements boolElements) Append(args ...interface{}) seriesElements {
	for _, v := range args {
		switch v.(type) {
		case []int:
			varr := v.([]int)
			for k := range varr {
				i := varr[k]
				t := true
				f := false
				if i > 0 {
					elements = append(elements, Bool{&t})
				} else {
					elements = append(elements, Bool{&f})
				}
			}
		case int:
			i := v.(int)
			t := true
			f := false
			if i > 0 {
				elements = append(elements, Bool{&t})
			} else {
				elements = append(elements, Bool{&f})
			}
		case []float64:
			varr := v.([]float64)
			for k := range varr {
				i := varr[k]
				t := true
				f := false
				if i > 0 {
					elements = append(elements, Bool{&t})
				} else {
					elements = append(elements, Bool{&f})
				}
			}
		case float64:
			i := v.(float64)
			t := true
			f := false
			if i > 0 {
				elements = append(elements, Bool{&t})
			} else {
				elements = append(elements, Bool{&f})
			}
		case []string:
			varr := v.([]string)
			for k := range varr {
				i := varr[k]
				t := true
				f := false
				if strings.ToLower(i) == "true" ||
					strings.ToLower(i) == "t" {
					elements = append(elements, Bool{&t})
				} else if strings.ToLower(i) == "false" ||
					strings.ToLower(i) == "f" {
					elements = append(elements, Bool{&f})
				} else {
					elements = append(elements, Bool{nil})
				}
			}
		case string:
			i := v.(string)
			t := true
			f := false
			if strings.ToLower(i) == "true" ||
				strings.ToLower(i) == "t" {
				elements = append(elements, Bool{&t})
			} else if strings.ToLower(i) == "false" ||
				strings.ToLower(i) == "f" {
				elements = append(elements, Bool{&f})
			} else {
				elements = append(elements, Bool{nil})
			}
		case []bool:
			varr := v.([]bool)
			for k := range varr {
				i := varr[k]
				t := true
				f := false
				if i {
					elements = append(elements, Bool{&t})
				} else {
					elements = append(elements, Bool{&f})
				}
			}
		case bool:
			i := v.(bool)
			t := true
			f := false
			if i {
				elements = append(elements, Bool{&t})
			} else {
				elements = append(elements, Bool{&f})
			}
		case nil:
			elements = append(elements, Bool{nil})
		case Series:
			s := v.(Series)
			elems := s.elements.Copy()
			elements = elements.Append(elems).(boolElements)
		default:
			s := reflect.ValueOf(v)
			ifElem := reflect.TypeOf((*elementInterface)(nil)).Elem()
			switch reflect.TypeOf(v).Kind() {
			case reflect.Slice:
				if s.Len() > 0 {
					for i := 0; i < s.Len(); i++ {
						if s.Index(i).Type().Implements(ifElem) {
							m := s.Index(i).MethodByName("ToBool").
								Call([]reflect.Value{})
							j := m[0].Interface().(Bool)
							elements = append(elements, j)
						} else {
							elements = append(elements, Bool{nil})
						}
					}
				}
			default:
				if s.Type().Implements(ifElem) {
					m := s.MethodByName("ToBool").Call([]reflect.Value{})
					j := m[0].Interface().(Bool)
					elements = append(elements, j)
				} else {
					elements = append(elements, Bool{nil})
				}
			}
		}
	}
	return elements
}

// Copy the elements of Elements
func (s stringElements) Copy() seriesElements {
	var elements stringElements
	for _, elem := range s {
		elements = append(elements, elem.Copy())
	}
	return elements
}
func (s intElements) Copy() seriesElements {
	var elements intElements
	for _, elem := range s {
		elements = append(elements, elem.Copy())
	}
	return elements
}
func (s floatElements) Copy() seriesElements {
	var elements floatElements
	for _, elem := range s {
		elements = append(elements, elem.Copy())
	}
	return elements
}
func (s boolElements) Copy() seriesElements {
	var elements boolElements
	for _, elem := range s {
		elements = append(elements, elem.Copy())
	}
	return elements
}

func (s stringElements) String() string {
	return strings.Join(s.Records(), " ")
}
func (s intElements) String() string {
	return strings.Join(s.Records(), " ")
}
func (s floatElements) String() string {
	return strings.Join(s.Records(), " ")
}
func (s boolElements) String() string {
	return strings.Join(s.Records(), " ")
}
