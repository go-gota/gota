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

type stringElements []stringElement
type intElements []intElement
type floatElements []floatElement
type boolElements []boolElement

func (s stringElements) Set(i int, val elementValue) (seriesElements, error) {
	if i >= len(s) || i < 0 {
		return nil, errors.New("Index out of bounds")
	}
	switch val.(type) {
	case int:
		v := strconv.Itoa(val.(int))
		s[i] = stringElement{&v}
	case float64:
		v := strconv.FormatFloat(val.(float64), 'f', 6, 64)
		s[i] = stringElement{&v}
	case string:
		v := val.(string)
		s[i] = stringElement{&v}
	case bool:
		b := val.(bool)
		if b {
			v := "true"
			s[i] = stringElement{&v}
		} else {
			v := "false"
			s[i] = stringElement{&v}
		}
	case nil:
		s[i] = stringElement{nil}
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
				j := m[0].Interface().(stringElement)
				s[i] = j
			} else {
				s[i] = stringElement{nil}
			}
		default:
			if rv.Type().Implements(ifElem) {
				m := rv.MethodByName("ToString").Call([]reflect.Value{})
				j := m[0].Interface().(stringElement)
				s[i] = j
			} else {
				s[i] = stringElement{nil}
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
		s[i] = intElement{&v}
	case float64:
		v := int(val.(float64))
		s[i] = intElement{&v}
	case string:
		v, err := strconv.Atoi(val.(string))
		if err != nil {
			return nil, err
		} else {
			s[i] = intElement{&v}
		}
	case bool:
		b := val.(bool)
		var v int
		if b {
			v = 1
		} else {
			v = 0
		}
		s[i] = intElement{&v}
	case nil:
		s[i] = intElement{nil}
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
				j := m[0].Interface().(intElement)
				s[i] = j
			} else {
				s[i] = intElement{nil}
			}
		default:
			if rv.Type().Implements(ifElem) {
				m := rv.MethodByName("ToInt").Call([]reflect.Value{})
				j := m[0].Interface().(intElement)
				s[i] = j
			} else {
				s[i] = intElement{nil}
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
		s[i] = floatElement{&v}
	case float64:
		v := val.(float64)
		s[i] = floatElement{&v}
	case string:
		v, err := strconv.ParseFloat(val.(string), 64)
		if err != nil {
			return nil, errors.New(err.Error())
		}
		s[i] = floatElement{&v}
	case bool:
		b := val.(bool)
		var v float64
		if b {
			v = 1.0
		} else {
			v = 0.0
		}
		s[i] = floatElement{&v}
	case nil:
		s[i] = floatElement{nil}
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
				j := m[0].Interface().(floatElement)
				s[i] = j
			} else {
				s[i] = floatElement{nil}
			}
		default:
			if rv.Type().Implements(ifElem) {
				m := rv.MethodByName("ToFloat").Call([]reflect.Value{})
				j := m[0].Interface().(floatElement)
				s[i] = j
			} else {
				s[i] = floatElement{nil}
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
		s[i] = boolElement{&b}
	case float64:
		v := val.(float64)
		var b bool
		if v > 0 {
			b = true
		}
		s[i] = boolElement{&b}
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
			s[i] = boolElement{nil}
			return s, nil
		}
		s[i] = boolElement{&b}
	case bool:
		v := val.(bool)
		s[i] = boolElement{&v}
	case nil:
		s[i] = boolElement{nil}
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
				j := m[0].Interface().(boolElement)
				s[i] = j
			} else {
				s[i] = boolElement{nil}
			}
		default:
			if rv.Type().Implements(ifElem) {
				m := rv.MethodByName("ToBool").Call([]reflect.Value{})
				j := m[0].Interface().(boolElement)
				s[i] = j
			} else {
				s[i] = boolElement{nil}
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
				elements = append(elements, stringElement{&s})
			}
		case int:
			s := strconv.Itoa(v.(int))
			elements = append(elements, stringElement{&s})
		case []float64:
			varr := v.([]float64)
			for k := range varr {
				s := strconv.FormatFloat(varr[k], 'f', 6, 64)
				elements = append(elements, stringElement{&s})
			}
		case float64:
			s := strconv.FormatFloat(v.(float64), 'f', 6, 64)
			elements = append(elements, stringElement{&s})
		case []string:
			varr := v.([]string)
			for k := range varr {
				s := varr[k]
				elements = append(elements, stringElement{&s})
			}
		case string:
			s := v.(string)
			elements = append(elements, stringElement{&s})
		case []bool:
			varr := v.([]bool)
			for k := range varr {
				b := varr[k]
				if b {
					s := "true"
					elements = append(elements, stringElement{&s})
				} else {
					s := "false"
					elements = append(elements, stringElement{&s})
				}
			}
		case bool:
			b := v.(bool)
			if b {
				s := "true"
				elements = append(elements, stringElement{&s})
			} else {
				s := "false"
				elements = append(elements, stringElement{&s})
			}
		case nil:
			elements = append(elements, stringElement{nil})
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
							j := m[0].Interface().(stringElement)
							elements = append(elements, j)
						} else {
							elements = append(elements, stringElement{nil})
						}
					}
				}
			default:
				if s.Type().Implements(ifElem) {
					m := s.MethodByName("ToString").Call([]reflect.Value{})
					j := m[0].Interface().(stringElement)
					elements = append(elements, j)
				} else {
					elements = append(elements, stringElement{nil})
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
				elements = append(elements, intElement{&varr[k]})
			}
		case int:
			i := v.(int)
			elements = append(elements, intElement{&i})
		case []float64:
			varr := v.([]float64)
			for k := range varr {
				f := varr[k]
				i := int(f)
				elements = append(elements, intElement{&i})
			}
		case float64:
			f := v.(float64)
			i := int(f)
			elements = append(elements, intElement{&i})
		case []string:
			varr := v.([]string)
			for k := range varr {
				s := varr[k]
				i, err := strconv.Atoi(s)
				if err != nil {
					elements = append(elements, intElement{nil})
				} else {
					elements = append(elements, intElement{&i})
				}
			}
		case string:
			i, err := strconv.Atoi(v.(string))
			if err != nil {
				elements = append(elements, intElement{nil})
			} else {
				elements = append(elements, intElement{&i})
			}
		case []bool:
			varr := v.([]bool)
			for k := range varr {
				b := varr[k]
				if b {
					i := 1
					elements = append(elements, intElement{&i})
				} else {
					i := 0
					elements = append(elements, intElement{&i})
				}
			}
		case bool:
			b := v.(bool)
			if b {
				i := 1
				elements = append(elements, intElement{&i})
			} else {
				i := 0
				elements = append(elements, intElement{&i})
			}
		case nil:
			elements = append(elements, intElement{nil})
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
							j := m[0].Interface().(intElement)
							elements = append(elements, j)
						} else {
							elements = append(elements, intElement{nil})
						}
					}
				}
			default:
				if s.Type().Implements(ifElem) {
					m := s.MethodByName("ToInt").Call([]reflect.Value{})
					j := m[0].Interface().(intElement)
					elements = append(elements, j)
				} else {
					elements = append(elements, intElement{nil})
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
				elements = append(elements, floatElement{&f})
			}
		case int:
			i := v.(int)
			f := float64(i)
			elements = append(elements, floatElement{&f})
		case []float64:
			varr := v.([]float64)
			for k := range varr {
				f := varr[k]
				elements = append(elements, floatElement{&f})
			}
		case float64:
			f := v.(float64)
			elements = append(elements, floatElement{&f})
		case []string:
			varr := v.([]string)
			for k := range varr {
				s := varr[k]
				f, err := strconv.ParseFloat(s, 64)
				if err != nil {
					elements = append(elements, floatElement{nil})
				} else {
					elements = append(elements, floatElement{&f})
				}
			}
		case string:
			f, err := strconv.ParseFloat(v.(string), 64)
			if err != nil {
				elements = append(elements, floatElement{nil})
			} else {
				elements = append(elements, floatElement{&f})
			}
		case []bool:
			varr := v.([]bool)
			for k := range varr {
				b := varr[k]
				if b {
					i := 1.0
					elements = append(elements, floatElement{&i})
				} else {
					i := 0.0
					elements = append(elements, floatElement{&i})
				}
			}
		case bool:
			b := v.(bool)
			if b {
				i := 1.0
				elements = append(elements, floatElement{&i})
			} else {
				i := 0.0
				elements = append(elements, floatElement{&i})
			}
		case nil:
			elements = append(elements, floatElement{nil})
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
							j := m[0].Interface().(floatElement)
							elements = append(elements, j)
						} else {
							elements = append(elements, floatElement{nil})
						}
					}
				}
			default:
				if s.Type().Implements(ifElem) {
					m := s.MethodByName("ToFloat").Call([]reflect.Value{})
					j := m[0].Interface().(floatElement)
					elements = append(elements, j)
				} else {
					elements = append(elements, floatElement{nil})
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
					elements = append(elements, boolElement{&t})
				} else {
					elements = append(elements, boolElement{&f})
				}
			}
		case int:
			i := v.(int)
			t := true
			f := false
			if i > 0 {
				elements = append(elements, boolElement{&t})
			} else {
				elements = append(elements, boolElement{&f})
			}
		case []float64:
			varr := v.([]float64)
			for k := range varr {
				i := varr[k]
				t := true
				f := false
				if i > 0 {
					elements = append(elements, boolElement{&t})
				} else {
					elements = append(elements, boolElement{&f})
				}
			}
		case float64:
			i := v.(float64)
			t := true
			f := false
			if i > 0 {
				elements = append(elements, boolElement{&t})
			} else {
				elements = append(elements, boolElement{&f})
			}
		case []string:
			varr := v.([]string)
			for k := range varr {
				i := varr[k]
				t := true
				f := false
				if strings.ToLower(i) == "true" ||
					strings.ToLower(i) == "t" {
					elements = append(elements, boolElement{&t})
				} else if strings.ToLower(i) == "false" ||
					strings.ToLower(i) == "f" {
					elements = append(elements, boolElement{&f})
				} else {
					elements = append(elements, boolElement{nil})
				}
			}
		case string:
			i := v.(string)
			t := true
			f := false
			if strings.ToLower(i) == "true" ||
				strings.ToLower(i) == "t" {
				elements = append(elements, boolElement{&t})
			} else if strings.ToLower(i) == "false" ||
				strings.ToLower(i) == "f" {
				elements = append(elements, boolElement{&f})
			} else {
				elements = append(elements, boolElement{nil})
			}
		case []bool:
			varr := v.([]bool)
			for k := range varr {
				i := varr[k]
				t := true
				f := false
				if i {
					elements = append(elements, boolElement{&t})
				} else {
					elements = append(elements, boolElement{&f})
				}
			}
		case bool:
			i := v.(bool)
			t := true
			f := false
			if i {
				elements = append(elements, boolElement{&t})
			} else {
				elements = append(elements, boolElement{&f})
			}
		case nil:
			elements = append(elements, boolElement{nil})
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
							j := m[0].Interface().(boolElement)
							elements = append(elements, j)
						} else {
							elements = append(elements, boolElement{nil})
						}
					}
				}
			default:
				if s.Type().Implements(ifElem) {
					m := s.MethodByName("ToBool").Call([]reflect.Value{})
					j := m[0].Interface().(boolElement)
					elements = append(elements, j)
				} else {
					elements = append(elements, boolElement{nil})
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
