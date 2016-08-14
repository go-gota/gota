package df

import (
	"reflect"
	"strconv"
	"strings"
)

type Elements interface {
	Copy() Elements
	Records() []string
	Elem(int) Element
	Append(...interface{}) Elements
}

type StringElements []String
type IntElements []Int
type FloatElements []Float
type BoolElements []Bool

// Records return the elements as a slice of strings
func (s StringElements) Records() []string {
	arr := []string{}
	for _, v := range s {
		arr = append(arr, v.String())
	}
	return arr
}
func (s IntElements) Records() []string {
	arr := []string{}
	for _, v := range s {
		arr = append(arr, v.String())
	}
	return arr
}
func (s FloatElements) Records() []string {
	arr := []string{}
	for _, v := range s {
		arr = append(arr, v.String())
	}
	return arr
}
func (s BoolElements) Records() []string {
	arr := []string{}
	for _, v := range s {
		arr = append(arr, v.String())
	}
	return arr
}

// Elem returns the Element at index i
func (s StringElements) Elem(i int) Element {
	if i > len(s) || i < 0 {
		return nil
	}
	return s[i]
}
func (s IntElements) Elem(i int) Element {
	if i > len(s) || i < 0 {
		return nil
	}
	return s[i]
}
func (s FloatElements) Elem(i int) Element {
	if i > len(s) || i < 0 {
		return nil
	}
	return s[i]
}
func (s BoolElements) Elem(i int) Element {
	if i > len(s) || i < 0 {
		return nil
	}
	return s[i]
}

func (elements StringElements) Append(args ...interface{}) Elements {
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
			elements = elements.Append(elems).(StringElements)
		default:
			// This should only happen if v (or its elements in case of a slice)
			// implements Stringer.
			ifElem := reflect.TypeOf((*Element)(nil)).Elem()
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
func (elements IntElements) Append(args ...interface{}) Elements {
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
			elements = elements.Append(elems).(IntElements)
		default:
			s := reflect.ValueOf(v)
			ifElem := reflect.TypeOf((*Element)(nil)).Elem()
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

func (elements FloatElements) Append(args ...interface{}) Elements {
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
			elements = elements.Append(elems).(FloatElements)
		default:
			s := reflect.ValueOf(v)
			ifElem := reflect.TypeOf((*Element)(nil)).Elem()
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

func (elements BoolElements) Append(args ...interface{}) Elements {
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
			elements = elements.Append(elems).(BoolElements)
		default:
			s := reflect.ValueOf(v)
			ifElem := reflect.TypeOf((*Element)(nil)).Elem()
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
func (s StringElements) Copy() Elements {
	var elements StringElements
	for _, elem := range s {
		elements = append(elements, elem.Copy())
	}
	return elements
}
func (s IntElements) Copy() Elements {
	var elements IntElements
	for _, elem := range s {
		elements = append(elements, elem.Copy())
	}
	return elements
}
func (s FloatElements) Copy() Elements {
	var elements FloatElements
	for _, elem := range s {
		elements = append(elements, elem.Copy())
	}
	return elements
}
func (s BoolElements) Copy() Elements {
	var elements BoolElements
	for _, elem := range s {
		elements = append(elements, elem.Copy())
	}
	return elements
}

func (s StringElements) String() string {
	return strings.Join(s.Records(), " ")
}
func (s IntElements) String() string {
	return strings.Join(s.Records(), " ")
}
func (s FloatElements) String() string {
	return strings.Join(s.Records(), " ")
}
func (s BoolElements) String() string {
	return strings.Join(s.Records(), " ")
}
