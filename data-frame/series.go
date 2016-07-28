package df

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

type Series struct {
	Name     string   // The name of the series
	Elements Elements // The values of the elements
	names    []string // The names of every element. If empty is an unnamed series
	t        string   // The type of the series
}
type Elements interface {
	String() string
}

func (s Series) Index(indexes interface{}) (*Series, error) {
	switch s.t {
	case "string":
		elements := s.Elements.(StringElements)
		switch indexes.(type) {
		case []int:
			elems := StringElements{}
			for _, v := range indexes.([]int) {
				if v >= len(elements) {
					return nil, errors.New("Index out of range")
				}
				elems = append(elems, elements[v])
			}
			series := Strings(elems)
			return &series, nil
		default:
			return nil, errors.New("Unknown indexing mode")
		}
	case "int":
		elements := s.Elements.(IntElements)
		switch indexes.(type) {
		case []int:
			elems := IntElements{}
			for _, v := range indexes.([]int) {
				if v >= len(elements) {
					return nil, errors.New("Index out of range")
				}
				elems = append(elems, elements[v])
			}
			series := Ints(elems)
			return &series, nil
		default:
			return nil, errors.New("Unknown indexing mode")
		}
	case "float":
		elements := s.Elements.(FloatElements)
		switch indexes.(type) {
		case []int:
			elems := FloatElements{}
			for _, v := range indexes.([]int) {
				if v >= len(elements) {
					return nil, errors.New("Index out of range")
				}
				elems = append(elems, elements[v])
			}
			series := Floats(elems)
			return &series, nil
		default:
			return nil, errors.New("Unknown indexing mode")
		}
	case "bool":
		elements := s.Elements.(BoolElements)
		switch indexes.(type) {
		case []int:
			elems := BoolElements{}
			for _, v := range indexes.([]int) {
				if v >= len(elements) {
					return nil, errors.New("Index out of range")
				}
				elems = append(elems, elements[v])
			}
			series := Bools(elems)
			return &series, nil
		default:
			return nil, errors.New("Unknown indexing mode")
		}
	}
	return nil, errors.New("Unknown Series type")
}

func (s Series) Compare(comparator string, comparando interface{}) ([]bool, error) {
	// TODO: What to do in case of NAs?
	// TODO: Handle logic to convert to series and the logic will only compare
	// between series?
	switch s.t {
	case "string":
		elements := s.Elements.(StringElements)
		ret := []bool{}
		comparando := Strings(comparando)
		compElements := comparando.Elements.(StringElements)
		switch comparator {
		case "==":
			if Len(comparando) == 1 {
				for _, v := range elements {
					ret = append(ret, v.String() == compElements[0].String())
				}
				return ret, nil
			}
			if Len(s) != Len(comparando) {
				return nil, errors.New("Can't compare Series: Different dimensions")
			}
			for i := 0; i < Len(s); i++ {
				ret = append(ret, elements[i].String() == compElements[i].String())
			}
			return ret, nil
		case "!=":
			if Len(comparando) == 1 {
				for _, v := range elements {
					ret = append(ret, v.String() != compElements[0].String())
				}
				return ret, nil
			}
			if Len(s) != Len(comparando) {
				return nil, errors.New("Can't compare Series: Different dimensions")
			}
			for i := 0; i < Len(s); i++ {
				ret = append(ret, elements[i].String() != compElements[i].String())
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
		elements := s.Elements.(IntElements)
		ret := []bool{}
		comparando := Ints(comparando)
		compElements := comparando.Elements.(IntElements)
		switch comparator {
		case "==":
			if Len(comparando) == 1 {
				compInt := compElements[0].Int()
				for _, v := range elements {
					sInt := v.Int()
					if sInt == nil || compInt == nil {
						ret = append(ret, false)
						continue
					}
					ret = append(ret, *sInt == *compInt)
				}
				return ret, nil
			}
			if Len(s) != Len(comparando) {
				return nil, errors.New("Can't compare Series: Different dimensions")
			}
			for i := 0; i < Len(s); i++ {
				sInt := elements[i].Int()
				compInt := compElements[i].Int()
				if sInt == nil || compInt == nil {
					ret = append(ret, false)
					continue
				}
				ret = append(ret, *sInt == *compInt)
			}
			return ret, nil
		case "!=":
			if Len(comparando) == 1 {
				compInt := compElements[0].Int()
				for _, v := range elements {
					sInt := v.Int()
					if sInt == nil || compInt == nil {
						ret = append(ret, true)
						continue
					}
					ret = append(ret, *sInt != *compInt)
				}
				return ret, nil
			}
			if Len(s) != Len(comparando) {
				return nil, errors.New("Can't compare Series: Different dimensions")
			}
			for i := 0; i < Len(s); i++ {
				sInt := elements[i].Int()
				compInt := compElements[i].Int()
				if sInt == nil || compInt == nil {
					ret = append(ret, true)
					continue
				}
				ret = append(ret, *sInt != *compInt)
			}
			return ret, nil
		case ">":
			if Len(comparando) == 1 {
				compInt := compElements[0].Int()
				for _, v := range elements {
					sInt := v.Int()
					if sInt == nil || compInt == nil {
						ret = append(ret, false)
						continue
					}
					ret = append(ret, *sInt > *compInt)
				}
				return ret, nil
			}
			if Len(s) != Len(comparando) {
				return nil, errors.New("Can't compare Series: Different dimensions")
			}
			for i := 0; i < Len(s); i++ {
				sInt := elements[i].Int()
				compInt := compElements[i].Int()
				if sInt == nil || compInt == nil {
					ret = append(ret, false)
					continue
				}
				ret = append(ret, *sInt > *compInt)
			}
			return ret, nil
		case ">=":
			if Len(comparando) == 1 {
				compInt := compElements[0].Int()
				for _, v := range elements {
					sInt := v.Int()
					if sInt == nil || compInt == nil {
						ret = append(ret, false)
						continue
					}
					ret = append(ret, *sInt >= *compInt)
				}
				return ret, nil
			}
			if Len(s) != Len(comparando) {
				return nil, errors.New("Can't compare Series: Different dimensions")
			}
			for i := 0; i < Len(s); i++ {
				sInt := elements[i].Int()
				compInt := compElements[i].Int()
				if sInt == nil || compInt == nil {
					ret = append(ret, false)
					continue
				}
				ret = append(ret, *sInt >= *compInt)
			}
			return ret, nil
		case "<":
			if Len(comparando) == 1 {
				compInt := compElements[0].Int()
				for _, v := range elements {
					sInt := v.Int()
					if sInt == nil || compInt == nil {
						ret = append(ret, false)
						continue
					}
					ret = append(ret, *sInt < *compInt)
				}
				return ret, nil
			}
			if Len(s) != Len(comparando) {
				return nil, errors.New("Can't compare Series: Different dimensions")
			}
			for i := 0; i < Len(s); i++ {
				sInt := elements[i].Int()
				compInt := compElements[i].Int()
				if sInt == nil || compInt == nil {
					ret = append(ret, false)
					continue
				}
				ret = append(ret, *sInt < *compInt)
			}
			return ret, nil
		case "<=":
			if Len(comparando) == 1 {
				compInt := compElements[0].Int()
				for _, v := range elements {
					sInt := v.Int()
					if sInt == nil || compInt == nil {
						ret = append(ret, false)
						continue
					}
					ret = append(ret, *sInt <= *compInt)
				}
				return ret, nil
			}
			if Len(s) != Len(comparando) {
				return nil, errors.New("Can't compare Series: Different dimensions")
			}
			for i := 0; i < Len(s); i++ {
				sInt := elements[i].Int()
				compInt := compElements[i].Int()
				if sInt == nil || compInt == nil {
					ret = append(ret, false)
					continue
				}
				ret = append(ret, *sInt <= *compInt)
			}
			return ret, nil
		case "in":
			for _, v := range elements {
				sInt := v.Int()
				found := false
				for _, w := range compElements {
					compInt := w.Int()
					if sInt == nil || compInt == nil {
						continue
					}
					if *sInt == *compInt {
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

type StringElements []String
type IntElements []Int
type FloatElements []Float
type BoolElements []Bool

// All String() methods
// ====================

func (s StringElements) String() string {
	str := []string{}
	for _, v := range s {
		str = append(str, v.String())
	}
	return strings.Join(str, " ")
}
func (s IntElements) String() string {
	str := []string{}
	for _, v := range s {
		str = append(str, v.String())
	}
	return strings.Join(str, " ")
}
func (s FloatElements) String() string {
	str := []string{}
	for _, v := range s {
		str = append(str, v.String())
	}
	return strings.Join(str, " ")
}
func (s BoolElements) String() string {
	str := []string{}
	for _, v := range s {
		str = append(str, v.String())
	}
	return strings.Join(str, " ")
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

func (s Series) String() string {
	return fmt.Sprint(s.Elements)
}

// All Int() methods
// ====================

// Int returns the integer value of String
func (s String) Int() *int {
	if s.s == nil {
		return nil
	}
	str, err := strconv.Atoi(*s.s)
	if err != nil {
		return nil
	}
	return &str
}

// Int returns the integer value of Int
func (i Int) Int() *int {
	if i.i != nil {
		return i.i
	}
	return nil
}

// Int returns the integer value of Float
func (f Float) Int() *int {
	if f.f != nil {
		i := int(*f.f)
		return &i
	}
	return nil
}

// Int returns the integer value of Bool
func (b Bool) Int() (*int, error) {
	if b.b == nil {
		return nil, errors.New("Empty value")
	}
	if *b.b {
		one := 1
		return &one, nil
	}
	zero := 0
	return &zero, nil
}

// All Float() methods
// ====================

// Float returns the float value of String
func (s String) Float() *float64 {
	if s.s == nil {
		return nil
	}
	f, err := strconv.ParseFloat(*s.s, 64)
	if err != nil {
		return nil
	}
	return &f
}

// Float returns the float value of Int
func (i Int) Float() *float64 {
	if i.i != nil {
		f := float64(*i.i)
		return &f
	}
	return nil
}

// Float returns the float value of Float
func (f Float) Float() *float64 {
	if f.f != nil {
		return f.f
	}
	return nil
}

// Float returns the float value of Bool
func (b Bool) Float() *float64 {
	if b.b == nil {
		return nil
	}
	if *b.b {
		one := 1.0
		return &one
	}
	zero := 0.0
	return &zero
}

// All Bool() methods
// ====================
// Bool returns the bool value of String
func (s String) Bool() *bool {
	if s.s == nil {
		return nil
	}
	t := true
	f := false
	if *s.s == "false" {
		return &f
	}
	if *s.s == "true" {
		return &t
	}
	return nil
}

// Bool returns the bool value of Int
func (i Int) Bool() *bool {
	t := true
	f := false
	if i.i == nil {
		return nil
	}
	if *i.i == 1 {
		return &t
	}
	if *i.i == 0 {
		return &f
	}
	return nil
}

// Bool returns the bool value of Bool
func (b Bool) Bool() *bool {
	t := true
	f := false
	if b.b == nil {
		return nil
	}
	if *b.b {
		return &t
	}
	if !*b.b {
		return &f
	}
	return nil
}

// Constructors
// ============
// Strings is a constructor for a String series
// TODO: Change the name of the constructor from Strings() to S()?
func Strings(args ...interface{}) Series {
	elements := make(StringElements, 0, len(args))
	for _, v := range args {
		// TODO: case map[string]string{}: for named series?
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
			switch s.t {
			case "string":
				elems := s.Elements.(StringElements)
				elements = append(elements, elems...)
			case "int", "float", "bool":
				elems := s.Elements
				strElems := Strings(elems).Elements.(StringElements)
				elements = append(elements, strElems...)
			}
		default:
			// This should only happen if v (or its elements in case of a slice)
			// implements Stringer.
			stringer := reflect.TypeOf((*fmt.Stringer)(nil)).Elem()
			s := reflect.ValueOf(v)
			switch reflect.TypeOf(v).Kind() {
			case reflect.Slice:
				if s.Len() > 0 {
					for i := 0; i < s.Len(); i++ {
						if s.Index(i).Type().Implements(stringer) {
							s := fmt.Sprint(s.Index(i).Interface())
							elements = append(elements, String{&s})
						} else {
							s := "NA"
							elements = append(elements, String{&s})
						}
					}
				}
			default:
				if s.Type().Implements(stringer) {
					s := fmt.Sprint(v)
					elements = append(elements, String{&s})
				} else {
					s := "NA"
					elements = append(elements, String{&s})
				}
			}
		}
	}

	ret := Series{
		Name:     "",
		Elements: elements,
		names:    []string{},
		t:        "string",
	}
	return ret
}

// Ints is a constructor for an Int series
func Ints(args ...interface{}) Series {
	elements := make(IntElements, 0, len(args))
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
			switch s.t {
			case "string", "float", "bool":
				elems := s.Elements
				intElems := Ints(elems).Elements.(IntElements)
				elements = append(elements, intElems...)
			case "int":
				elems := s.Elements.(IntElements)
				elements = append(elements, elems...)
			}
		default:
			s := reflect.ValueOf(v)
			tointer := reflect.TypeOf((*tointeger)(nil)).Elem()
			switch reflect.TypeOf(v).Kind() {
			case reflect.Slice:
				if s.Len() > 0 {
					for i := 0; i < s.Len(); i++ {
						if s.Index(i).Type().Implements(tointer) {
							m := s.Index(i).MethodByName("Int")
							resolvedMethod := m.Call([]reflect.Value{})
							j := resolvedMethod[0].Interface().(*int)
							if j == nil {
								elements = append(elements, Int{nil})
							} else {
								elements = append(elements, Int{j})
							}
						} else {
							elements = append(elements, Int{nil})
						}
					}
				}
			default:
				elements = append(elements, Int{nil})
			}
		}
	}

	ret := Series{
		Name:     "",
		Elements: elements,
		names:    []string{},
		t:        "int",
	}
	return ret
}

// Floats is a constructor for a Float series
func Floats(args ...interface{}) Series {
	elements := make(FloatElements, 0, len(args))
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
			switch s.t {
			case "string", "int", "bool":
				elems := s.Elements
				floatElems := Floats(elems).Elements.(FloatElements)
				elements = append(elements, floatElems...)
			case "float":
				elems := s.Elements.(FloatElements)
				elements = append(elements, elems...)
			}
		default:
			s := reflect.ValueOf(v)
			tofloat := reflect.TypeOf((*tofloat)(nil)).Elem()
			switch reflect.TypeOf(v).Kind() {
			case reflect.Slice:
				if s.Len() > 0 {
					for i := 0; i < s.Len(); i++ {
						if s.Index(i).Type().Implements(tofloat) {
							m := s.Index(i).MethodByName("Float")
							resolvedMethod := m.Call([]reflect.Value{})
							j := resolvedMethod[0].Interface().(*float64)
							if j == nil {
								elements = append(elements, Float{nil})
							} else {
								elements = append(elements, Float{j})
							}
						} else {
							elements = append(elements, Float{nil})
						}
					}
				}
			default:
				elements = append(elements, Float{nil})
			}
		}
	}

	ret := Series{
		Name:     "",
		Elements: elements,
		names:    []string{},
		t:        "float",
	}
	return ret
}

// Bools is a constructor for a bools series
func Bools(args ...interface{}) Series {
	elements := make(BoolElements, 0, len(args))
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
			switch s.t {
			case "string", "int", "float":
				elems := s.Elements
				strElems := Bools(elems).Elements.(BoolElements)
				elements = append(elements, strElems...)
			case "bool":
				elems := s.Elements.(BoolElements)
				elements = append(elements, elems...)
			}
		default:
			s := reflect.ValueOf(v)
			tobool := reflect.TypeOf((*tobool)(nil)).Elem()
			switch reflect.TypeOf(v).Kind() {
			case reflect.Slice:
				if s.Len() > 0 {
					for i := 0; i < s.Len(); i++ {
						if s.Index(i).Type().Implements(tobool) {
							m := s.Index(i).MethodByName("Bool")
							resolvedMethod := m.Call([]reflect.Value{})
							j := resolvedMethod[0].Interface().(*bool)
							if j == nil {
								elements = append(elements, Bool{nil})
							} else {
								elements = append(elements, Bool{j})
							}
						} else {
							elements = append(elements, Bool{nil})
						}
					}
				}
			default:
				elements = append(elements, Bool{nil})
			}
		}
	}

	ret := Series{
		Name:     "",
		Elements: elements,
		names:    []string{},
		t:        "bool",
	}
	return ret
}

// Extra Series functions
func Str(s Series) string {
	// TODO: If name print name
	// TODO: Print summary of the elements. i.e. string[1:20] "a", "b", ...
	return fmt.Sprint(s)
}

func Len(s Series) int {
	switch s.t {
	case "string":
		elems := s.Elements.(StringElements)
		return (len(elems))
	case "int":
		elems := s.Elements.(IntElements)
		return (len(elems))
	case "float":
		elems := s.Elements.(FloatElements)
		return (len(elems))
	case "bool":
		elems := s.Elements.(BoolElements)
		return (len(elems))
	}
	return -1
}

//// Copy returns a copy of a given Cell
//func (s String) Copy() Cell {
//if s.s == nil {
//return String{nil}
//}
//j := *s.s
//return String{&j}
//}

//// NA returns the empty element for this type
//func (s String) NA() Cell {
//return String{nil}
//}

//// IsNA returns true if the element is empty and viceversa
//func (s String) IsNA() bool {
//if s.s == nil {
//return true
//}
//return false
//}

//// Copy returns a copy of a given Cell
//func (i Int) Copy() Cell {
//if i.i == nil {
//return Int{nil}
//}
//j := *i.i
//return Int{&j}
//}

//// Compare tries to compare a Cell with the current element
//func (i Int) Compare(cell Cell, op comparator) (*bool, error) {
//switch op {
//case eq:
//a, err := i.Int()
//if err != nil {
//return nil, err
//}
//b, err := cell.Int()
//if err != nil {
//return nil, err
//}
//comp := *a == *b
//return &comp, nil
//case neq:
//a, err := i.Int()
//if err != nil {
//return nil, err
//}
//b, err := cell.Int()
//if err != nil {
//return nil, err
//}
//comp := *a != *b
//return &comp, nil
//case gt:
//if !i.IsNA() && !cell.IsNA() {
//a, err := i.Int()
//if err != nil {
//return nil, err
//}
//b, err := cell.Int()
//if err != nil {
//return nil, err
//}
//comp := *a > *b
//return &comp, nil
//}
//case lt:
//if !i.IsNA() && !cell.IsNA() {
//a, err := i.Int()
//if err != nil {
//return nil, err
//}
//b, err := cell.Int()
//if err != nil {
//return nil, err
//}
//comp := *a < *b
//return &comp, nil
//}
//case get:
//if !i.IsNA() && !cell.IsNA() {
//a, err := i.Int()
//if err != nil {
//return nil, err
//}
//b, err := cell.Int()
//if err != nil {
//return nil, err
//}
//comp := *a >= *b
//return &comp, nil
//}
//case let:
//if !i.IsNA() && !cell.IsNA() {
//a, err := i.Int()
//if err != nil {
//return nil, err
//}
//b, err := cell.Int()
//if err != nil {
//return nil, err
//}
//comp := *a <= *b
//return &comp, nil
//}
//}

//return nil, errors.New("Invalid comparator operation")
//}

//// Checksum generates a pseudo-unique 16 byte array
//func (i Int) Checksum() [16]byte {
//s := i.String()
//b := []byte(s + "Int")
//return md5.Sum(b)
//}

//// NA returns the empty element for this type
//func (i Int) NA() Cell {
//return Int{nil}
//}

//// IsNA returns true if the element is empty and viceversa
//func (i Int) IsNA() bool {
//if i.i == nil {
//return true
//}
//return false
//}

//// Copy returns a copy of a given Cell
//func (f Float) Copy() Cell {
//if f.f == nil {
//return Float{nil}
//}
//j := *f.f
//return Float{&j}
//}

//// Compare tries to compare a Cell with the current element
//func (f Float) Compare(cell Cell, op comparator) (*bool, error) {
//switch op {
//case eq:
//a, err := f.Float()
//if err != nil {
//return nil, err
//}
//b, err := cell.Float()
//if err != nil {
//return nil, err
//}
//comp := *a == *b
//return &comp, nil
//case neq:
//a, err := f.Float()
//if err != nil {
//return nil, err
//}
//b, err := cell.Float()
//if err != nil {
//return nil, err
//}
//comp := *a != *b
//return &comp, nil
//case gt:
//if !f.IsNA() && !cell.IsNA() {
//a, err := f.Float()
//if err != nil {
//return nil, err
//}
//b, err := cell.Float()
//if err != nil {
//return nil, err
//}
//comp := *a > *b
//return &comp, nil
//}
//case lt:
//if !f.IsNA() && !cell.IsNA() {
//a, err := f.Float()
//if err != nil {
//return nil, err
//}
//b, err := cell.Float()
//if err != nil {
//return nil, err
//}
//comp := *a < *b
//return &comp, nil
//}
//case get:
//if !f.IsNA() && !cell.IsNA() {
//a, err := f.Float()
//if err != nil {
//return nil, err
//}
//b, err := cell.Float()
//if err != nil {
//return nil, err
//}
//comp := *a >= *b
//return &comp, nil
//}
//case let:
//if !f.IsNA() && !cell.IsNA() {
//a, err := f.Float()
//if err != nil {
//return nil, err
//}
//b, err := cell.Float()
//if err != nil {
//return nil, err
//}
//comp := *a <= *b
//return &comp, nil
//}
//}

//return nil, errors.New("Invalid comparator operation")
//}

//func (f Float) String() string {
//return formatCell(f.f)
//}

//// Bool returns the bool value of Float
//func (f Float) Bool() (*bool, error) {
//t := true
//fa := false
//if f.f == nil {
//return nil, errors.New("Can't convert to Bool")
//}
//if *f.f == 1.0 {
//return &t, nil
//}
//if *f.f == 0.0 {
//return &fa, nil
//}
//return nil, errors.New("Can't convert to Bool")
//}

//// Checksum generates a pseudo-unique 16 byte array
//func (f Float) Checksum() [16]byte {
//s := f.String()
//b := []byte(s + "Float")
//return md5.Sum(b)
//}

//// NA returns the empty element for this type
//func (f Float) NA() Cell {
//return Float{nil}
//}

//// IsNA returns true if the element is empty and viceversa
//func (f Float) IsNA() bool {
//if f.f == nil {
//return true
//}
//return false
//}

//// Copy returns a copy of a given Cell
//func (b Bool) Copy() Cell {
//if b.b == nil {
//return Bool{nil}
//}
//j := *b.b
//return Bool{&j}
//}

//// Compare tries to compare a Cell with the current element
//func (b Bool) Compare(cell Cell, op comparator) (*bool, error) {
//switch op {
//case eq:
//if !b.IsNA() && !cell.IsNA() {
//a, err := b.Bool()
//if err != nil {
//return nil, err
//}
//b, err := cell.Bool()
//if err != nil {
//return nil, err
//}
//comp := *a == *b
//return &comp, nil
//}
//case neq:
//if !b.IsNA() && !cell.IsNA() {
//a, err := b.Bool()
//if err != nil {
//return nil, err
//}
//b, err := cell.Bool()
//if err != nil {
//return nil, err
//}
//comp := *a != *b
//return &comp, nil
//}
//case gt:
//case lt:
//case get:
//case let:
//}

//return nil, errors.New("Invalid comparator operation")
//}

//// Checksum generates a pseudo-unique 16 byte array
//func (b Bool) Checksum() [16]byte {
//bs := []byte(b.String() + "Bool")
//return md5.Sum(bs)
//}

//// NA returns the empty element for this type
//func (b Bool) NA() Cell {
//return Bool{nil}
//}

//// IsNA returns true if the element is empty and viceversa
//func (b Bool) IsNA() bool {
//if b.b == nil {
//return true
//}
//return false
//}

// Helper interfaces
// =================
type tointeger interface {
	Int() *int
}
type tofloat interface {
	Float() *float64
}
type tobool interface {
	Bool() *bool
}
