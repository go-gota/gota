package df

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
)

// String is an alias for string to be able to implement custom methods
type String struct {
	s string
}

// ToInteger returns the integer value of String
func (s String) ToInteger() (int, error) {
	str, err := strconv.Atoi(s.s)
	if err != nil {
		return 0, errors.New("Could't convert to int")
	}
	return str, nil
}

func (s String) String() string {
	return s.s
}

// Int is an alias for string to be able to implement custom methods
type Int struct {
	i *int
}

// ToInteger returns the integer value of Int
func (s Int) ToInteger() (int, error) {
	if s.i != nil {
		return *s.i, nil
	}
	return 0, errors.New("Could't convert to int")
}

func (s Int) String() string {
	return formatCell(s.i)
}

// Strings is a constructor for a String array
func Strings(args ...interface{}) cells {
	ret := make([]cell, 0, len(args))
	for _, v := range args {
		switch v.(type) {
		case int:
			s := strconv.Itoa(v.(int))
			ret = append(ret, String{s})
		case float64:
			s := strconv.FormatFloat(v.(float64), 'f', 6, 64)
			ret = append(ret, String{s})
		case []int:
			varr := v.([]int)
			for k := range varr {
				s := strconv.Itoa(varr[k])
				ret = append(ret, String{s})
			}
		case []float64:
			varr := v.([]float64)
			for k := range varr {
				s := strconv.FormatFloat(varr[k], 'f', 6, 64)
				ret = append(ret, String{s})
			}
		case string:
			ret = append(ret, String{v.(string)})
		case []string:
			varr := v.([]string)
			for k := range varr {
				ret = append(ret, String{varr[k]})
			}
		case nil:
			ret = append(ret, String{""})
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
							ret = append(ret, String{fmt.Sprint(s.Index(i).Interface())})
						} else {
							ret = append(ret, String{"NA"})
						}
					}
				}
			default:
				if s.Type().Implements(stringer) {
					ret = append(ret, String{fmt.Sprint(v)})
				} else {
					ret = append(ret, String{"NA"})
				}
			}
		}
	}

	return ret
}

// Ints is a constructor for an Int array
func Ints(args ...interface{}) cells {
	ret := make(cells, 0, len(args))
	for _, v := range args {
		switch v.(type) {
		case int:
			i := v.(int)
			ret = append(ret, Int{&i})
		case float64:
			f := v.(float64)
			i := int(f)
			ret = append(ret, Int{&i})
		case []int:
			varr := v.([]int)
			for k := range varr {
				ret = append(ret, Int{&varr[k]})
			}
		case []float64:
			varr := v.([]float64)
			for k := range varr {
				f := varr[k]
				i := int(f)
				ret = append(ret, Int{&i})
			}
		case []string:
			varr := v.([]string)
			for k := range varr {
				s := varr[k]
				i, err := strconv.Atoi(s)
				if err != nil {
					ret = append(ret, Int{nil})
				} else {
					ret = append(ret, Int{&i})
				}
			}
		case string:
			i, err := strconv.Atoi(v.(string))
			if err != nil {
				ret = append(ret, Int{nil})
			} else {
				ret = append(ret, Int{&i})
			}
		case nil:
			ret = append(ret, Int{nil})
		default:
			s := reflect.ValueOf(v)
			tointer := reflect.TypeOf((*tointeger)(nil)).Elem()
			switch reflect.TypeOf(v).Kind() {
			case reflect.Slice:
				if s.Len() > 0 {
					for i := 0; i < s.Len(); i++ {
						if s.Index(i).Type().Implements(tointer) {
							m := s.Index(i).MethodByName("ToInteger")
							resolvedMethod := m.Call([]reflect.Value{})
							j := resolvedMethod[0].Interface().(int)
							err := resolvedMethod[1].Interface()
							if err != nil {
								ret = append(ret, Int{nil})
							} else {
								ret = append(ret, Int{&j})
							}
						} else {
							ret = append(ret, Int{nil})
						}
					}
				}
			default:
				ret = append(ret, Int{nil})
			}
		}
	}

	return ret
}
