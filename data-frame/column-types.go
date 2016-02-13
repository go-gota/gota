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

// ToFloat returns the float value of String
func (s String) ToFloat() (float64, error) {
	f, err := strconv.ParseFloat(s.s, 64)
	if err != nil {
		return 0, errors.New("Could't convert to float64")
	}
	return f, nil
}

func (s String) String() string {
	return s.s
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

// Int is an alias for int to be able to implement custom methods
type Int struct {
	i *int
}

// ToInteger returns the integer value of Int
func (i Int) ToInteger() (int, error) {
	if i.i != nil {
		return *i.i, nil
	}
	return 0, errors.New("Could't convert to int")
}

// ToFloat returns the float value of Int
func (i Int) ToFloat() (float64, error) {
	if i.i != nil {
		f := float64(*i.i)
		return f, nil
	}
	return 0, errors.New("Could't convert to float")
}

func (i Int) String() string {
	return formatCell(i.i)
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

// Float is an alias for float64 to be able to implement custom methods
type Float struct {
	f *float64
}

func (f Float) String() string {
	return formatCell(f.f)
}

// ToInteger returns the integer value of Float
func (f Float) ToInteger() (int, error) {
	if f.f != nil {
		return int(*f.f), nil
	}
	return 0, errors.New("Could't convert to int")
}

// ToFloat returns the float value of Float
func (f Float) ToFloat() (float64, error) {
	if f.f != nil {
		return *f.f, nil
	}
	return 0, errors.New("Could't convert to float64")
}

// Floats is a constructor for a Float array
func Floats(args ...interface{}) cells {
	ret := make(cells, 0, len(args))
	for _, v := range args {
		switch v.(type) {
		case []int:
			varr := v.([]int)
			for k := range varr {
				i := varr[k]
				f := float64(i)
				ret = append(ret, Float{&f})
			}
		case int:
			i := v.(int)
			f := float64(i)
			ret = append(ret, Float{&f})
		case []float64:
			varr := v.([]float64)
			for k := range varr {
				f := varr[k]
				ret = append(ret, Float{&f})
			}
		case float64:
			f := v.(float64)
			ret = append(ret, Float{&f})
		case []string:
			varr := v.([]string)
			for k := range varr {
				s := varr[k]
				f, err := strconv.ParseFloat(s, 64)
				if err != nil {
					ret = append(ret, Float{nil})
				} else {
					ret = append(ret, Float{&f})
				}
			}
		case string:
			f, err := strconv.ParseFloat(v.(string), 64)
			if err != nil {
				ret = append(ret, Float{nil})
			} else {
				ret = append(ret, Float{&f})
			}
		case nil:
			ret = append(ret, Float{nil})
		default:
			s := reflect.ValueOf(v)
			tofloat := reflect.TypeOf((*tofloat)(nil)).Elem()
			switch reflect.TypeOf(v).Kind() {
			case reflect.Slice:
				if s.Len() > 0 {
					for i := 0; i < s.Len(); i++ {
						if s.Index(i).Type().Implements(tofloat) {
							m := s.Index(i).MethodByName("ToFloat")
							resolvedMethod := m.Call([]reflect.Value{})
							j := resolvedMethod[0].Interface().(float64)
							err := resolvedMethod[1].Interface()
							if err != nil {
								ret = append(ret, Float{nil})
							} else {
								ret = append(ret, Float{&j})
							}
						} else {
							ret = append(ret, Float{nil})
						}
					}
				}
			default:
				ret = append(ret, Float{nil})
			}
		}
	}

	return ret
}
