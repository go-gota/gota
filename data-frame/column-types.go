package df

import (
	"crypto/md5"
	"errors"
	"fmt"
	"reflect"
	"strconv"
)

// String is an alias for string to be able to implement custom methods
type String struct {
	s *string
}

// ToInteger returns the integer value of String
func (s String) ToInteger() (*int, error) {
	if s.s == nil {
		return nil, errors.New("Could't convert to int")
	}
	str, err := strconv.Atoi(*s.s)
	if err != nil {
		return nil, errors.New("Could't convert to int")
	}
	return &str, nil
}

// ToFloat returns the float value of String
func (s String) ToFloat() (*float64, error) {
	if s.s == nil {
		return nil, errors.New("Could't convert to float64")
	}
	f, err := strconv.ParseFloat(*s.s, 64)
	if err != nil {
		return nil, errors.New("Could't convert to float64")
	}
	return &f, nil
}

// ToBool returns the bool value of String
func (s String) ToBool() (*bool, error) {
	if s.s == nil {
		return nil, errors.New("Could't convert to bool")
	}
	t := true
	f := false
	if *s.s == "false" {
		return &f, nil
	}
	if *s.s == "true" {
		return &t, nil
	}
	return nil, errors.New("Can't convert to Bool")
}

func (s String) String() string {
	if s.s == nil {
		return "NA"
	}
	return *s.s
}

// Checksum generates a pseudo-unique 16 byte array
func (s String) Checksum() [16]byte {
	b := []byte(s.String() + "String")
	return md5.Sum(b)
}

// Strings is a constructor for a String array
func Strings(args ...interface{}) cells {
	ret := make([]cell, 0, len(args))
	for _, v := range args {
		switch v.(type) {
		case int:
			s := strconv.Itoa(v.(int))
			ret = append(ret, String{&s})
		case float64:
			s := strconv.FormatFloat(v.(float64), 'f', 6, 64)
			ret = append(ret, String{&s})
		case []int:
			varr := v.([]int)
			for k := range varr {
				s := strconv.Itoa(varr[k])
				ret = append(ret, String{&s})
			}
		case []float64:
			varr := v.([]float64)
			for k := range varr {
				s := strconv.FormatFloat(varr[k], 'f', 6, 64)
				ret = append(ret, String{&s})
			}
		case string:
			s := v.(string)
			ret = append(ret, String{&s})
		case []string:
			varr := v.([]string)
			for k := range varr {
				s := varr[k]
				ret = append(ret, String{&s})
			}
		case nil:
			ret = append(ret, String{nil})
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
							ret = append(ret, String{&s})
						} else {
							s := "NA"
							ret = append(ret, String{&s})
						}
					}
				}
			default:
				if s.Type().Implements(stringer) {
					s := fmt.Sprint(v)
					ret = append(ret, String{&s})
				} else {
					s := "NA"
					ret = append(ret, String{&s})
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
func (i Int) ToInteger() (*int, error) {
	if i.i != nil {
		return i.i, nil
	}
	return nil, errors.New("Could't convert to int")
}

// ToFloat returns the float value of Int
func (i Int) ToFloat() (*float64, error) {
	if i.i != nil {
		f := float64(*i.i)
		return &f, nil
	}
	return nil, errors.New("Could't convert to float")
}

func (i Int) String() string {
	return formatCell(i.i)
}

// ToBool returns the bool value of Int
func (i Int) ToBool() (*bool, error) {
	t := true
	f := false
	if i.i == nil {
		return nil, errors.New("Can't convert to Bool")
	}
	if *i.i == 1 {
		return &t, nil
	}
	if *i.i == 0 {
		return &f, nil
	}
	return nil, errors.New("Can't convert to Bool")
}

// Checksum generates a pseudo-unique 16 byte array
func (i Int) Checksum() [16]byte {
	s := i.String()
	b := []byte(s + "Int")
	return md5.Sum(b)
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
							j := resolvedMethod[0].Interface().(*int)
							err := resolvedMethod[1].Interface()
							if err != nil {
								ret = append(ret, Int{nil})
							} else {
								ret = append(ret, Int{j})
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
func (f Float) ToInteger() (*int, error) {
	if f.f != nil {
		i := int(*f.f)
		return &i, nil
	}
	return nil, errors.New("Could't convert to int")
}

// ToFloat returns the float value of Float
func (f Float) ToFloat() (*float64, error) {
	if f.f != nil {
		return f.f, nil
	}
	return nil, errors.New("Could't convert to float64")
}

// ToBool returns the bool value of Float
func (f Float) ToBool() (*bool, error) {
	t := true
	fa := false
	if f.f == nil {
		return nil, errors.New("Can't convert to Bool")
	}
	if *f.f == 1.0 {
		return &t, nil
	}
	if *f.f == 0.0 {
		return &fa, nil
	}
	return nil, errors.New("Can't convert to Bool")
}

// Checksum generates a pseudo-unique 16 byte array
func (f Float) Checksum() [16]byte {
	s := f.String()
	b := []byte(s + "Float")
	return md5.Sum(b)
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
							j := resolvedMethod[0].Interface().(*float64)
							err := resolvedMethod[1].Interface()
							if err != nil {
								ret = append(ret, Float{nil})
							} else {
								ret = append(ret, Float{j})
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

// Bool is an alias for string to be able to implement custom methods
type Bool struct {
	b *bool
}

// ToInteger returns the integer value of Bool
func (b Bool) ToInteger() (*int, error) {
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

// ToFloat returns the float value of Bool
func (b Bool) ToFloat() (*float64, error) {
	if b.b == nil {
		return nil, errors.New("Empty value")
	}
	if *b.b {
		one := 1.0
		return &one, nil
	}
	zero := 0.0
	return &zero, nil
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

// ToBool returns the bool value of Bool
func (b Bool) ToBool() (*bool, error) {
	t := true
	f := false
	if b.b == nil {
		return nil, errors.New("Can't convert to Bool")
	}
	if *b.b {
		return &t, nil
	}
	if *b.b {
		return &f, nil
	}
	return nil, errors.New("Can't convert to Bool")
}

// Checksum generates a pseudo-unique 16 byte array
func (b Bool) Checksum() [16]byte {
	bs := []byte(b.String() + "Bool")
	return md5.Sum(bs)
}

// Bools is a constructor for a bools array
func Bools(args ...interface{}) cells {
	ret := make(cells, 0, len(args))
	for _, v := range args {
		switch v.(type) {
		case []int:
			varr := v.([]int)
			for k := range varr {
				i := varr[k]
				t := true
				f := false
				if i == 1 {
					ret = append(ret, Bool{&t})
				} else if i == 0 {
					ret = append(ret, Bool{&f})
				} else {
					ret = append(ret, Bool{nil})
				}
			}
		case int:
			i := v.(int)
			t := true
			f := false
			if i == 1 {
				ret = append(ret, Bool{&t})
			} else if i == 0 {
				ret = append(ret, Bool{&f})
			} else {
				ret = append(ret, Bool{nil})
			}
		case []float64:
			varr := v.([]float64)
			for k := range varr {
				i := varr[k]
				t := true
				f := false
				if i == 1 {
					ret = append(ret, Bool{&t})
				} else if i == 0 {
					ret = append(ret, Bool{&f})
				} else {
					ret = append(ret, Bool{nil})
				}
			}
		case float64:
			i := v.(float64)
			t := true
			f := false
			if i == 1 {
				ret = append(ret, Bool{&t})
			} else if i == 0 {
				ret = append(ret, Bool{&f})
			} else {
				ret = append(ret, Bool{nil})
			}
		case []string:
			varr := v.([]string)
			for k := range varr {
				i := varr[k]
				t := true
				f := false
				if i == "true" {
					ret = append(ret, Bool{&t})
				} else if i == "false" {
					ret = append(ret, Bool{&f})
				} else {
					ret = append(ret, Bool{nil})
				}
			}
		case string:
			i := v.(string)
			t := true
			f := false
			if i == "true" {
				ret = append(ret, Bool{&t})
			} else if i == "false" {
				ret = append(ret, Bool{&f})
			} else {
				ret = append(ret, Bool{nil})
			}
		case nil:
			ret = append(ret, Bool{nil})
		default:
			s := reflect.ValueOf(v)
			tobool := reflect.TypeOf((*tobool)(nil)).Elem()
			switch reflect.TypeOf(v).Kind() {
			case reflect.Slice:
				if s.Len() > 0 {
					for i := 0; i < s.Len(); i++ {
						if s.Index(i).Type().Implements(tobool) {
							m := s.Index(i).MethodByName("ToBool")
							resolvedMethod := m.Call([]reflect.Value{})
							j := resolvedMethod[0].Interface().(*bool)
							err := resolvedMethod[1].Interface()
							if err != nil {
								ret = append(ret, Bool{nil})
							} else {
								ret = append(ret, Bool{j})
							}
						} else {
							ret = append(ret, Bool{nil})
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
