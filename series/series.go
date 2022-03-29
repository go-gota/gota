package series

import (
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"

	"github.com/cespare/xxhash/v2"
	"gonum.org/v1/gonum/stat"
)

// Series is a data structure designed for operating on arrays of elements that
// should comply with a certain type structure. They are flexible enough that can
// be transformed to other Series types and account for missing or non valid
// elements. Most of the power of Series resides on the ability to compare and
// subset Series of different types.
type Series struct {
	Name     string   // The name of the series
	elements Elements // The values of the elements
	t        Type     // The type of the series

	// deprecated: use Error() instead
	Err error
}

// Elements is the interface that represents the array of elements contained on
// a Series.
type Elements interface {
	Elem(int) Element
	Len() int
}

// Element is the interface that defines the types of methods to be present for
// elements of a Series
type Element interface {
	// Setter method
	Set(interface{})

	// Comparation methods
	Eq(Element) bool
	Neq(Element) bool
	Less(Element) bool
	LessEq(Element) bool
	Greater(Element) bool
	GreaterEq(Element) bool

	// Accessor/conversion methods
	Copy() Element     // FIXME: Returning interface is a recipe for pain
	Val() ElementValue // FIXME: Returning interface is a recipe for pain
	String() string
	Int() (int, error)
	Float() float64
	Bool() (bool, error)
	StringList() []string
	IntList() ([]int, error)
	FloatList() []float64
	BoolList() ([]bool, error)

	// Information methods
	IsNA() bool
	Type() Type
}

// intElements is the concrete implementation of Elements for Int elements.
type intElements []intElement

func (e intElements) Len() int           { return len(e) }
func (e intElements) Elem(i int) Element { return &e[i] }

// intListElements is the concrete implementation of Elements for IntList elements.
type intListElements []intListElement

func (e intListElements) Len() int           { return len(e) }
func (e intListElements) Elem(i int) Element { return &e[i] }

// stringElements is the concrete implementation of Elements for String elements.
type stringElements []stringElement

func (e stringElements) Len() int           { return len(e) }
func (e stringElements) Elem(i int) Element { return &e[i] }

// stringListElements is the concrete implementation of Elements for IntList elements.
type stringListElements []stringListElement

func (e stringListElements) Len() int           { return len(e) }
func (e stringListElements) Elem(i int) Element { return &e[i] }

// floatElements is the concrete implementation of Elements for Float elements.
type floatElements []floatElement

func (e floatElements) Len() int           { return len(e) }
func (e floatElements) Elem(i int) Element { return &e[i] }

// floatListElements is the concrete implementation of Elements for IntList elements.
type floatListElements []floatListElement

func (e floatListElements) Len() int           { return len(e) }
func (e floatListElements) Elem(i int) Element { return &e[i] }

// boolElements is the concrete implementation of Elements for Bool elements.
type boolElements []boolElement

func (e boolElements) Len() int           { return len(e) }
func (e boolElements) Elem(i int) Element { return &e[i] }

// boolListElements is the concrete implementation of Elements for IntList elements.
type boolListElements []boolListElement

func (e boolListElements) Len() int           { return len(e) }
func (e boolListElements) Elem(i int) Element { return &e[i] }

// ElementValue represents the value that can be used for marshaling or
// unmarshaling Elements.
type ElementValue interface{}

type MapFunction func(Element) Element

// Comparator is a convenience alias that can be used for a more type safe way of
// reason and use comparators.
type Comparator string

// Supported Comparators
const (
	Eq        Comparator = "=="   // Equal
	Neq       Comparator = "!="   // Non equal
	Greater   Comparator = ">"    // Greater than
	GreaterEq Comparator = ">="   // Greater or equal than
	Less      Comparator = "<"    // Lesser than
	LessEq    Comparator = "<="   // Lesser or equal than
	In        Comparator = "in"   // Inside
	CompFunc  Comparator = "func" // user-defined comparison function
)

// compFunc defines a user-defined comparator function. Used internally for type assertions
type compFunc = func(el Element) bool

// Type is a convenience alias that can be used for a more type safe way of
// reason and use Series types.
type Type string

// Supported Series Types
const (
	String     Type = "string"
	Int        Type = "int"
	Float      Type = "float"
	Bool       Type = "bool"
	StringList Type = "string_list"
	IntList    Type = "int_list"
	FloatList  Type = "float_list"
	BoolList   Type = "bool_list"
)

// Indexes represent the elements that can be used for selecting a subset of
// elements within a Series. Currently supported are:
//
//     int            // Matches the given index number
//     []int          // Matches all given index numbers
//     []bool         // Matches all elements in a Series marked as true
//     Series [Int]   // Same as []int
//     Series [Bool]  // Same as []bool
type Indexes interface{}

type LogicalOperator int

const (
	And LogicalOperator = iota
	Or
	XOr
)

func NewEmpty(t Type, name string, length int) Series {
	values := make([]interface{}, length)
	return New(values, t, name)
}

// New is the generic Series constructor
func New(values interface{}, t Type, name string) Series {
	ret := Series{
		Name: name,
		t:    t,
	}

	// Pre-allocate elements
	preAlloc := func(n int) {
		switch t {
		case String:
			ret.elements = make(stringElements, n)
		case Int:
			ret.elements = make(intElements, n)
		case Float:
			ret.elements = make(floatElements, n)
		case Bool:
			ret.elements = make(boolElements, n)
		case StringList:
			ret.elements = make(stringListElements, n)
		case IntList:
			ret.elements = make(intListElements, n)
		case FloatList:
			ret.elements = make(floatListElements, n)
		case BoolList:
			ret.elements = make(boolListElements, n)
		default:
			panic(fmt.Sprintf("unknown type %v", t))
		}
	}

	if values == nil {
		preAlloc(1)
		ret.elements.Elem(0).Set(nil)
		return ret
	}

	switch v := values.(type) {
	case []string:
		if strings.HasSuffix(string(t), "_list") {
			if len(v) > 0 {
				preAlloc(1)
				ret.elements.Elem(0).Set(v)
			} else {
				preAlloc(0)
			}
		} else {
			l := len(v)
			preAlloc(l)
			for i := 0; i < l; i++ {
				ret.elements.Elem(i).Set(v[i])
			}
		}
	case [][]string:
		l := len(v)
		preAlloc(l)
		for i := 0; i < l; i++ {
			ret.elements.Elem(i).Set(v[i])
		}
	case []float64:
		if strings.HasSuffix(string(t), "_list") {
			if len(v) > 0 {
				preAlloc(1)
				ret.elements.Elem(0).Set(v)
			} else {
				preAlloc(0)
			}
		} else {
			l := len(v)
			preAlloc(l)
			for i := 0; i < l; i++ {
				ret.elements.Elem(i).Set(v[i])
			}
		}
	case [][]float64:
		l := len(v)
		preAlloc(l)
		for i := 0; i < l; i++ {
			ret.elements.Elem(i).Set(v[i])
		}
	case []int:
		if strings.HasSuffix(string(t), "_list") {
			if len(v) > 0 {
				preAlloc(1)
				ret.elements.Elem(0).Set(v)
			} else {
				preAlloc(0)
			}
		} else {
			l := len(v)
			preAlloc(l)
			for i := 0; i < l; i++ {
				ret.elements.Elem(i).Set(v[i])
			}
		}
	case [][]int:
		l := len(v)
		preAlloc(l)
		for i := 0; i < l; i++ {
			ret.elements.Elem(i).Set(v[i])
		}
	case []bool:
		if strings.HasSuffix(string(t), "_list") {
			if len(v) > 0 {
				preAlloc(1)
				ret.elements.Elem(0).Set(v)
			} else {
				preAlloc(0)
			}
		} else {
			l := len(v)
			preAlloc(l)
			for i := 0; i < l; i++ {
				ret.elements.Elem(i).Set(v[i])
			}
		}
	case [][]bool:
		l := len(v)
		preAlloc(l)
		for i := 0; i < l; i++ {
			ret.elements.Elem(i).Set(v[i])
		}
	case []interface{}:
		if strings.HasSuffix(string(t), "_list") {
			l := len(v)
			preAlloc(l)
			for i := 0; i < l; i++ {
				if v[i] == nil {
					ret.elements.Elem(i).Set(nil)
					continue
				}
				switch reflect.TypeOf(v[i]).Kind() {
				case reflect.Slice:
					ret.elements.Elem(i).Set(v[i])
				default:
					ret.elements.Elem(i).Set(fmt.Sprint(v[i]))
				}
			}
		} else {
			l := len(v)
			preAlloc(l)
			for i := 0; i < l; i++ {
				if v[i] == nil {
					ret.elements.Elem(i).Set(nil)
					continue
				}
				switch reflect.TypeOf(v[i]).Kind() {
				case reflect.Slice:
					ret.elements.Elem(i).Set(v[i])
				default:
					ret.elements.Elem(i).Set(v[i])
				}
			}
		}
	case [][]interface{}:
		l := len(v)
		preAlloc(l)
		for i := 0; i < l; i++ {
			if v[i] == nil {
				ret.elements.Elem(i).Set(nil)
				continue
			}
			s := toStringArray(v[i])
			ret.elements.Elem(i).Set(s)
		}
	case Series:
		l := v.Len()
		preAlloc(l)
		for i := 0; i < l; i++ {
			ret.elements.Elem(i).Set(v.elements.Elem(i))
		}
	default:
		switch reflect.TypeOf(values).Kind() {
		case reflect.Slice:
			v := reflect.ValueOf(values)
			l := v.Len()
			preAlloc(v.Len())
			for i := 0; i < l; i++ {
				val := v.Index(i).Interface()
				ret.elements.Elem(i).Set(val)
			}
		default:
			preAlloc(1)
			v := reflect.ValueOf(values)
			val := v.Interface()
			ret.elements.Elem(0).Set(val)
		}
	}
	return ret
}

func toStringArray(v []interface{}) []string {
	l := len(v)
	s := make([]string, l)
	for i := 0; i < l; i++ {
		if v[i] == nil {
			s[i] = "NaN"
			continue
		}

		s[i] = fmt.Sprint(v[i])
	}
	return s
}

// Strings is a constructor for a String Series
func Strings(values interface{}) Series {
	return New(values, String, "")
}

// Ints is a constructor for an Int Series
func Ints(values interface{}) Series {
	return New(values, Int, "")
}

// Floats is a constructor for a Float Series
func Floats(values interface{}) Series {
	return New(values, Float, "")
}

// Bools is a constructor for a Bool Series
func Bools(values interface{}) Series {
	return New(values, Bool, "")
}

// StringsList is a constructor for an StringList Series
func StringsList(values interface{}) Series {
	return New(values, StringList, "")
}

// IntsList is a constructor for an IntList Series
func IntsList(values interface{}) Series {
	return New(values, IntList, "")
}

// FloatsList is a constructor for an FloatList Series
func FloatsList(values interface{}) Series {
	return New(values, FloatList, "")
}

// BoolsList is a constructor for an BoolList Series
func BoolsList(values interface{}) Series {
	return New(values, BoolList, "")
}

// Empty returns an empty Series of the same type
func (s Series) Empty() Series {
	return New([]int{}, s.t, s.Name)
}

// Returns Error or nil if no error occured
func (s *Series) Error() error {
	return s.Err
}

// Append adds new elements to the end of the Series. When using Append, the
// Series is modified in place.
func (s *Series) Append(values interface{}) {
	if err := s.Err; err != nil {
		return
	}
	news := New(values, s.t, s.Name)
	switch s.t {
	case String:
		s.elements = append(s.elements.(stringElements), news.elements.(stringElements)...)
	case Int:
		s.elements = append(s.elements.(intElements), news.elements.(intElements)...)
	case Float:
		s.elements = append(s.elements.(floatElements), news.elements.(floatElements)...)
	case Bool:
		s.elements = append(s.elements.(boolElements), news.elements.(boolElements)...)
	case StringList:
		s.elements = append(s.elements.(stringListElements), news.elements.(stringListElements)...)
	case IntList:
		s.elements = append(s.elements.(intListElements), news.elements.(intListElements)...)
	case FloatList:
		s.elements = append(s.elements.(floatListElements), news.elements.(floatListElements)...)
	case BoolList:
		s.elements = append(s.elements.(boolListElements), news.elements.(boolListElements)...)
	}
}

// Concat concatenates two series together. It will return a new Series with the
// combined elements of both Series.
func (s Series) Concat(x Series) Series {
	if err := s.Err; err != nil {
		return s
	}
	if err := x.Err; err != nil {
		s.Err = fmt.Errorf("concat error: argument has errors: %v", err)
		return s
	}
	y := s.Copy()
	y.Append(x)
	return y
}

// Subset returns a subset of the series based on the given Indexes.
func (s Series) Subset(indexes Indexes) Series {
	if err := s.Err; err != nil {
		return s
	}
	idx, err := parseIndexes(s.Len(), indexes)
	if err != nil {
		s.Err = err
		return s
	}
	ret := Series{
		Name: s.Name,
		t:    s.t,
	}
	switch s.t {
	case String:
		elements := make(stringElements, len(idx))
		for k, i := range idx {
			elements[k] = s.elements.(stringElements)[i]
		}
		ret.elements = elements
	case Int:
		elements := make(intElements, len(idx))
		for k, i := range idx {
			elements[k] = s.elements.(intElements)[i]
		}
		ret.elements = elements
	case Float:
		elements := make(floatElements, len(idx))
		for k, i := range idx {
			elements[k] = s.elements.(floatElements)[i]
		}
		ret.elements = elements
	case Bool:
		elements := make(boolElements, len(idx))
		for k, i := range idx {
			elements[k] = s.elements.(boolElements)[i]
		}
		ret.elements = elements
	case StringList:
		elements := make(stringListElements, len(idx))
		for k, i := range idx {
			elements[k] = s.elements.(stringListElements)[i]
		}
		ret.elements = elements
	case IntList:
		elements := make(intListElements, len(idx))
		for k, i := range idx {
			elements[k] = s.elements.(intListElements)[i]
		}
		ret.elements = elements
	case FloatList:
		elements := make(floatListElements, len(idx))
		for k, i := range idx {
			elements[k] = s.elements.(floatListElements)[i]
		}
		ret.elements = elements
	case BoolList:
		elements := make(boolListElements, len(idx))
		for k, i := range idx {
			elements[k] = s.elements.(boolListElements)[i]
		}
		ret.elements = elements
	default:
		panic("unknown series type")
	}
	return ret
}

// Set sets the values on the indexes of a Series and returns the reference
// for itself. The original Series is modified.
func (s Series) Set(indexes Indexes, newvalues Series) Series {
	if err := s.Err; err != nil {
		return s
	}
	if err := newvalues.Err; err != nil {
		s.Err = fmt.Errorf("set error: argument has errors: %v", err)
		return s
	}
	idx, err := parseIndexes(s.Len(), indexes)
	if err != nil {
		s.Err = err
		return s
	}
	if len(idx) != newvalues.Len() {
		s.Err = fmt.Errorf("set error: dimensions mismatch")
		return s
	}
	for k, i := range idx {
		if i < 0 || i >= s.Len() {
			s.Err = fmt.Errorf("set error: index out of range")
			return s
		}
		s.elements.Elem(i).Set(newvalues.elements.Elem(k))
	}
	return s
}

// SetMutualExclusiveValue sets the values on the indexes of a Series, but exclude given indexes that already applied previously. This will returns the reference
// for itself. The original Series is modified.
func (s Series) SetMutualExclusiveValue(indexes, excludingIndexes Indexes, newvalues Series) Series {
	existingSeries := s
	if err := existingSeries.Err; err != nil {
		return s
	}
	if err := newvalues.Err; err != nil {
		s.Err = fmt.Errorf("set error: argument has errors: %v", err)
		return s
	}
	idx, err := parseIndexes(existingSeries.Len(), indexes)
	if err != nil {
		s.Err = err
		return s
	}

	excludedIdx, err := parseIndexes(existingSeries.Len(), excludingIndexes)
	if err != nil {
		s.Err = err
		return s
	}

	excludedIdxDict := make(map[int]int, len(excludedIdx))
	for _, k := range excludedIdx {
		excludedIdxDict[k] = k
	}

	isBroadcastRightValue := newvalues.Len() == 1
	if existingSeries.Len() != newvalues.Len() && !isBroadcastRightValue {
		s.Err = fmt.Errorf("set error: dimensions mismatch")
		return s
	}
	for _, i := range idx {
		rightValIdx := 0
		if !isBroadcastRightValue {
			rightValIdx = i
		}

		if _, isExcludedIdx := excludedIdxDict[i]; isExcludedIdx {
			continue
		}

		if i < 0 || i >= existingSeries.Len() {
			s.Err = fmt.Errorf("set error: index out of range")
			return s
		}
		newVal := newvalues.elements.Elem(rightValIdx)
		if newVal.IsNA() {
			newVal = nil
		}
		s.elements.Elem(i).Set(newVal)
	}
	return s
}

// HasNaN checks whether the Series contain NaN elements.
func (s Series) HasNaN() bool {
	for i := 0; i < s.Len(); i++ {
		if s.elements.Elem(i).IsNA() {
			return true
		}
	}
	return false
}

// IsNaN returns an array that identifies which of the elements are NaN.
func (s Series) IsNaN() []bool {
	ret := make([]bool, s.Len())
	for i := 0; i < s.Len(); i++ {
		ret[i] = s.elements.Elem(i).IsNA()
	}
	return ret
}

// Compare compares the values of a Series with other elements. To do so, the
// elements with are to be compared are first transformed to a Series of the same
// type as the caller.
func (s Series) Compare(comparator Comparator, comparando interface{}) Series {
	if err := s.Err; err != nil {
		return s
	}
	compareElements := func(a, b Element, c Comparator) (bool, error) {
		var ret bool
		switch c {
		case Eq:
			ret = a.Eq(b)
		case Neq:
			ret = a.Neq(b)
		case Greater:
			ret = a.Greater(b)
		case GreaterEq:
			ret = a.GreaterEq(b)
		case Less:
			ret = a.Less(b)
		case LessEq:
			ret = a.LessEq(b)
		default:
			return false, fmt.Errorf("unknown comparator: %v", c)
		}
		return ret, nil
	}

	bools := make([]bool, s.Len())

	// CompFunc comparator comparison
	if comparator == CompFunc {
		f, ok := comparando.(compFunc)
		if !ok {
			panic("comparando is not a comparison function of type func(el Element) bool")
		}

		for i := 0; i < s.Len(); i++ {
			e := s.elements.Elem(i)
			bools[i] = f(e)
		}

		return Bools(bools)
	}

	// check whether comparando already series
	var comp Series
	switch val := comparando.(type) {
	case Series:
		comp = val
	case *Series:
		comp = *val
	default:
		if comparando == nil {
			comparando = "NaN"
		}
		comp = New(comparando, s.t, "")
	}

	// In comparator comparison
	if comparator == In {
		for i := 0; i < s.Len(); i++ {
			e := s.elements.Elem(i)
			b := false
			for j := 0; j < comp.Len(); j++ {
				m := comp.elements.Elem(j)
				c, err := compareElements(e, m, Eq)
				if err != nil {
					s = s.Empty()
					s.Err = err
					return s
				}
				if c {
					b = true
					break
				}
			}
			bools[i] = b
		}
		return Bools(bools)
	}

	isBroadcastLeftValue := s.Len() == 1
	isBroadcastRightValue := comp.Len() == 1

	if s.Len() != comp.Len() && !isBroadcastLeftValue && !isBroadcastRightValue {
		s := s.Empty()
		s.Err = fmt.Errorf("can't compare: length mismatch")
		return s
	}

	len := s.Len()
	if isBroadcastLeftValue {
		len = comp.Len()
	}
	resultBools := make([]bool, len)
	for i := 0; i < len; i++ {
		leftIdx := 0
		rightIdx := 0
		if !isBroadcastLeftValue {
			leftIdx = i
		}
		if !isBroadcastRightValue {
			rightIdx = i
		}
		e := s.elements.Elem(leftIdx)
		c, err := compareElements(e, comp.elements.Elem(rightIdx), comparator)
		if err != nil {
			s = s.Empty()
			s.Err = err
			return s
		}
		resultBools[i] = c
	}

	return Bools(resultBools)
}

// AND operation in multiple series
// Returning Boolean series
func (s Series) And(rightValues interface{}) Series {
	return s.logicalOperation(And, rightValues)
}

// OR operation in multiple series
// Returning Boolean series
func (s Series) Or(rightValues interface{}) Series {
	return s.logicalOperation(Or, rightValues)
}

// XOR operation in multiple series
// Returning Boolean series
func (s Series) XOr(rightValues interface{}) Series {
	return s.logicalOperation(XOr, rightValues)
}

func (s Series) logicalOperation(operator LogicalOperator, rightValues interface{}) Series {
	res, err := s.Bool()
	if err != nil {
		errSeries := s.Empty()
		errSeries.Err = fmt.Errorf("could not convert to bool")
		return errSeries
	}

	var rValues []interface{}
	rightVal := reflect.ValueOf(rightValues)
	switch rightVal.Kind() {
	case reflect.Slice:
		rValues = make([]interface{}, rightVal.Len())
		for index := 0; index < rightVal.Len(); index++ {
			idxVal := rightVal.Index(index)
			rValues[index] = idxVal.Interface()
		}
	default:
		rValues = []interface{}{rightValues}
	}

	for i := 0; i < len(rValues); i++ {

		var rightSeries Series
		switch val := rValues[i].(type) {
		case Series:
			rightSeries = val
		case *Series:
			rightSeries = *val
		default:
			rightSeries = New(val, s.t, "")
		}
		nexRes, err := rightSeries.Bool()
		if err != nil {
			return Series{Err: err}
		}

		isBroadCastLeft := len(res) == 1
		isBroadCastRight := len(nexRes) == 1

		if len(res) != len(nexRes) && !isBroadCastLeft && !isBroadCastRight {
			errRes := s.Empty()
			errRes.Err = fmt.Errorf("can't compare mismatch length")
			return errRes
		}

		lenOfResult := len(nexRes)
		if lenOfResult < len(res) {
			lenOfResult = len(res)
		}

		newRes := make([]bool, lenOfResult)
		for j := 0; j < lenOfResult; j++ {
			leftVal := res[0]
			if !isBroadCastLeft {
				leftVal = res[j]
			}
			rightVal := nexRes[0]
			if !isBroadCastRight {
				rightVal = nexRes[j]
			}
			switch operator {
			case And:
				newRes[j] = leftVal && rightVal
			case Or:
				newRes[j] = leftVal || rightVal
			case XOr:
				newRes[j] = leftVal != rightVal
			default:
				panic(fmt.Errorf("operator not valid %v", operator))
			}
		}
		res = newRes
	}
	return New(res, Bool, "")
}

// Copy will return a copy of the Series.
func (s Series) Copy() Series {
	return s.CopyWithName(s.Name)
}

func (s Series) CopyWithName(colName string) Series {
	name := colName
	t := s.t
	err := s.Err
	var elements Elements
	switch s.t {
	case String:
		elements = make(stringElements, s.Len())
		copy(elements.(stringElements), s.elements.(stringElements))
	case Float:
		elements = make(floatElements, s.Len())
		copy(elements.(floatElements), s.elements.(floatElements))
	case Bool:
		elements = make(boolElements, s.Len())
		copy(elements.(boolElements), s.elements.(boolElements))
	case Int:
		elements = make(intElements, s.Len())
		copy(elements.(intElements), s.elements.(intElements))
	case StringList:
		elements = make(stringListElements, s.Len())
		copy(elements.(stringListElements), s.elements.(stringListElements))
	case FloatList:
		elements = make(floatListElements, s.Len())
		copy(elements.(floatListElements), s.elements.(floatListElements))
	case BoolList:
		elements = make(boolListElements, s.Len())
		copy(elements.(boolListElements), s.elements.(boolListElements))
	case IntList:
		elements = make(intListElements, s.Len())
		copy(elements.(intListElements), s.elements.(intListElements))
	}
	ret := Series{
		Name:     name,
		t:        t,
		elements: elements,
		Err:      err,
	}
	return ret
}

// Records returns the elements of a Series as a []string
func (s Series) Records() []string {
	ret := make([]string, s.Len())
	for i := 0; i < s.Len(); i++ {
		e := s.elements.Elem(i)
		ret[i] = e.String()
	}
	return ret
}

// Float returns the elements of a Series as a []float64. If the elements can not
// be converted to float64 or contains a NaN returns the float representation of
// NaN.
func (s Series) Float() []float64 {
	ret := make([]float64, s.Len())
	for i := 0; i < s.Len(); i++ {
		e := s.elements.Elem(i)
		ret[i] = e.Float()
	}
	return ret
}

// Int returns the elements of a Series as a []int or an error if the
// transformation is not possible.
func (s Series) Int() ([]int, error) {
	ret := make([]int, s.Len())
	for i := 0; i < s.Len(); i++ {
		e := s.elements.Elem(i)
		val, err := e.Int()
		if err != nil {
			return nil, err
		}
		ret[i] = val
	}
	return ret, nil
}

// Bool returns the elements of a Series as a []bool or an error if the
// transformation is not possible.
func (s Series) Bool() ([]bool, error) {
	ret := make([]bool, s.Len())
	for i := 0; i < s.Len(); i++ {
		e := s.elements.Elem(i)
		val, err := e.Bool()
		if err != nil {
			return nil, err
		}
		ret[i] = val
	}
	return ret, nil
}

// StringList returns the elements of a Series as a [][]string
func (s Series) StringList() [][]string {
	ret := make([][]string, s.Len())
	for i := 0; i < s.Len(); i++ {
		e := s.elements.Elem(i)
		ret[i] = e.StringList()
	}
	return ret
}

// IntList returns the elements of a Series as a [][]int or an error if the
// transformation is not possible.
func (s Series) IntList() ([][]int, error) {
	ret := make([][]int, s.Len())
	for i := 0; i < s.Len(); i++ {
		e := s.elements.Elem(i)
		val, err := e.IntList()
		if err != nil {
			return nil, err
		}
		ret[i] = val
	}
	return ret, nil
}

// FloatList returns the elements of a Series as a [][]float64. If the elements can not
// be converted to float64 or contains a NaN returns the float representation of
// NaN.
func (s Series) FloatList() [][]float64 {
	ret := make([][]float64, s.Len())
	for i := 0; i < s.Len(); i++ {
		e := s.elements.Elem(i)
		ret[i] = e.FloatList()
	}
	return ret
}

// BoolList returns the elements of a Series as a [][]bool or an error if the
// transformation is not possible.
func (s Series) BoolList() ([][]bool, error) {
	ret := make([][]bool, s.Len())
	for i := 0; i < s.Len(); i++ {
		e := s.elements.Elem(i)
		val, err := e.BoolList()
		if err != nil {
			return nil, err
		}
		ret[i] = val
	}
	return ret, nil
}

// Type returns the type of a given series
func (s Series) Type() Type {
	return s.t
}

// IsListElement returns true if the series' element type is list type.
func (s Series) IsListElement() bool {
	return s.t == StringList || s.t == IntList || s.t == FloatList || s.t == BoolList
}

// Len returns the length of a given Series
func (s Series) Len() int {
	return s.elements.Len()
}

// String implements the Stringer interface for Series
func (s Series) String() string {
	return fmt.Sprint(s.elements)
}

// Str prints some extra information about a given series
func (s Series) Str() string {
	var ret []string
	// If name exists print name
	if s.Name != "" {
		ret = append(ret, "Name: "+s.Name)
	}
	ret = append(ret, "Type: "+fmt.Sprint(s.t))
	ret = append(ret, "Length: "+fmt.Sprint(s.Len()))
	if s.Len() != 0 {
		ret = append(ret, "Values: "+fmt.Sprint(s))
	}
	return strings.Join(ret, "\n")
}

// Val returns the value of a series for the given index. Will panic if the index
// is out of bounds.
func (s Series) Val(i int) interface{} {
	return s.elements.Elem(i).Val()
}

// Elem returns the element of a series for the given index. Will panic if the
// index is out of bounds.
func (s Series) Elem(i int) Element {
	return s.elements.Elem(i)
}

// parseIndexes will parse the given indexes for a given series of length `l`. No
// out of bounds checks is performed.
func parseIndexes(l int, indexes Indexes) ([]int, error) {
	var idx []int
	switch idxs := indexes.(type) {
	case []int:
		idx = idxs
	case int:
		idx = []int{idxs}
	case []bool:
		bools := idxs
		if len(bools) != l && len(bools) != 1 {
			return nil, fmt.Errorf("indexing error: index dimensions mismatch")
		}
		isBroadcasted := len(bools) == 1
		for i := 0; i < l; i++ {
			index := 0
			if !isBroadcasted {
				index = i
			}
			if b := bools[index]; b {
				idx = append(idx, i)
			}
		}
	case Series:
		s := idxs
		if err := s.Err; err != nil {
			return nil, fmt.Errorf("indexing error: new values has errors: %v", err)
		}
		if s.HasNaN() {
			return nil, fmt.Errorf("indexing error: indexes contain NaN")
		}
		switch s.t {
		case Int:
			return s.Int()
		case Bool:
			bools, err := s.Bool()
			if err != nil {
				return nil, fmt.Errorf("indexing error: %v", err)
			}
			return parseIndexes(l, bools)
		default:
			return nil, fmt.Errorf("indexing error: unknown indexing mode")
		}
	default:
		return nil, fmt.Errorf("indexing error: unknown indexing mode")
	}
	return idx, nil
}

// Order returns the indexes for sorting a Series. NaN or nil elements are pushed to the
// end by order of appearance. Empty elements are pushed to the beginning by order of
// appearance.
func (s Series) Order(reverse bool) []int {
	var ie indexedElements
	var nasIdx []int
	for i := 0; i < s.Len(); i++ {
		e := s.elements.Elem(i)
		if e.IsNA() {
			nasIdx = append(nasIdx, i)
		} else {
			ie = append(ie, indexedElement{i, e, s.IsListElement()})
		}
	}
	var srt sort.Interface
	srt = ie
	if reverse {
		srt = sort.Reverse(srt)
	}
	sort.Stable(srt)
	var ret []int
	for _, e := range ie {
		ret = append(ret, e.index)
	}
	return append(ret, nasIdx...)
}

type indexedElement struct {
	index         int
	element       Element
	IsListElement bool
}

type indexedElements []indexedElement

func (e indexedElements) Len() int { return len(e) }
func (e indexedElements) Less(i, j int) bool {
	if e[i].IsListElement {
		return e[i].element.LessEq(e[j].element)
	}
	return e[i].element.Less(e[j].element)
}
func (e indexedElements) Swap(i, j int) { e[i], e[j] = e[j], e[i] }

// StdDev calculates the standard deviation of a series.
// If a series is a list element type, flatten the series first.
func (s Series) StdDev() float64 {
	if s.IsListElement() {
		s = s.Flatten()
	}

	stdDev := stat.StdDev(s.Float(), nil)
	return stdDev
}

// Mean calculates the average value of a series.
// If a series is a list element type, flatten the series first.
func (s Series) Mean() float64 {
	if s.IsListElement() {
		s = s.Flatten()
	}

	mean := stat.Mean(s.Float(), nil)
	return mean
}

// Median calculates the middle or median value, as opposed to
// mean, and there is less susceptible to being affected by outliers.
// If a series is a list element type, flatten the series first.
func (s Series) Median() float64 {
	if s.IsListElement() {
		s = s.Flatten()
	}

	if s.elements.Len() == 0 ||
		s.Type() == String ||
		s.Type() == Bool {
		return math.NaN()
	}
	ix := s.Order(false)
	newElem := make([]Element, len(ix))

	for newpos, oldpos := range ix {
		newElem[newpos] = s.elements.Elem(oldpos)
	}

	// When length is odd, we just take length(list)/2
	// value as the median.
	if len(newElem)%2 != 0 {
		return newElem[len(newElem)/2].Float()
	}
	// When length is even, we take middle two elements of
	// list and the median is an average of the two of them.
	return (newElem[(len(newElem)/2)-1].Float() +
		newElem[len(newElem)/2].Float()) * 0.5
}

// Max return the biggest element in the series.
// If a series is a list element type, flatten the series first.
func (s Series) Max() float64 {
	if s.IsListElement() {
		s = s.Flatten()
	}

	if s.elements.Len() == 0 || s.Type() == String {
		return math.NaN()
	}

	max := s.elements.Elem(0)
	for i := 1; i < s.elements.Len(); i++ {
		elem := s.elements.Elem(i)
		if elem.Greater(max) {
			max = elem
		}
	}
	return max.Float()
}

// MaxStr return the biggest element in a series of type String.
// If a series is a list element type, flatten the series first.
func (s Series) MaxStr() string {
	if s.IsListElement() {
		s = s.Flatten()
	}

	if s.elements.Len() == 0 || s.Type() != String {
		return ""
	}

	max := s.elements.Elem(0)
	for i := 1; i < s.elements.Len(); i++ {
		elem := s.elements.Elem(i)
		if elem.Greater(max) {
			max = elem
		}
	}
	return max.String()
}

// Min return the lowest element in the series.
// If a series is a list element type, flatten the series first.
func (s Series) Min() float64 {
	if s.IsListElement() {
		s = s.Flatten()
	}

	if s.elements.Len() == 0 || s.Type() == String {
		return math.NaN()
	}

	min := s.elements.Elem(0)
	for i := 1; i < s.elements.Len(); i++ {
		elem := s.elements.Elem(i)
		if elem.Less(min) {
			min = elem
		}
	}
	return min.Float()
}

// MinStr return the lowest element in a series of type String.
// If a series is a list element type, flatten the series first.
func (s Series) MinStr() string {
	if s.IsListElement() {
		s = s.Flatten()
	}

	if s.elements.Len() == 0 || s.Type() != String {
		return ""
	}

	min := s.elements.Elem(0)
	for i := 1; i < s.elements.Len(); i++ {
		elem := s.elements.Elem(i)
		if elem.Less(min) {
			min = elem
		}
	}
	return min.String()
}

// Quantile returns the sample of x such that x is greater than or
// equal to the fraction p of samples.
// Note: gonum/stat panics when called with strings.
// If a series is a list element type, flatten the series first.
func (s Series) Quantile(p float64) float64 {
	if s.IsListElement() {
		s = s.Flatten()
	}

	if s.Type() == String || s.Len() == 0 {
		return math.NaN()
	}

	ordered := s.Subset(s.Order(false)).Float()

	return stat.Quantile(p, stat.Empirical, ordered, nil)
}

// Map applies a function matching MapFunction signature, which itself
// allowing for a fairly flexible MAP implementation, intended for mapping
// the function over each element in Series and returning a new Series object.
// Function must be compatible with the underlying type of data in the Series.
// In other words it is expected that when working with a Float Series, that
// the function passed in via argument `f` will not expect another type, but
// instead expects to handle Element(s) of type Float.
func (s Series) Map(f MapFunction) Series {
	mappedValues := make([]Element, s.Len())
	for i := 0; i < s.Len(); i++ {
		value := f(s.elements.Elem(i))
		mappedValues[i] = value
	}
	return New(mappedValues, s.Type(), s.Name)
}

// Sum calculates the sum value of a series.
// If a series is a list element type, flatten the series first.
func (s Series) Sum() float64 {
	if s.IsListElement() {
		s = s.Flatten()
	}

	if s.elements.Len() == 0 || s.Type() == String || s.Type() == Bool {
		return math.NaN()
	}
	sFloat := s.Float()
	sum := sFloat[0]
	for i := 1; i < len(sFloat); i++ {
		elem := sFloat[i]
		sum += elem
	}
	return sum
}

// Slice slices Series from j to k-1 index.
func (s Series) Slice(j, k int) Series {
	if s.Err != nil {
		return s
	}

	if j > k || j < 0 || k > s.Len() {
		empty := s.Empty()
		empty.Err = fmt.Errorf("slice index out of bounds")
		return empty
	}

	idxs := make([]int, k-j)
	for i := 0; j+i < k; i++ {
		idxs[i] = j + i
	}

	return s.Subset(idxs)
}

// Flatten returns the flattened elements of series. If the series is list type (2D), it returns the standard type (1D).
// Examples:
// - Strings([]string{"A", "B", "C"}) -> Strings([]string{"A", "B", "C"})
// - IntsList([][]int{{1, 11}, {3, 33}}) -> Ints([]int{1, 11, 3, 33})
func (s Series) Flatten() Series {
	switch s.Type() {
	case StringList:
		elements := []string{}
		for i := 0; i < s.elements.Len(); i++ {
			elements = append(elements, s.elements.Elem(i).StringList()...)
		}
		return New(elements, String, s.Name)
	case IntList:
		elements := []int{}
		for i := 0; i < s.elements.Len(); i++ {
			l, err := s.elements.Elem(i).IntList()
			if err != nil {
				continue
			}
			elements = append(elements, l...)
		}
		return New(elements, Int, s.Name)
	case FloatList:
		elements := []float64{}
		for i := 0; i < s.elements.Len(); i++ {
			elements = append(elements, s.elements.Elem(i).FloatList()...)
		}
		return New(elements, Float, s.Name)
	case BoolList:
		elements := []bool{}
		for i := 0; i < s.elements.Len(); i++ {
			l, err := s.elements.Elem(i).BoolList()
			if err != nil {
				continue
			}
			elements = append(elements, l...)
		}
		return New(elements, Bool, s.Name)
	default:
		return s
	}
}

// Unique returns unique values based on a hash table.
// Examples:
// - Strings([]string{"A", "B", "C", "A", "B"}) -> Strings([]string{"A", "B", "C"})
// - IntsList([][]int{{1, 11}, {3, 33}, {3, 33}}) -> IntsList([][]int{{1, 11}, {3, 33}})
func (s Series) Unique() Series {
	switch s.Type() {
	case StringList:
		l := s.elements.Len()
		m := make(map[uint64]int, l)
		elements := [][]string{}
		for i := 0; i < l; i++ {
			key := xxhash.Sum64String(strings.Join(s.elements.Elem(i).StringList(), ":"))
			if _, ok := m[key]; ok {
				continue
			}

			m[key] = 1
			elements = append(elements, s.elements.Elem(i).StringList())
		}
		return New(elements, s.Type(), s.Name)
	case IntList:
		l := s.elements.Len()
		m := make(map[uint64]int, l)
		elements := [][]int{}
		for i := 0; i < l; i++ {
			list, err := s.elements.Elem(i).IntList()
			if err != nil {
				continue
			}

			h := xxhash.New()
			for _, i := range list {
				h.WriteString(fmt.Sprintf("%v:", i))
			}
			key := h.Sum64()

			if _, ok := m[key]; ok {
				continue
			}

			m[key] = 1
			elements = append(elements, list)
		}
		return New(elements, s.Type(), s.Name)
	case FloatList:
		l := s.elements.Len()
		m := make(map[uint64]int, l)
		elements := [][]float64{}
		for i := 0; i < l; i++ {
			list := s.elements.Elem(i).FloatList()

			h := xxhash.New()
			for _, i := range list {
				h.WriteString(fmt.Sprintf("%v:", i))
			}
			key := h.Sum64()

			if _, ok := m[key]; ok {
				continue
			}

			m[key] = 1
			elements = append(elements, s.elements.Elem(i).FloatList())
		}
		return New(elements, s.Type(), s.Name)
	case BoolList:
		l := s.elements.Len()
		m := make(map[uint64]int, l)
		elements := [][]bool{}
		for i := 0; i < l; i++ {
			list, err := s.elements.Elem(i).BoolList()
			if err != nil {
				continue
			}

			h := xxhash.New()
			for _, i := range list {
				h.WriteString(fmt.Sprintf("%v:", i))
			}
			key := h.Sum64()

			if _, ok := m[key]; ok {
				continue
			}

			m[key] = 1
			elements = append(elements, list)
		}
		return New(elements, s.Type(), s.Name)
	default:
		l := s.elements.Len()
		m := make(map[uint64]int, l)
		elements := []interface{}{}
		for i := 0; i < l; i++ {
			key := xxhash.Sum64String(fmt.Sprint(s.elements.Elem(i)))
			if _, ok := m[key]; ok {
				continue
			}

			m[key] = 1
			elements = append(elements, s.elements.Elem(i))
		}
		return New(elements, s.Type(), s.Name)
	}
}
