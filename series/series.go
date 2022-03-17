package series

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"math"

	"gonum.org/v1/gonum/floats"
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
	Slice(start, end int) Elements
	Get(indexs ...int) Elements
	Append(Elements) Elements
	Copy() Elements
}

// Element is the interface that defines the types of methods to be present for
// elements of a Series
type Element interface {
	// Setter method
	Set(interface{})
	SetElement(val Element)
	SetBool(val bool)
	SetFloat(val float64)
	SetInt(val int)
	SetString(val string)

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

	// Information methods
	IsNA() bool
	Type() Type
}

// intElements is the concrete implementation of Elements for Int elements.
type intElements []intElement

func (e intElements) Len() int                      { return len(e) }
func (e intElements) Elem(i int) Element            { return &e[i] }
func (e intElements) Slice(start, end int) Elements { return e[start:end] }
func (e intElements) Get(indexs ...int) Elements {
	elements := make(intElements, len(indexs))
	for k, i := range indexs {
		elements[k] = e[i]
	}
	return elements
}
func (e intElements) Append(elements Elements) Elements {
	eles := elements.(intElements)
	ret := append(e, eles...)
	return ret
}
func (e intElements) Copy() Elements {
	elements := make(intElements, len(e))
	copy(elements, e)
	return elements
}

// stringElements is the concrete implementation of Elements for String elements.
type stringElements []stringElement

func (e stringElements) Len() int                      { return len(e) }
func (e stringElements) Elem(i int) Element            { return &e[i] }
func (e stringElements) Slice(start, end int) Elements { return e[start:end] }
func (e stringElements) Get(indexs ...int) Elements {
	elements := make(stringElements, len(indexs))
	for k, i := range indexs {
		elements[k] = e[i]
	}
	return elements
}
func (e stringElements) Append(elements Elements) Elements {
	eles := elements.(stringElements)
	ret := append(e, eles...)
	return ret
}
func (e stringElements) Copy() Elements {
	elements := make(stringElements, len(e))
	copy(elements, e)
	return elements
}

// floatElements is the concrete implementation of Elements for Float elements.
type floatElements []floatElement

func (e floatElements) Len() int                      { return len(e) }
func (e floatElements) Elem(i int) Element            { return &e[i] }
func (e floatElements) Slice(start, end int) Elements { return e[start:end] }
func (e floatElements) Get(indexs ...int) Elements {
	elements := make(floatElements, len(indexs))
	for k, i := range indexs {
		elements[k] = e[i]
	}
	return elements
}
func (e floatElements) Append(elements Elements) Elements {
	eles := elements.(floatElements)
	ret := append(e, eles...)
	return ret
}
func (e floatElements) Copy() Elements {
	elements := make(floatElements, len(e))
	copy(elements, e)
	return elements
}

// boolElements is the concrete implementation of Elements for Bool elements.
type boolElements []boolElement

func (e boolElements) Len() int                      { return len(e) }
func (e boolElements) Elem(i int) Element            { return &e[i] }
func (e boolElements) Slice(start, end int) Elements { return e[start:end] }
func (e boolElements) Get(indexs ...int) Elements {
	elements := make(boolElements, len(indexs))
	for k, i := range indexs {
		elements[k] = e[i]
	}
	return elements
}
func (e boolElements) Append(elements Elements) Elements {
	eles := elements.(boolElements)
	ret := append(e, eles...)
	return ret
}
func (e boolElements) Copy() Elements {
	elements := make(boolElements, len(e))
	copy(elements, e)
	return elements
}

// ElementValue represents the value that can be used for marshaling or
// unmarshaling Elements.
type ElementValue interface{}

type MapFunction func(ele Element, index int) Element

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
	String Type = "string"
	Int    Type = "int"
	Float  Type = "float"
	Bool   Type = "bool"
)

func (t Type) emptyElements(n int) Elements {
	var elements Elements
	switch t {
	case String:
		elements = make(stringElements, n)
	case Int:
		elements = make(intElements, n)
	case Float:
		elements = make(floatElements, n)
	case Bool:
		elements = make(boolElements, n)
	default:
		panic(fmt.Sprintf("unknown type %v", t))
	}
	return elements
}

const NaN = "NaN"

// Indexes represent the elements that can be used for selecting a subset of
// elements within a Series. Currently supported are:
//
//     int            // Matches the given index number
//     []int          // Matches all given index numbers
//     []bool         // Matches all elements in a Series marked as true
//     Series [Int]   // Same as []int
//     Series [Bool]  // Same as []bool
type Indexes interface{}

// New is the generic Series constructor
func New(values interface{}, t Type, name string) Series {
	ret := Series{
		Name: name,
		t:    t,
	}

	// Pre-allocate elements
	preAlloc := func(n int) {
		ret.elements = t.emptyElements(n)
	}

	if values == nil {
		preAlloc(1)
		ret.elements.Elem(0).Set(nil)
		return ret
	}

	switch v := values.(type) {
	case []string:
		l := len(v)
		preAlloc(l)
		for i := 0; i < l; i++ {
			ret.elements.Elem(i).SetString(v[i])
		}
	case []float64:
		l := len(v)
		preAlloc(l)
		for i := 0; i < l; i++ {
			ret.elements.Elem(i).SetFloat(v[i])
		}
	case []int:
		l := len(v)
		preAlloc(l)
		for i := 0; i < l; i++ {
			ret.elements.Elem(i).SetInt(v[i])
		}
	case []bool:
		l := len(v)
		preAlloc(l)
		for i := 0; i < l; i++ {
			ret.elements.Elem(i).SetBool(v[i])
		}
	case []Element:
		l := len(v)
		preAlloc(l)
		for i := 0; i < l; i++ {
			ret.elements.Elem(i).SetElement(v[i])
		}
	case Series:
		l := v.Len()
		preAlloc(l)
		for i := 0; i < l; i++ {
			ret.elements.Elem(i).SetElement(v.elements.Elem(i))
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

func NewDefault(defaultValue interface{}, t Type, name string, len int) Series {
	ret := Series{
		Name: name,
		t:    t,
	}

	// Pre-allocate elements
	preAlloc := func(n int) {
		ret.elements = t.emptyElements(n)
	}

	if defaultValue == nil {
		preAlloc(1)
		ret.elements.Elem(0).Set(nil)
		return ret
	}
	preAlloc(len)

	switch v := defaultValue.(type) {
	case string:
		for i := 0; i < len; i++ {
			ret.elements.Elem(i).SetString(v)
		}
	case float64:
		for i := 0; i < len; i++ {
			ret.elements.Elem(i).SetFloat(v)
		}
	case int:
		for i := 0; i < len; i++ {
			ret.elements.Elem(i).SetInt(v)
		}
	case bool:
		for i := 0; i < len; i++ {
			ret.elements.Elem(i).SetBool(v)
		}
	case Element:
		for i := 0; i < len; i++ {
			ret.elements.Elem(i).SetElement(v)
		}
	default:
		for i := 0; i < len; i++ {
			ret.elements.Elem(i).Set(defaultValue)
		}
	}
	return ret
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
	s.elements = s.elements.Append(news.elements)
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
		Name:     s.Name,
		t:        s.t,
		elements: s.elements.Get(idx...),
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
		s.elements.Elem(i).SetElement(newvalues.elements.Elem(k))
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

// IsNotNaN returns an array that identifies which of the elements are not NaN.
func (s Series) IsNotNaN() []bool {
	ret := make([]bool, s.Len())
	for i := 0; i < s.Len(); i++ {
		ret[i] = !s.elements.Elem(i).IsNA()
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

	comp := New(comparando, s.t, "")
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

	// Single element comparison
	if comp.Len() == 1 {
		for i := 0; i < s.Len(); i++ {
			e := s.elements.Elem(i)
			c, err := compareElements(e, comp.elements.Elem(0), comparator)
			if err != nil {
				s = s.Empty()
				s.Err = err
				return s
			}
			bools[i] = c
		}
		return Bools(bools)
	}

	// Multiple element comparison
	if s.Len() != comp.Len() {
		s := s.Empty()
		s.Err = fmt.Errorf("can't compare: length mismatch")
		return s
	}
	for i := 0; i < s.Len(); i++ {
		e := s.elements.Elem(i)
		c, err := compareElements(e, comp.elements.Elem(i), comparator)
		if err != nil {
			s = s.Empty()
			s.Err = err
			return s
		}
		bools[i] = c
	}
	return Bools(bools)
}

// Copy will return a copy of the Series.
func (s Series) Copy() Series {
	ret := Series{
		Name:     s.Name,
		t:        s.t,
		elements: s.elements.Copy(),
		Err:      s.Err,
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

// Type returns the type of a given series
func (s Series) Type() Type {
	return s.t
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
// The index could be less than 0. When the index equals -1, Elem returns the last element of a series.
func (s Series) Elem(i int) Element {
	if i < 0 {
		return s.elements.Elem(s.Len() + i)
	}
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
		if len(bools) != l {
			return nil, fmt.Errorf("indexing error: index dimensions mismatch")
		}
		for i, b := range bools {
			if b {
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

// Order returns the indexes for sorting a Series. NaN elements are pushed to the
// end by order of appearance.
func (s Series) Order(reverse bool) []int {
	var ie indexedElements
	var nasIdx []int
	for i := 0; i < s.Len(); i++ {
		e := s.elements.Elem(i)
		if e.IsNA() {
			nasIdx = append(nasIdx, i)
		} else {
			ie = append(ie, indexedElement{i, e})
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
	index   int
	element Element
}

type indexedElements []indexedElement

func (e indexedElements) Len() int           { return len(e) }
func (e indexedElements) Less(i, j int) bool { return e[i].element.Less(e[j].element) }
func (e indexedElements) Swap(i, j int)      { e[i], e[j] = e[j], e[i] }

// StdDev calculates the standard deviation of a series
func (s Series) StdDev() float64 {
	stdDev := stat.StdDev(s.Float(), nil)
	return stdDev
}

// Mean calculates the average value of a series
func (s Series) Mean() float64 {
	stdDev := stat.Mean(s.Float(), nil)
	return stdDev
}

// Median calculates the middle or median value, as opposed to
// mean, and there is less susceptible to being affected by outliers.
func (s Series) Median() float64 {
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

// Max return the biggest element in the series
func (s Series) Max() float64 {
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

// MaxStr return the biggest element in a series of type String
func (s Series) MaxStr() string {
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

// Min return the lowest element in the series
func (s Series) Min() float64 {
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

// MinStr return the lowest element in a series of type String
func (s Series) MinStr() string {
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
// Note: gonum/stat panics when called with strings
func (s Series) Quantile(p float64) float64 {
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
	eles := s.Type().emptyElements(s.Len())
	for i := 0; i < s.Len(); i++ {
		value := f(s.elements.Elem(i), i)
		eles.Elem(i).SetElement(value)
	}
	ret := Series{
		Name:     s.Name,
		elements: eles,
		t:        s.Type(),
		Err:      nil,
	}
	return ret
}

//Shift series by desired number of periods and returning a new Series object.
func (s Series) Shift(periods int) Series {
	if s.Len() == 0 {
		return s.Empty()
	}
	if periods == 0 {
		return s.Copy()
	}

	naLen := periods
	if naLen < 0 {
		naLen = -naLen
	}
	naEles := s.t.emptyElements(naLen)
	for i := 0; i < naLen; i++ {
		naEles.Elem(i).Set(NaN)
	}

	var shiftElements Elements
	if periods < 0 {
		//shift up
		shiftElements = s.elements.Slice(-periods, s.Len()).Copy().Append(naEles)
	} else if periods > 0 {
		//move down
		shiftElements = naEles.Append(s.elements.Slice(0, s.Len()-periods))
	}
	ret := Series{
		Name:     fmt.Sprintf("%s_Shift_%d", s.Name, periods),
		elements: shiftElements,
		t:        s.t,
		Err:      nil,
	}
	return ret
}

// CumProd finds the cumulative product of the first i elements in s and returning a new Series object.
func (s Series) CumProd() Series {
	dst := make([]float64, s.Len())
	floats.CumProd(dst, s.Float())
	return New(dst, s.Type(), fmt.Sprintf("%s_CumProd", s.Name))
}

// Prod returns the product of the elements of the Series. Returns 1 if len(s) = 0.
func (s Series) Prod() float64 {
	return floats.Prod(s.Float())
}

// AddConst adds the scalar c to all of the values in Series and returning a new Series object.
func (s Series) AddConst(c float64) Series {
	dst := s.Float()
	floats.AddConst(c, dst)
	return New(dst, s.Type(), fmt.Sprintf("(%s + %v)", s.Name, c))
}

// AddConst multiply the scalar c to all of the values in Series and returning a new Series object.
func (s Series) MulConst(c float64) Series {
	sm := s.Map(func(e Element, index int) Element {
		result := e.Copy()
		f := result.Float()
		result.Set(f * c)
		return result
	})
	sm.Name = fmt.Sprintf("(%s * %v)", s.Name, c)
	return sm
}

// DivConst Div the scalar c to all of the values in Series and returning a new Series object.
func (s Series) DivConst(c float64) Series {
	sm := s.Map(func(e Element, index int) Element {
		result := e.Copy()
		f := result.Float()
		result.Set(f / c)
		return result
	})
	sm.Name = fmt.Sprintf("(%s / %v)", s.Name, c)
	return sm
}

func (s Series) Add(c Series) Series {
	sf := s.Float()
	cf := c.Float()
	dst := make([]float64, s.Len())
	floats.AddTo(dst, sf, cf)
	return New(dst, Float, fmt.Sprintf("(%s + %s)", s.Name, c.Name))
}

func (s Series) Sub(c Series) Series {
	sf := s.Float()
	cf := c.Float()
	dst := make([]float64, s.Len())
	floats.SubTo(dst, sf, cf)
	return New(dst, Float, fmt.Sprintf("(%s - %s)", s.Name, c.Name))
}

func (s Series) Mul(c Series) Series {
	sf := s.Float()
	cf := c.Float()
	dst := make([]float64, s.Len())
	floats.MulTo(dst, sf, cf)
	return New(dst, Float, fmt.Sprintf("(%s * %s)", s.Name, c.Name))
}

func (s Series) Div(c Series) Series {
	sf := s.Float()
	cf := c.Float()
	dst := make([]float64, s.Len())
	floats.DivTo(dst, sf, cf)
	return New(dst, Float, fmt.Sprintf("(%s / %s)", s.Name, c.Name))
}

func (s Series) Abs() Series {
	sm := s.Map(func(e Element, index int) Element {
		result := e.Copy()
		f := result.Float()
		result.Set(math.Abs(f))
		return result
	})
	sm.Name = fmt.Sprintf("Abs(%s)", s.Name)
	return sm
}

// FillNaN Fill NaN values using the specified value.
func (s Series) FillNaN(value ElementValue) {
	for i := 0; i < s.Len(); i++ {
		ele := s.Elem(i)
		if ele.IsNA() {
			ele.Set(value)
		}
	}
}

// FillNaNForward Fill NaN values using the last non-NaN value
func (s Series) FillNaNForward() {
	var lastNotNaNValue ElementValue = nil
	for i := 0; i < s.Len(); i++ {
		ele := s.Elem(i)
		if !ele.IsNA() {
			lastNotNaNValue = ele.Val()
		} else {
			if lastNotNaNValue != nil {
				ele.Set(lastNotNaNValue)
			}
		}
	}
}

// FillNaNBackward fill NaN values using the next non-NaN value
func (s Series) FillNaNBackward() {
	var lastNotNaNValue ElementValue = nil
	for i := s.Len() - 1; i >= 0; i-- {
		ele := s.Elem(i)
		if !ele.IsNA() {
			lastNotNaNValue = ele.Val()
		} else {
			if lastNotNaNValue != nil {
				ele.Set(lastNotNaNValue)
			}
		}
	}
}

func (s Series) Rolling(window int, minPeriods int) RollingSeries {
	return NewRollingSeries(window, minPeriods, s)
}

//Operation for multiple series calculation
func Operation(operate func(index int, eles ...Element) interface{}, seriess ...Series) (Series, error) {
	if len(seriess) == 0 {
		return Series{}, errors.New("seriess num must > 0")
	}
	sl := seriess[0].Len()
	maxLen := sl
	for i := 1; i < len(seriess); i++ {
		slen := seriess[i].Len()
		if sl != slen && slen != 1 {
			return Series{}, errors.New("seriess length must be 1 or same")
		}
		if slen > maxLen {
			maxLen = slen
		}
	}

	t := seriess[0].t
	eles := t.emptyElements(maxLen)
	for i := 0; i < maxLen; i++ {
		operateParam := make([]Element, len(seriess))
		for j := 0; j < len(seriess); j++ {
			if seriess[j].Len() == 1 {
				operateParam[j] = seriess[j].Elem(0)
			} else {
				operateParam[j] = seriess[j].Elem(i)
			}
		}
		res := operate(i, operateParam...)
		eles.Elem(i).Set(res)
	}
	result := Series{
		Name:     "",
		elements: eles,
		t:        t,
		Err:      nil,
	}
	return result, nil
}

// Sum calculates the sum value of a series
func (s Series) Sum() float64 {
	if s.elements.Len() == 0 || s.Type() == String || s.Type() == Bool {
		return math.NaN()
	}
	sFloat := s.Float()
	sum := sFloat[0]
	for i := 1; i < len(sFloat); i++ {
		sum += sFloat[i]
	}
	return sum
}

// Slice slices Series from start to end-1 index.
func (s Series) Slice(start, end int) Series {
	if s.Err != nil {
		return s
	}

	if start > end || start < 0 || end > s.Len() {
		empty := s.Empty()
		empty.Err = fmt.Errorf("slice index out of bounds")
		return empty
	}

	ret := Series{
		Name: s.Name,
		t:    s.t,
	}
	ret.elements = s.elements.Slice(start, end)
	return ret
}
