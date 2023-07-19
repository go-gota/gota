package series

var _ Series = (*immutableSeries)(nil)

//immutableSeries is an immutable series and the series can not be modified.
type immutableSeries struct {
	Series
}

func newImmutableSeries(s Series) Series {
	ret := &immutableSeries{
		Series: s,
	}
	return ret
}

func (s immutableSeries) Elem(i int) Element {
	ele := &immutableElement{
		Element: s.Series.Elem(i),
	}
	return ele
}

func (s *immutableSeries) Immutable() Series {
	return s
}

func (s immutableSeries) Slice(start, end int) Series {
	ret := &immutableSeries{
		Series: s.Series.Slice(start, end),
	}
	return ret
}
func (s *immutableSeries) FillNaN(value ElementValue) {
	panic("The method[FillNaN] is not supported by immutableSeries")
}
func (s *immutableSeries) FillNaNForward() {
	panic("The method[FillNaNForward] is not supported by immutableSeries")
}
func (s *immutableSeries) FillNaNBackward() {
	panic("The method[FillNaNBackward] is not supported by immutableSeries")
}
func (s *immutableSeries) Set(indexes Indexes, newvalues Series) Series {
	panic("The method[Set] is not supported by immutableSeries")
}
func (s *immutableSeries) Append(values interface{}) {
	panic("The method[Append] is not supported by immutableSeries")
}

//immutableElement is an immutable element and the element can not be modified.
type immutableElement struct {
	Element
}

func (e *immutableElement) Set(interface{}) {
	panic("The method[Set] is not supported by immutableElement")
}
func (e *immutableElement) SetElement(val Element) {
	panic("The method[SetElement] is not supported by immutableElement")
}
func (e *immutableElement) SetBool(val bool) {
	panic("The method[SetBool] is not supported by immutableElement")
}
func (e *immutableElement) SetFloat(val float64) {
	panic("The method[SetFloat] is not supported by immutableElement")
}
func (e *immutableElement) SetInt(val int) {
	panic("The method[SetInt] is not supported by immutableElement")
}
func (e *immutableElement) SetString(val string) {
	panic("The method[SetString] is not supported by immutableElement")
}
