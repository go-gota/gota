package series

import "fmt"

func (s Series) And(in interface{}) Series {
	result := seriesLogic(s, in, func(e1, e2 Element) bool {
		e1b,_ := e1.Bool()
		e2b,_ := e2.Bool()
		return e1b && e2b
	})
	return result
}

func (s Series) Or(in interface{}) Series {
	result := seriesLogic(s, in, func(e1, e2 Element) bool {
		e1b,_ := e1.Bool()
		e2b,_ := e2.Bool()
		return e1b || e2b
	})
	return result
}

func seriesLogic (s Series, in interface{}, elementLogic func(e1, e2 Element) bool) Series {
	inSeries := New(in, s.t, "")
	if inSeries.Len() != 1 && inSeries.Len() != s.Len() {
		s := s.Empty()
		s.Err = fmt.Errorf("length mismatch")
		return s
	}
	bools := make([]bool, s.Len())

	if inSeries.Len() == 1 {
		for i := 0; i < s.Len(); i++ {
			bools[i] =  elementLogic(s.elements.Elem(i), inSeries.elements.Elem(0))
		}
		return Bools(bools)
	} else {
		for i := 0; i < s.Len(); i++ {
			bools[i] =  elementLogic(s.elements.Elem(i), inSeries.elements.Elem(i))
		}
		return Bools(bools)
	}
}