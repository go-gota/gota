package series

import "math"

type Number float64

func (n Number) Sub(s Series) Series {
	result := s.Map(func(e Element, i int) Element {
		ele := e.Copy()
		v := float64(n) - e.Float()
		ele.Set(v)
		return ele
	})
	return result
}

func (n Number) Div(s Series) Series {
	result := s.Map(func(e Element, i int) Element {
		ele := e.Copy()
		v := float64(n) / e.Float()
		ele.Set(v)
		return ele
	})
	return result
}

func (n Number) Mod(s Series) Series {
	result := s.Map(func(e Element, i int) Element {
		ele := e.Copy()
		v := math.Mod(float64(n), e.Float())
		ele.Set(v)
		return ele
	})
	return result
}
