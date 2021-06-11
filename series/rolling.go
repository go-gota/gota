package series

type Rolling interface {
	Max() Series
	Min() Series
	Mean() Series
	Quantile(p float64) Series
	Median() Series
	StdDev() Series
}

type rollingSeries struct {
	Series
	window     int
	minPeriods int
}

func NewRollingSeries(window int, minPeriods int, s Series) Rolling {
	if window < 1 {
		panic("window must >= 1")
	}
	if minPeriods < 1 || minPeriods > window {
		panic("minPeriods must >= 1 && minPeriods must <= window")
	}
	return &rollingSeries{
		Series:     s,
		window:     window,
		minPeriods: minPeriods,
	}
}

func (s rollingSeries) Max() Series {
	if s.Len() == 0 {
		return s.Empty()
	}
	eles := make([]Element, s.Len())
	for i := 0; i < s.minPeriods-1; i++ {
		eles[i] = s.Elem(0).NA()
	}
	for i := s.minPeriods-1; i < s.Len(); i++ {
		eles[i] = findMax(i + 1 - s.minPeriods, s.window, s.Series).Copy()
	}
	newS := New(eles, s.Type(), "")
	return newS
}


func (s rollingSeries) Min() Series {
	if s.Len() == 0 {
		return s.Empty()
	}
	eles := make([]Element, s.Len())
	for i := 0; i < s.minPeriods-1; i++ {
		eles[i] = s.Elem(0).NA()
	}
	for i := s.minPeriods-1; i < s.Len(); i++ {
		eles[i] = findMin(i + 1 - s.minPeriods, s.window, s.Series).Copy()
	}
	newS := New(eles, s.Type(), "")
	return newS
}

// todo
func (s rollingSeries) Mean() Series {
	return s.Series
}
// todo
func (s rollingSeries) Quantile(p float64) Series {
	return s.Series
}
// todo
func (s rollingSeries) Median() Series {
	return s.Series
}
// todo
func (s rollingSeries) StdDev() Series {
	return s.Series
}

func findMax(startIndex, window int, s Series) Element {
	max := s.Elem(startIndex)
	for i := startIndex + 1; i < startIndex + window && i < s.Len(); i++ {
		elem := s.Elem(i)
		if elem.Greater(max) {
			max = elem
		}
	}
	return max
}

func findMin(startIndex, window int, s Series) Element {
	min := s.Elem(startIndex)
	for i := startIndex + 1; i < startIndex + window && i < s.Len(); i++ {
		elem := s.Elem(i)
		if elem.Less(min) {
			min = elem
		}
	}
	return min
}