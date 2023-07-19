package series

import (
	"fmt"

	"gonum.org/v1/gonum/floats"
)

//RollingSeries defines methods of a rolling series
type RollingSeries interface {
	// Max return the biggest element in the rolling series
	Max() Series
	// Min return the lowest element in the rolling series
	Min() Series
	// Mean calculates the average value of the rolling series
	Mean() Series
	// Mean calculates the weighted average value of the rolling series
	MeanByWeights(weights []float64) Series
	// Quantile returns the quantile of the window of the rolling series.
	Quantile(p float64) Series
	// Quantiles can be computed in batches.
	Quantiles(ps ...float64) []Series
	// QuantileRolling scrolls to calculate the quantile in the rolling series.
	// the p's element corresponds to the window of the rolling series one by one.
	QuantileRolling(p Series) Series
	// DataQuantile scrolls to calculate the current data's quantile in the rolling series.
	// the data's element corresponds to the window of the rolling series one by one.
	DataQuantileRolling(data Series) Series
	// Median calculates the middle or median value of the rolling series
	Median() Series
	// StdDev calculates the standard deviation of the rolling series
	StdDev() Series
	// Apply applies a function for the rolling series
	Apply(f func(window Series, windowIndex int) interface{}, t Type) Series
	//Iterate iterates the rolling series, the window series is nil when minPeriods is less than the window size
	Iterate(f func(window Series, windowIndex int))
}

type rollingSeries struct {
	Series
	window     int
	minPeriods int
}

//RollingWindow define rolling window
type RollingWindow interface {
	HasNext() bool
	NextWindow() Series
}

type rollingWindow struct {
	startIndex      int
	endIndexExclude int
	windowSize      int
	s               Series
}

func NewRollingWindow(s Series, windowSize int) RollingWindow {
	return &rollingWindow{
		startIndex:      0,
		endIndexExclude: 1,
		windowSize:      windowSize,
		s:               s,
	}
}

func (rw *rollingWindow) HasNext() bool {
	return rw.endIndexExclude <= rw.s.Len()
}

func (rw *rollingWindow) NextWindow() Series {
	window := rw.s.Slice(rw.startIndex, rw.endIndexExclude)
	rw.endIndexExclude++
	startIndex := rw.endIndexExclude - rw.windowSize
	if startIndex > rw.startIndex {
		rw.startIndex = startIndex
	}
	return window
}

//newRollingSeries establish a rolling Series
func newRollingSeries(window int, minPeriods int, s Series) RollingSeries {
	if window < 1 {
		panic("window must >= 1")
	}
	if minPeriods < 1 || minPeriods > window {
		panic("minPeriods must >= 1 && minPeriods must <= window")
	}
	return rollingSeries{
		Series:     s.Copy().Immutable(),
		window:     window,
		minPeriods: minPeriods,
	}
}

func (s rollingSeries) Max() Series {

	var maxFunc func(window Series, windowIndex int) interface{}
	if s.Type() == String {
		maxFunc = func(window Series, windowIndex int) interface{} {
			return window.MaxStr()
		}
	} else {
		maxFunc = func(window Series, windowIndex int) interface{} {
			return window.Max()
		}
	}

	newS := s.Apply(maxFunc, "")
	newS.SetName(fmt.Sprintf("%s_RMax[w:%d]", s.Name(), s.window))
	return newS
}

func (s rollingSeries) Min() Series {
	var minFunc func(window Series, windowIndex int) interface{}
	if s.Type() == String {
		minFunc = func(window Series, windowIndex int) interface{} {
			return window.MinStr()
		}
	} else {
		minFunc = func(window Series, windowIndex int) interface{} {
			return window.Min()
		}
	}

	newS := s.Apply(minFunc, "")
	newS.SetName(fmt.Sprintf("%s_RMin[w:%d]", s.Name(), s.window))
	return newS
}

func (s rollingSeries) Mean() Series {
	newS := s.Apply(func(window Series, windowIndex int) interface{} {
		return window.Mean()
	}, Float)
	newS.SetName(fmt.Sprintf("%s_RMean[w:%d]", s.Name(), s.window))
	return newS
}

func (s rollingSeries) MeanByWeights(weights []float64) Series {
	if s.window != len(weights) {
		panic("window must be equal to weights length")
	}
	weightSum := floats.Sum(weights)
	weightLen := len(weights)
	newS := s.Apply(
		func(window Series, windowIndex int) interface{} {
			weightsUse := weights
			weightSumUse := weightSum
			wfL := window.Len()
			if wfL < weightLen {
				weightsUse = weights[weightLen-wfL:]
				weightSumUse = floats.Sum(weightsUse)
			}
			totalSum := 0.0
			windowFloats := window.Float()
			for i := 0; i < wfL; i++ {
				totalSum += weightsUse[i] * windowFloats[i]
			}
			return totalSum / weightSumUse
		}, Float)
	newS.SetName(fmt.Sprintf("%s_RMeanByWeights[w:%d,%v]", s.Name(), s.window, weights))
	return newS
}

func (s rollingSeries) Quantile(p float64) Series {
	newS := s.Apply(func(window Series, windowIndex int) interface{} {
		return window.Quantile(p)
	}, Float)
	newS.SetName(fmt.Sprintf("%s_RQuantile[w:%d,p:%f]", s.Name(), s.window, p))
	return newS
}

func (s rollingSeries) Quantiles(ps ...float64) []Series {
	ret := make([]Series, len(ps))

	for i := 0; i < len(ps); i++ {
		ret[i] = &series{
			name:     fmt.Sprintf("%s_RQuantile[w:%d,p:%f]", s.Name(), s.window, ps[i]),
			elements: Float.emptyElements(s.Len()),
			t:        Float,
			err:      nil,
		}
	}

	s.Iterate(func(window Series, windowIndex int) {
		if window == nil {
			for i := 0; i < len(ps); i++ {
				ret[i].Elem(windowIndex).SetString(NaN)
			}
		} else {
			qs := window.Quantiles(ps...)
			for i := 0; i < len(ps); i++ {
				ret[i].Elem(windowIndex).SetFloat(qs[i])
			}
		}
	})
	return ret
}

func (s rollingSeries) QuantileRolling(p Series) Series {
	newS := s.Apply(func(window Series, windowIndex int) interface{} {
		ele := p.Elem(windowIndex)
		if ele.IsNA() {
			return NaN
		}
		thisP := ele.Float()
		return window.Quantile(thisP)
	}, Float)
	newS.SetName(fmt.Sprintf("%s_RQuantileRolling[w:%d,p:%s]", s.Name(), s.window, p.Name()))
	return newS
}

func (s rollingSeries) DataQuantileRolling(data Series) Series {
	newS := s.Apply(func(window Series, windowIndex int) interface{} {
		ele := data.Elem(windowIndex)
		if ele.IsNA() {
			return NaN
		}
		thisData := ele.Float()
		return window.DataQuantile(thisData)
	}, Float)
	newS.SetName(fmt.Sprintf("%s_RDataQuantileRolling[w:%d,d:%s]", s.Name(), s.window, data.Name()))
	return newS
}

func (s rollingSeries) Median() Series {
	newS := s.Apply(func(window Series, windowIndex int) interface{} {
		return window.Median()
	}, Float)
	newS.SetName(fmt.Sprintf("%s_RMedian[w:%d]", s.Name(), s.window))
	return newS
}

func (s rollingSeries) StdDev() Series {
	newS := s.Apply(func(window Series, windowIndex int) interface{} {
		return window.StdDev()
	}, Float)
	newS.SetName(fmt.Sprintf("%s_RStdDev[w:%d]", s.Name(), s.window))
	return newS
}

func (s rollingSeries) Apply(f func(window Series, windowIndex int) interface{}, t Type) Series {
	if s.Len() == 0 {
		return s.Empty()
	}
	if len(t) == 0 {
		t = s.Type()
	}
	eles := t.emptyElements(s.Len())
	index := 0
	rw := NewRollingWindow(s.Series, s.window)
	for rw.HasNext() {
		window := rw.NextWindow()
		if window.Len() >= s.minPeriods {
			eles.Elem(index).Set(f(window, index))
		} else {
			eles.Elem(index).Set(NaN)
		}
		index++
	}
	newS := &series{
		name:     fmt.Sprintf("%s_RApply[w:%d]", s.Name(), s.window),
		elements: eles,
		t:        t,
		err:      nil,
	}
	return newS
}

func (s rollingSeries) Iterate(f func(window Series, windowIndex int)) {
	index := 0
	rw := NewRollingWindow(s.Series, s.window)
	for rw.HasNext() {
		window := rw.NextWindow()
		if window.Len() >= s.minPeriods {
			f(window, index)
		} else {
			f(nil, index)
		}
		index++
	}
}
