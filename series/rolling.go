package series

import (
	"fmt"
	"gonum.org/v1/gonum/floats"
)

//RollingSeries define rolling methods
type RollingSeries interface {
	Max() Series
	Min() Series
	Mean() Series
	MeanByWeights(weights []float64) Series
	Quantile(p float64) Series
	Median() Series
	StdDev() Series
	Apply(f func(window Series, windowIndex int) interface{}, t Type) Series
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
	startIndex int
	endIndexExclude   int
	windowSize int
	s Series
}

func NewRollingWindow(s Series, windowSize int) RollingWindow {
	return &rollingWindow{
		startIndex:      0,
		endIndexExclude: 1,
		windowSize:      windowSize,
		s:               s.Copy(),
	}
}

func (rw *rollingWindow) HasNext() bool {
	return rw.endIndexExclude <= rw.s.Len()
}

func (rw *rollingWindow) NextWindow() Series {
	window := Series{
		Name: rw.s.Name,
		t:    rw.s.t,
	}
	window.elements = rw.s.elements.Slice(rw.startIndex, rw.endIndexExclude)
	rw.endIndexExclude++
	startIndex := rw.endIndexExclude - rw.windowSize
	if startIndex > rw.startIndex {
		rw.startIndex = startIndex
	}
	return window
}

//NewRollingSeries establish a rolling series
func NewRollingSeries(window int, minPeriods int, s Series) RollingSeries {
	if window < 1 {
		panic("window must >= 1")
	}
	if minPeriods < 1 || minPeriods > window {
		panic("minPeriods must >= 1 && minPeriods must <= window")
	}
	return rollingSeries{
		Series:     s,
		window:     window,
		minPeriods: minPeriods,
	}
}

// Max return the biggest element in the rollingSeries
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
	newS.Name = fmt.Sprintf("%s_RMax[w:%d]", s.Name, s.window)
	return newS
}

// Min return the lowest element in the rollingSeries
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
	newS.Name = fmt.Sprintf("%s_RMin[w:%d]", s.Name, s.window)
	return newS
}

// Mean calculates the average value of a rollingSeries
func (s rollingSeries) Mean() Series {
	newS := s.Apply(func(window Series, windowIndex int) interface{} {
		return window.Mean()
	}, Float)
	newS.Name = fmt.Sprintf("%s_RMean[w:%d]", s.Name, s.window)
	return newS
}

// MeanByWeights calculates the weighted average value of a rollingSeries
func (s rollingSeries) MeanByWeights(weights []float64) Series {
	if s.window != len(weights) {
		panic("window must be equal to weights length")
	}
	weightSum := floats.Sum(weights)
	weightLen := len(weights)
	ma := s.Apply(
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
	return ma
}

// Quantile calculates the quantile value of a rollingSeries
func (s rollingSeries) Quantile(p float64) Series {
	newS := s.Apply(func(window Series, windowIndex int) interface{} {
		return window.Quantile(p)
	}, Float)
	newS.Name = fmt.Sprintf("%s_RQuantile[w:%d, p:%f]", s.Name, s.window, p)
	return newS
}

// Median calculates the median value of a rollingSeries
func (s rollingSeries) Median() Series {
	newS := s.Apply(func(window Series, windowIndex int) interface{} {
		return window.Median()
	}, Float)
	newS.Name = fmt.Sprintf("%s_RMedian[w:%d]", s.Name, s.window)
	return newS
}

// StdDev calculates the standard deviation of a rollingSeries
func (s rollingSeries) StdDev() Series {
	newS := s.Apply(func(window Series, windowIndex int) interface{} {
		return window.StdDev()
	}, Float)
	newS.Name = fmt.Sprintf("%s_RStdDev[w:%d]", s.Name, s.window)
	return newS
}

// Apply for extend the computation
func (s rollingSeries) Apply(f func(window Series, windowIndex int) interface{}, t Type) Series {
	if s.Len() == 0 {
		return s.Empty()
	}
	if len(t) == 0 {
		t = s.t
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
	newS := Series{
		Name:     fmt.Sprintf("%s_RApply[w:%d]", s.Name, s.window),
		elements: eles,
		t:        t,
		Err:      nil,
	}
	return newS
}