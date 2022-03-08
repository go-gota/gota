package series

import (
	"fmt"
	"gonum.org/v1/gonum/floats"
)

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

func (s rollingSeries) Mean() Series {
	newS := s.Apply(func(window Series, windowIndex int) interface{} {
		return window.Mean()
	}, Float)
	newS.Name = fmt.Sprintf("%s_RMean[w:%d]", s.Name, s.window)
	return newS
}

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

func (s rollingSeries) Quantile(p float64) Series {
	newS := s.Apply(func(window Series, windowIndex int) interface{} {
		return window.Quantile(p)
	}, Float)
	newS.Name = fmt.Sprintf("%s_RQuantile[w:%d, p:%f]", s.Name, s.window, p)
	return newS
}

func (s rollingSeries) Median() Series {
	newS := s.Apply(func(window Series, windowIndex int) interface{} {
		return window.Median()
	}, Float)
	newS.Name = fmt.Sprintf("%s_RMedian[w:%d]", s.Name, s.window)
	return newS
}

func (s rollingSeries) StdDev() Series {
	newS := s.Apply(func(window Series, windowIndex int) interface{} {
		return window.StdDev()
	}, Float)
	newS.Name = fmt.Sprintf("%s_RStdDev[w:%d]", s.Name, s.window)
	return newS
}

func (s rollingSeries) Apply(f func(window Series, windowIndex int) interface{}, t Type) Series {
	if s.Len() == 0 {
		return s.Empty()
	}
	if len(t) == 0 {
		t = s.t
	}
	eles := make([]Element, s.Len())
	index := 0
	rw := NewRollingWindow(s.Series, s.window)
	for rw.HasNext() {
		window := rw.NextWindow()
		ele := NaNElementByType(t)
		if window.Len() >= s.minPeriods {
			ele.Set(f(window, index))
		}
		eles[index] = ele
		index++
	}
	newS := New(eles, t, fmt.Sprintf("%s_RApply[w:%d]", s.Name, s.window))
	return newS
}