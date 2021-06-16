package series

import (
	"math"
	"sort"

	"gonum.org/v1/gonum/stat"
)

type RollingWindow interface {
	HasNext() bool
	Next() Window
}

type Window interface {
	Max() interface{}
	Min() interface{}
	Quantile(p float64) float64
	Median() float64
	StdDev() float64
}

type rollingWindow struct {
	floats []float64
	eles []Element
	startIndex int
	endIndexExclude   int
	windowSize int
	eleType Type
}

func NewRollingWindow(s Series, windowSize int, minPeriods int) RollingWindow {

	eles := make([]Element, s.Len())
	for i := 0; i < s.Len(); i++ {
		eles[i] = s.Elem(i)
	}

	return &rollingWindow{
		floats:     s.Float(),
		eles: eles,
		startIndex: 0,
		endIndexExclude:   minPeriods,
		windowSize: windowSize,
		eleType: s.t,
	}
}

func (rw *rollingWindow) HasNext() bool {
	return rw.endIndexExclude <= len(rw.eles)
}

func (rw *rollingWindow) Next() Window {
	fw := elementsWindow {
		rw.floats[rw.startIndex:rw.endIndexExclude],
		rw.eles[rw.startIndex:rw.endIndexExclude],
	}
	rw.endIndexExclude++
	startIndex := rw.endIndexExclude - rw.windowSize
	if startIndex > rw.startIndex {
		rw.startIndex = startIndex
	}

	return fw
}

type elementsWindow struct {
	floats []float64
	eles 	[]Element
}

func (ew elementsWindow) Max() interface{} {
	return findMax(ew.eles).Val()
}

func (ew elementsWindow) Min() interface{} {
	return findMin(ew.eles).Val()
}

func (ew elementsWindow) Quantile(p float64) float64 {
	fs := make([]float64, len(ew.floats))
	copy(fs, ew.floats)
	sort.Float64s(fs)
	return stat.Quantile(p, stat.Empirical, fs, nil)
}

func (ew elementsWindow) Median() float64 {
	if len(ew.eles) == 0 ||
		ew.eles[0].Type() == String ||
		ew.eles[0].Type() == Bool {
		return math.NaN()
	}

	fs := make([]float64, len(ew.floats))
	copy(fs, ew.floats)
	sort.Float64s(fs)

	if len(ew.floats) %2 != 0 {
		return fs[len(ew.floats)/2]
	}
	return (ew.floats[(len(ew.floats)/2)-1] +
		ew.floats[len(ew.floats)/2]) * 0.5
}

func (ew elementsWindow) StdDev() float64 {
	return stat.StdDev(ew.floats, nil)
}


func findMax(eles []Element) Element {
	max := eles[0]
	for i := 1; i < len(eles); i++ {
		if eles[i].Greater(max) {
			max = eles[i]
		}
	}
	return max
}

func findMin(eles []Element) Element {
	min := eles[0]
	for i := 1; i < len(eles); i++ {
		if eles[i].Less(min) {
			min = eles[i]
		}
	}
	return min
}
