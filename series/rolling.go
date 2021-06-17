package series

import (
	"fmt"
	"math"

	"github.com/go-gota/gota/util"
	"gonum.org/v1/gonum/floats"
)

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
	var index int
	for index = 0; index < s.minPeriods-1; index++ {
		eles[index] = s.Elem(0).NA()
	}
	frw := NewRollingWindow(s.Series, s.window, s.minPeriods)
	for frw.HasNext() {
		ele := s.Elem(0).NA()
		ele.Set(frw.Next().Max())
		eles[index] = ele 
		index++
	}
	newS := New(eles, s.Type(), fmt.Sprintf("%s-Max(w: %d)", s.Name, s.window))
	return newS
}


func (s rollingSeries) Min() Series {
	if s.Len() == 0 {
		return s.Empty()
	}
	eles := make([]Element, s.Len())
	var index int
	for index = 0; index < s.minPeriods-1; index++ {
		eles[index] = s.Elem(0).NA()
	}
	frw := NewRollingWindow(s.Series, s.window, s.minPeriods)
	for frw.HasNext() {
		ele := s.Elem(0).NA()
		ele.Set(frw.Next().Min())
		eles[index] = ele 
		index++
	}
	newS := New(eles, s.Type(), fmt.Sprintf("%s-Min(w: %d)", s.Name, s.window))
	return newS
}


func (s rollingSeries) Mean() Series {
	if s.Len() == 0 {
		return s.Empty()
	}
	sf := s.Float()
	sum := make([]float64, s.Len())
	floats.CumSum(sum, sf)
	
	eles := make([]float64, s.Len())
	for i := 0; i < s.minPeriods-1; i++ {
		eles[i] = math.NaN()
	}

	// sum0 / sfIndex0
	sum0 := sum[s.minPeriods-1 : s.window - 1]
	sfIndex0 := util.MakeFloatSliceRange(s.window - s.minPeriods, float64(s.minPeriods), 1)
	floats.DivTo(eles[s.minPeriods-1 : s.window - 1], sum0, sfIndex0)

	sum1 := sum[0 : s.Len() - s.window + 1]
	sum2 := sum[s.window - 1 :]
	sf1 := sf[0 : s.Len() - s.window + 1]

	// (sum2 - sum1 + sf1) / window
	windows := util.MakeFloatSlice(s.Len() - s.window + 1, float64(s.window))
	floats.SubTo(eles[s.window - 1 : ], sum2, sum1)
	floats.Add(eles[s.window - 1 : ], sf1)
	floats.Div(eles[s.window - 1 : ], windows)
	newS := New(eles, Float, 
		fmt.Sprintf("%s-Mean(w: %d, p:%d)", s.Name, s.window, s.minPeriods))
	return newS
}


func (s rollingSeries) Quantile(p float64) Series {
	if s.Len() == 0 {
		return s.Empty()
	}
	eles := make([]Element, s.Len())
	var index int
	for index = 0; index < s.minPeriods-1; index++ {
		eles[index] = s.Elem(0).NA()
	}
	frw := NewRollingWindow(s.Series, s.window, s.minPeriods)
	for frw.HasNext() {
		ele := s.Elem(0).NA()
		ele.Set(frw.Next().Quantile(p))
		eles[index] = ele 
		index++
	}
	newS := New(eles, s.Type(), 
	fmt.Sprintf("%s-Quantile(w: %d, p:%f)", s.Name, s.window, p))
	return newS
}

func (s rollingSeries) Median() Series {

	if s.Len() == 0 {
		return s.Empty()
	}
	eles := make([]Element, s.Len())
	var index int
	for index = 0; index < s.minPeriods-1; index++ {
		eles[index] = s.Elem(0).NA()
	}
	frw := NewRollingWindow(s.Series, s.window, s.minPeriods)
	for frw.HasNext() {
		ele := s.Elem(0).NA()
		ele.Set(frw.Next().Median())
		eles[index] = ele 
		index++
	}
	newS := New(eles, s.Type(), 
	fmt.Sprintf("%s-Median(w: %d)", s.Name, s.window))
	return newS
}


func (s rollingSeries) StdDev() Series {
	if s.Len() == 0 {
		return s.Empty()
	}
	eles := make([]Element, s.Len())
	var index int
	for index = 0; index < s.minPeriods-1; index++ {
		eles[index] = &floatElement{0.0, true}
	}
	frw := NewRollingWindow(s.Series, s.window, s.minPeriods)
	for frw.HasNext() {
		ele := &floatElement{0.0, false}
		ele.Set(frw.Next().StdDev())
		eles[index] = ele 
		index++
	}
	newS := New(eles, Float,
		fmt.Sprintf("%s-StdDev(w: %d)", s.Name, s.window))
	return newS
}





