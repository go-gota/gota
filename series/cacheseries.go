package series

import (
	"fmt"
	"unsafe"
)

var _ CacheAbleSeries = cacheAbleSeries{}

type CacheAbleSeries interface {
	Rolling(window int, minPeriods int) RollingSeries
	HasNaN() bool
	IsNaN() []bool
	IsNotNaN() []bool
	Compare(comparator Comparator, comparando interface{}) Series
	Float() []float64
	Order(reverse bool) []int
	StdDev() float64
	Mean() float64
	Median() float64
	Max() float64
	MaxStr() string
	Min() float64
	MinStr() string
	Quantile(p float64) float64
	Map(f MapFunction) Series
	Shift(periods int) Series
	CumProd() Series
	Prod() float64
	AddConst(c float64) Series
	MulConst(c float64) Series
	DivConst(c float64) Series
	Add(c Series) Series
	Sub(c Series) Series
	Mul(c Series) Series
	Div(c Series) Series
	Abs() Series
	Sum() float64

}

type cacheAbleSeries struct {
	s Series
	cacheKey string
}

func newCacheAbleSeries(s Series) CacheAbleSeries {
	if len(s.Name) == 0 {
		panic("series must have a name")
	}
	if c == nil {
		InitCache(nil)
	}

	ret := &cacheAbleSeries{
		s:        s.Copy(),
		cacheKey: fmt.Sprintf("%s|%d", s.Name, s.Len()),
	}
	return ret
}

func (cs cacheAbleSeries) Rolling(window int, minPeriods int) RollingSeries {
	cr := cacheAbleRollingSeries{
		RollingSeries: NewRollingSeries(window, minPeriods, cs.s),
		cacheKey:      fmt.Sprintf("%s|%d|%d", cs.cacheKey, window, minPeriods),
	}
	return cr
}

func (cs cacheAbleSeries) HasNaN() bool {
	cacheKey := cs.cacheKey + "_HasNaN"
	ret, _ := cacheOrExecuted(cacheKey, func() (interface{}, error) {
		ret := cs.s.HasNaN()
		return ret, nil
	})
	return ret.(bool)
}

func cacheOrExecuted(cacheKey string, f func() (interface{}, error)) (interface{}, error) {
	if ret, found := c.Get(cacheKey); found {
		return ret, nil
	}
	ret, err := f()
	if err != nil {
		c.Set(cacheKey, ret)
	}
	return ret, err
}

func (cs cacheAbleSeries) IsNaN() []bool {
	cacheKey := cs.cacheKey + "_IsNaN"
	ret, _ := cacheOrExecuted(cacheKey, func() (interface{}, error) {
		ret := cs.s.IsNaN()
		return ret, nil
	})
	return ret.([]bool)
}

func (cs cacheAbleSeries) IsNotNaN() []bool {
	cacheKey := cs.cacheKey + "_IsNotNaN"
	ret, _ := cacheOrExecuted(cacheKey, func() (interface{}, error) {
		ret := cs.s.IsNotNaN()
		return ret, nil
	})
	return ret.([]bool)
}

func (cs cacheAbleSeries) Compare(comparator Comparator, comparando interface{}) Series {
	var cacheKey string
	if comparator == CompFunc {
		f, ok := comparando.(compFunc)
		if !ok {
			panic("comparando is not a comparison function of type func(el Element) bool")
		}
		cacheKey = fmt.Sprintf("%s_Compare(%s, %v)", cs.cacheKey, comparator, (*(*int64)(unsafe.Pointer(&f))))
	} else {
		return cs.s.Compare(comparator, comparando)
	}
	
	ret, _ := cacheOrExecuted(cacheKey, func() (interface{}, error) {
		ret := cs.s.Compare(comparator, comparando)
		return ret, nil
	})
	return ret.(Series)
}

func (cs cacheAbleSeries) Float() []float64 {
	cacheKey := cs.cacheKey + "_Float"
	ret, _ := cacheOrExecuted(cacheKey, func() (interface{}, error) {
		ret := cs.s.Float()
		return ret, nil
	})
	return ret.([]float64)
}

func (cs cacheAbleSeries) Order(reverse bool) []int {
	cacheKey := fmt.Sprintf("%s_Order(%v)", cs.cacheKey, reverse)
	ret, _ := cacheOrExecuted(cacheKey, func() (interface{}, error) {
		ret := cs.s.Order(reverse)
		return ret, nil
	})
	return ret.([]int)
}

func (cs cacheAbleSeries) StdDev() float64 {
	cacheKey := cs.cacheKey + "_StdDev"
	ret, _ := cacheOrExecuted(cacheKey, func() (interface{}, error) {
		ret := cs.s.StdDev()
		return ret, nil
	})
	return ret.(float64)
}

func (cs cacheAbleSeries) Mean() float64 {
	cacheKey := cs.cacheKey + "_Mean"
	ret, _ := cacheOrExecuted(cacheKey, func() (interface{}, error) {
		ret := cs.s.Mean()
		return ret, nil
	})
	return ret.(float64)
}

func (cs cacheAbleSeries) Median() float64 {
	cacheKey := cs.cacheKey + "_Median"
	ret, _ := cacheOrExecuted(cacheKey, func() (interface{}, error) {
		ret := cs.s.Median()
		return ret, nil
	})
	return ret.(float64)
}

func (cs cacheAbleSeries) Max() float64 {
	cacheKey := cs.cacheKey + "_Max"
	ret, _ := cacheOrExecuted(cacheKey, func() (interface{}, error) {
		ret := cs.s.Max()
		return ret, nil
	})
	return ret.(float64)
}

func (cs cacheAbleSeries) MaxStr() string {
	cacheKey := cs.cacheKey + "_MaxStr"
	ret, _ := cacheOrExecuted(cacheKey, func() (interface{}, error) {
		ret := cs.s.MaxStr()
		return ret, nil
	})
	return ret.(string)
}

func (cs cacheAbleSeries) Min() float64 {
	cacheKey := cs.cacheKey + "_Min"
	ret, _ := cacheOrExecuted(cacheKey, func() (interface{}, error) {
		ret := cs.s.Min()
		return ret, nil
	})
	return ret.(float64)
}

func (cs cacheAbleSeries) MinStr() string {
	cacheKey := cs.cacheKey + "_MinStr"
	ret, _ := cacheOrExecuted(cacheKey, func() (interface{}, error) {
		ret := cs.s.MinStr()
		return ret, nil
	})
	return ret.(string)
}

func (cs cacheAbleSeries) Quantile(p float64) float64 {
	cacheKey := fmt.Sprintf("%s_Quantile(%f)", cs.cacheKey, p)
	ret, _ := cacheOrExecuted(cacheKey, func() (interface{}, error) {
		ret := cs.s.Quantile(p)
		return ret, nil
	})
	return ret.(float64)
}

func (cs cacheAbleSeries) Map(f MapFunction) Series {
	cacheKey := fmt.Sprintf("%s_Map(%v)", cs.cacheKey, (*(*int64)(unsafe.Pointer(&f))))
	ret, _ := cacheOrExecuted(cacheKey, func() (interface{}, error) {
		ret := cs.s.Map(f)
		return ret, nil
	})
	return ret.(Series)
}

func (cs cacheAbleSeries) Shift(periods int) Series {
	cacheKey := fmt.Sprintf("%s_Shift(%d)", cs.cacheKey, periods)
	ret, _ := cacheOrExecuted(cacheKey, func() (interface{}, error) {
		ret := cs.s.Shift(periods)
		return ret, nil
	})
	return ret.(Series)
}

func (cs cacheAbleSeries) CumProd() Series {
	cacheKey := cs.cacheKey + "_CumProd"
	ret, _ := cacheOrExecuted(cacheKey, func() (interface{}, error) {
		ret := cs.s.CumProd()
		return ret, nil
	})
	return ret.(Series)
}

func (cs cacheAbleSeries) Prod() float64 {
	cacheKey := cs.cacheKey + "_Prod"
	ret, _ := cacheOrExecuted(cacheKey, func() (interface{}, error) {
		ret := cs.s.Prod()
		return ret, nil
	})
	return ret.(float64)
}

func (cs cacheAbleSeries) AddConst(c float64) Series {
	cacheKey := fmt.Sprintf("%s_AddConst(%f)", cs.cacheKey, c)
	ret, _ := cacheOrExecuted(cacheKey, func() (interface{}, error) {
		ret := cs.s.AddConst(c)
		return ret, nil
	})
	return ret.(Series)
}

func (cs cacheAbleSeries) MulConst(c float64) Series {
	cacheKey := fmt.Sprintf("%s_MulConst(%f)", cs.cacheKey, c)
	ret, _ := cacheOrExecuted(cacheKey, func() (interface{}, error) {
		ret := cs.s.MulConst(c)
		return ret, nil
	})
	return ret.(Series)
}

func (cs cacheAbleSeries) DivConst(c float64) Series {
	cacheKey := fmt.Sprintf("%s_DivConst(%f)", cs.cacheKey, c)
	ret, _ := cacheOrExecuted(cacheKey, func() (interface{}, error) {
		ret := cs.s.DivConst(c)
		return ret, nil
	})
	return ret.(Series)
}

func (cs cacheAbleSeries) Add(c Series) Series {
	if len(c.Name) == 0 {
		panic("series c must have a name")
	}
	cacheKey := fmt.Sprintf("%s_Add(%s|%d)", cs.cacheKey, c.Name, c.Len())
	ret, _ := cacheOrExecuted(cacheKey, func() (interface{}, error) {
		ret := cs.s.Add(c)
		return ret, nil
	})
	return ret.(Series)
}

func (cs cacheAbleSeries) Sub(c Series) Series {
	if len(c.Name) == 0 {
		panic("series c must have a name")
	}
	cacheKey := fmt.Sprintf("%s_Sub(%s|%d)", cs.cacheKey, c.Name, c.Len())
	ret, _ := cacheOrExecuted(cacheKey, func() (interface{}, error) {
		ret := cs.s.Sub(c)
		return ret, nil
	})
	return ret.(Series)
}

func (cs cacheAbleSeries) Mul(c Series) Series {
	if len(c.Name) == 0 {
		panic("series c must have a name")
	}
	cacheKey := fmt.Sprintf("%s_Mul(%s|%d)", cs.cacheKey, c.Name, c.Len())
	ret, _ := cacheOrExecuted(cacheKey, func() (interface{}, error) {
		ret := cs.s.Mul(c)
		return ret, nil
	})
	return ret.(Series)
}

func (cs cacheAbleSeries) Div(c Series) Series {
	if len(c.Name) == 0 {
		panic("series c must have a name")
	}
	cacheKey := fmt.Sprintf("%s_Div(%s|%d)", cs.cacheKey, c.Name, c.Len())
	ret, _ := cacheOrExecuted(cacheKey, func() (interface{}, error) {
		ret := cs.s.Div(c)
		return ret, nil
	})
	return ret.(Series)
}

func (cs cacheAbleSeries) Abs() Series {
	cacheKey := cs.cacheKey + "_Abs"
	ret, _ := cacheOrExecuted(cacheKey, func() (interface{}, error) {
		ret := cs.s.Abs()
		return ret, nil
	})
	return ret.(Series)
}

func (cs cacheAbleSeries) Sum() float64 {
	cacheKey := cs.cacheKey + "_Sum"
	ret, _ := cacheOrExecuted(cacheKey, func() (interface{}, error) {
		ret := cs.s.Sum()
		return ret, nil
	})
	return ret.(float64)
}

