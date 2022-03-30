package series

import (
	"fmt"
	"reflect"
	"unsafe"

	uuid "github.com/satori/go.uuid"
)

var _ Series = (*cacheAbleSeries)(nil)


type cacheAbleSeries struct {
	Series
	cacheKey string
}

func newCacheAbleSeries(s Series) Series {
	if len(s.Name()) == 0 {
		panic("series must have a name")
	}
	if c == nil {
		InitCache(nil)
	}

	ret := &cacheAbleSeries{
		Series:        s,
		cacheKey: fmt.Sprintf("%s|%d", s.Name(), s.Len()),
	}
	return ret
}

func (cs cacheAbleSeries) Rolling(window int, minPeriods int) RollingSeries {
	cr := cacheAbleRollingSeries{
		RollingSeries: NewRollingSeries(window, minPeriods, cs.Series),
		cacheKey:      fmt.Sprintf("%s|%d|%d", cs.cacheKey, window, minPeriods),
	}
	return cr
}

func (cs cacheAbleSeries) HasNaN() bool {
	cacheKey := cs.cacheKey + "_HasNaN"
	ret, _ := cacheOrExecuted(cacheKey, func() (interface{}, error) {
		ret := cs.Series.HasNaN()
		return ret, nil
	})
	return ret.(bool)
}

func cacheOrExecuted(cacheKey string, f func() (interface{}, error)) (interface{}, error) {
	if ret, found := c.Get(cacheKey); found {
		return ret, nil
	}
	ret, err := f()
	if err == nil {
		c.Set(cacheKey, ret)
	}
	return ret, err
}

func (cs cacheAbleSeries) IsNaN() []bool {
	cacheKey := cs.cacheKey + "_IsNaN"
	ret, _ := cacheOrExecuted(cacheKey, func() (interface{}, error) {
		ret := cs.Series.IsNaN()
		return ret, nil
	})
	return ret.([]bool)
}

func (cs cacheAbleSeries) IsNotNaN() []bool {
	cacheKey := cs.cacheKey + "_IsNotNaN"
	ret, _ := cacheOrExecuted(cacheKey, func() (interface{}, error) {
		ret := cs.Series.IsNotNaN()
		return ret, nil
	})
	return ret.([]bool)
}

//Compare. The result will be cached if comparando is 'compFunc', 'Series' or not a slice.
func (cs cacheAbleSeries) Compare(comparator Comparator, comparando interface{}) Series {
	var cacheKey string
	if comparator == CompFunc {
		f, ok := comparando.(compFunc)
		if !ok {
			panic("comparando is not a comparison function of type func(el Element) bool")
		}
		cacheKey = fmt.Sprintf("%s_Compare(%s, %v)", cs.cacheKey, comparator, (*(*int64)(unsafe.Pointer(&f))))
	} else {
		switch v := comparando.(type) {
		case series:
			if len(v.name) == 0 {
				panic("series must have a name")
			}
			cacheKey = fmt.Sprintf("%s_Compare(%s, %s|%d)", cs.cacheKey, comparator, v.name, v.Len())
		default:
			switch reflect.TypeOf(comparando).Kind() {
				case reflect.Slice:
					return cs.Series.Compare(comparator, comparando)
				default:
					cacheKey = fmt.Sprintf("%s_Compare(%s, %v)", cs.cacheKey, comparator, comparando)
			}
		}
	}
	
	ret, _ := cacheOrExecuted(cacheKey, func() (interface{}, error) {
		ret := cs.Series.Compare(comparator, comparando)
		return ret, nil
	})
	return ret.(Series)
}

func (cs cacheAbleSeries) Float() []float64 {
	cacheKey := cs.cacheKey + "_Float"
	ret, _ := cacheOrExecuted(cacheKey, func() (interface{}, error) {
		ret := cs.Series.Float()
		return ret, nil
	})
	return ret.([]float64)
}

func (cs cacheAbleSeries) Order(reverse bool) []int {
	cacheKey := fmt.Sprintf("%s_Order(%v)", cs.cacheKey, reverse)
	ret, _ := cacheOrExecuted(cacheKey, func() (interface{}, error) {
		ret := cs.Series.Order(reverse)
		return ret, nil
	})
	return ret.([]int)
}

func (cs cacheAbleSeries) StdDev() float64 {
	cacheKey := cs.cacheKey + "_StdDev"
	ret, _ := cacheOrExecuted(cacheKey, func() (interface{}, error) {
		ret := cs.Series.StdDev()
		return ret, nil
	})
	return ret.(float64)
}

func (cs cacheAbleSeries) Mean() float64 {
	cacheKey := cs.cacheKey + "_Mean"
	ret, _ := cacheOrExecuted(cacheKey, func() (interface{}, error) {
		ret := cs.Series.Mean()
		return ret, nil
	})
	return ret.(float64)
}

func (cs cacheAbleSeries) Median() float64 {
	cacheKey := cs.cacheKey + "_Median"
	ret, _ := cacheOrExecuted(cacheKey, func() (interface{}, error) {
		ret := cs.Series.Median()
		return ret, nil
	})
	return ret.(float64)
}

func (cs cacheAbleSeries) Max() float64 {
	cacheKey := cs.cacheKey + "_Max"
	ret, _ := cacheOrExecuted(cacheKey, func() (interface{}, error) {
		ret := cs.Series.Max()
		return ret, nil
	})
	return ret.(float64)
}

func (cs cacheAbleSeries) MaxStr() string {
	cacheKey := cs.cacheKey + "_MaxStr"
	ret, _ := cacheOrExecuted(cacheKey, func() (interface{}, error) {
		ret := cs.Series.MaxStr()
		return ret, nil
	})
	return ret.(string)
}

func (cs cacheAbleSeries) Min() float64 {
	cacheKey := cs.cacheKey + "_Min"
	ret, _ := cacheOrExecuted(cacheKey, func() (interface{}, error) {
		ret := cs.Series.Min()
		return ret, nil
	})
	return ret.(float64)
}

func (cs cacheAbleSeries) MinStr() string {
	cacheKey := cs.cacheKey + "_MinStr"
	ret, _ := cacheOrExecuted(cacheKey, func() (interface{}, error) {
		ret := cs.Series.MinStr()
		return ret, nil
	})
	return ret.(string)
}

func (cs cacheAbleSeries) Quantile(p float64) float64 {
	cacheKey := fmt.Sprintf("%s_Quantile(%f)", cs.cacheKey, p)
	ret, _ := cacheOrExecuted(cacheKey, func() (interface{}, error) {
		ret := cs.Series.Quantile(p)
		return ret, nil
	})
	return ret.(float64)
}

func (cs cacheAbleSeries) Map(f MapFunction) Series {
	cacheKey := fmt.Sprintf("%s_Map(%v)", cs.cacheKey, (*(*int64)(unsafe.Pointer(&f))))
	ret, _ := cacheOrExecuted(cacheKey, func() (interface{}, error) {
		ret := cs.Series.Map(f)
		return ret, nil
	})
	return ret.(Series)
}

func (cs cacheAbleSeries) Shift(periods int) Series {
	cacheKey := fmt.Sprintf("%s_Shift(%d)", cs.cacheKey, periods)
	ret, _ := cacheOrExecuted(cacheKey, func() (interface{}, error) {
		res := cs.Series.Shift(periods)
		res.SetName(cacheKey)
		ret := newCacheAbleSeries(res)
		return ret, nil
	})
	return ret.(Series)
}

func (cs cacheAbleSeries) CumProd() Series {
	cacheKey := cs.cacheKey + "_CumProd"
	ret, _ := cacheOrExecuted(cacheKey, func() (interface{}, error) {
		res := cs.Series.CumProd()
		res.SetName(cacheKey)
		ret := newCacheAbleSeries(res)
		return ret, nil
	})
	return ret.(Series)
}

func (cs cacheAbleSeries) Prod() float64 {
	cacheKey := cs.cacheKey + "_Prod"
	ret, _ := cacheOrExecuted(cacheKey, func() (interface{}, error) {
		ret := cs.Series.Prod()
		return ret, nil
	})
	return ret.(float64)
}

func (cs cacheAbleSeries) AddConst(c float64) Series {
	cacheKey := fmt.Sprintf("%s_AddConst(%f)", cs.cacheKey, c)
	ret, _ := cacheOrExecuted(cacheKey, func() (interface{}, error) {
		res := cs.Series.AddConst(c)
		res.SetName(cacheKey)
		ret := newCacheAbleSeries(res)
		return ret, nil
	})
	return ret.(Series)
}

func (cs cacheAbleSeries) MulConst(c float64) Series {
	cacheKey := fmt.Sprintf("%s_MulConst(%f)", cs.cacheKey, c)
	ret, _ := cacheOrExecuted(cacheKey, func() (interface{}, error) {
		res := cs.Series.MulConst(c)
		res.SetName(cacheKey)
		ret := newCacheAbleSeries(res)
		return ret, nil
	})
	return ret.(Series)
}

func (cs cacheAbleSeries) DivConst(c float64) Series {
	cacheKey := fmt.Sprintf("%s_DivConst(%f)", cs.cacheKey, c)
	ret, _ := cacheOrExecuted(cacheKey, func() (interface{}, error) {
		res := cs.Series.DivConst(c)
		res.SetName(cacheKey)
		ret := newCacheAbleSeries(res)
		return ret, nil
	})
	return ret.(Series)
}

func (cs cacheAbleSeries) Add(c Series) Series {
	if len(c.Name()) == 0 {
		panic("series c must have a name")
	}
	cacheKey := fmt.Sprintf("%s_Add(%s|%d)", cs.cacheKey, c.Name(), c.Len())
	ret, _ := cacheOrExecuted(cacheKey, func() (interface{}, error) {
		res := cs.Series.Add(c)
		res.SetName(cacheKey)
		ret := newCacheAbleSeries(res)
		return ret, nil
	})
	return ret.(Series)
}

func (cs cacheAbleSeries) Sub(c Series) Series {
	if len(c.Name()) == 0 {
		panic("series c must have a name")
	}
	cacheKey := fmt.Sprintf("%s_Sub(%s|%d)", cs.cacheKey, c.Name(), c.Len())
	ret, _ := cacheOrExecuted(cacheKey, func() (interface{}, error) {
		res := cs.Series.Sub(c)
		res.SetName(cacheKey)
		ret := newCacheAbleSeries(res)
		return ret, nil
	})
	return ret.(Series)
}

func (cs cacheAbleSeries) Mul(c Series) Series {
	if len(c.Name()) == 0 {
		panic("series c must have a name")
	}
	cacheKey := fmt.Sprintf("%s_Mul(%s|%d)", cs.cacheKey, c.Name(), c.Len())
	ret, _ := cacheOrExecuted(cacheKey, func() (interface{}, error) {
		res := cs.Series.Mul(c)
		res.SetName(cacheKey)
		ret := newCacheAbleSeries(res)
		return ret, nil
	})
	return ret.(Series)
}

func (cs cacheAbleSeries) Div(c Series) Series {
	if len(c.Name()) == 0 {
		panic("series c must have a name")
	}
	cacheKey := fmt.Sprintf("%s_Div(%s|%d)", cs.cacheKey, c.Name(), c.Len())
	ret, _ := cacheOrExecuted(cacheKey, func() (interface{}, error) {
		res := cs.Series.Div(c)
		res.SetName(cacheKey)
		ret := newCacheAbleSeries(res)
		return ret, nil
	})
	return ret.(Series)
}

func (cs cacheAbleSeries) Abs() Series {
	cacheKey := cs.cacheKey + "_Abs"
	ret, _ := cacheOrExecuted(cacheKey, func() (interface{}, error) {
		res := cs.Series.Abs()
		res.SetName(cacheKey)
		ret := newCacheAbleSeries(res)
		return ret, nil
	})
	return ret.(Series)
}

func (cs cacheAbleSeries) Sum() float64 {
	cacheKey := cs.cacheKey + "_Sum"
	ret, _ := cacheOrExecuted(cacheKey, func() (interface{}, error) {
		ret := cs.Series.Sum()
		return ret, nil
	})
	return ret.(float64)
}

func (cs cacheAbleSeries) Empty() Series {
	return cs.Series.Empty()
}

func (cs cacheAbleSeries) Error() error {
	return cs.Series.Error()
}

func (cs cacheAbleSeries) Subset(indexes Indexes) Series {
	return cs.Series.Subset(indexes)
}

func (cs cacheAbleSeries) Concat(x Series) Series {
	if len(x.Name()) == 0 {
		panic("series x must have a name")
	}
	cacheKey := fmt.Sprintf("%s_Concat(%s|%d)", cs.cacheKey, x.Name(), x.Len())
	res := cs.Series.Concat(x)
	res.SetName(cacheKey)
	ret := newCacheAbleSeries(res)
	return ret
}

func (cs cacheAbleSeries) Copy() Series {
	cacheKey := fmt.Sprintf("%s_Copy{%s}", cs.cacheKey, uuid.NewV4().String())
	res := cs.Series.Copy()
	res.SetName(cacheKey)
	ret := newCacheAbleSeries(res)
	return ret
}

func (cs cacheAbleSeries) Records() []string {
	return cs.Series.Records()
}

func (cs cacheAbleSeries) Type() Type {
	return cs.Series.Type()
}

func (cs cacheAbleSeries) Len() int {
	return cs.Series.Len()
}

func (cs cacheAbleSeries) String() string {
	return cs.Series.String()
}

func (cs cacheAbleSeries) Str() string {
	return cs.Series.Str()
}

func (cs cacheAbleSeries) Val(i int) interface{} {
	return cs.Series.Val(i)
}

func (cs cacheAbleSeries) Elem(i int) Element {
	return cs.Series.Elem(i)
}

func (cs cacheAbleSeries) Slice(start int, end int) Series {
	return cs.Series.Slice(start, end)
}

