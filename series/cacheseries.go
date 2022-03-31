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
		Series:   s,
		cacheKey: s.Name(),
	}
	return ret
}

func (cs cacheAbleSeries) Rolling(window int, minPeriods int) RollingSeries {
	cr := cacheAbleRollingSeries{
		RollingSeries: NewRollingSeries(window, minPeriods, cs.Series),
		cacheKey:      fmt.Sprintf("%s|[w%d,p%d]", cs.cacheKey, window, minPeriods),
	}
	return cr
}

func (cs cacheAbleSeries) HasNaN() bool {
	cacheKey := cs.cacheKey + "_HasNaN"
	ret, _ := cacheOrExecute(cacheKey, func() (interface{}, error) {
		ret := cs.Series.HasNaN()
		return ret, nil
	})
	return ret.(bool)
}

func cacheOrExecute(cacheKey string, f func() (interface{}, error)) (interface{}, error) {
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
	ret, _ := cacheOrExecute(cacheKey, func() (interface{}, error) {
		ret := cs.Series.IsNaN()
		return ret, nil
	})
	return ret.([]bool)
}

func (cs cacheAbleSeries) IsNotNaN() []bool {
	cacheKey := cs.cacheKey + "_IsNotNaN"
	ret, _ := cacheOrExecute(cacheKey, func() (interface{}, error) {
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
		case Series:
			if len(v.Name()) == 0 {
				panic("series must have a name")
			}
			cacheKey = fmt.Sprintf("%s_Compare(%s, %s|%d)", cs.cacheKey, comparator, v.Name(), v.Len())
		default:
			switch reflect.TypeOf(comparando).Kind() {
			case reflect.Slice:
				return cs.Series.Compare(comparator, comparando)
			default:
				cacheKey = fmt.Sprintf("%s_Compare(%s, %v)", cs.cacheKey, comparator, comparando)
			}
		}
	}

	ret, _ := cacheOrExecute(cacheKey, func() (interface{}, error) {
		ret := cs.Series.Compare(comparator, comparando)
		return ret, nil
	})
	return ret.(Series)
}

func (cs cacheAbleSeries) Float() []float64 {
	cacheKey := cs.cacheKey + "_Float"
	ret, _ := cacheOrExecute(cacheKey, func() (interface{}, error) {
		ret := cs.Series.Float()
		return ret, nil
	})
	return ret.([]float64)
}

func (cs cacheAbleSeries) Order(reverse bool) []int {
	cacheKey := fmt.Sprintf("%s_Order(%v)", cs.cacheKey, reverse)
	ret, _ := cacheOrExecute(cacheKey, func() (interface{}, error) {
		ret := cs.Series.Order(reverse)
		return ret, nil
	})
	return ret.([]int)
}

func (cs cacheAbleSeries) StdDev() float64 {
	cacheKey := cs.cacheKey + "_StdDev"
	ret, _ := cacheOrExecute(cacheKey, func() (interface{}, error) {
		ret := cs.Series.StdDev()
		return ret, nil
	})
	return ret.(float64)
}

func (cs cacheAbleSeries) Mean() float64 {
	cacheKey := cs.cacheKey + "_Mean"
	ret, _ := cacheOrExecute(cacheKey, func() (interface{}, error) {
		ret := cs.Series.Mean()
		return ret, nil
	})
	return ret.(float64)
}

func (cs cacheAbleSeries) Median() float64 {
	cacheKey := cs.cacheKey + "_Median"
	ret, _ := cacheOrExecute(cacheKey, func() (interface{}, error) {
		ret := cs.Series.Median()
		return ret, nil
	})
	return ret.(float64)
}

func (cs cacheAbleSeries) Max() float64 {
	cacheKey := cs.cacheKey + "_Max"
	ret, _ := cacheOrExecute(cacheKey, func() (interface{}, error) {
		ret := cs.Series.Max()
		return ret, nil
	})
	return ret.(float64)
}

func (cs cacheAbleSeries) MaxStr() string {
	cacheKey := cs.cacheKey + "_MaxStr"
	ret, _ := cacheOrExecute(cacheKey, func() (interface{}, error) {
		ret := cs.Series.MaxStr()
		return ret, nil
	})
	return ret.(string)
}

func (cs cacheAbleSeries) Min() float64 {
	cacheKey := cs.cacheKey + "_Min"
	ret, _ := cacheOrExecute(cacheKey, func() (interface{}, error) {
		ret := cs.Series.Min()
		return ret, nil
	})
	return ret.(float64)
}

func (cs cacheAbleSeries) MinStr() string {
	cacheKey := cs.cacheKey + "_MinStr"
	ret, _ := cacheOrExecute(cacheKey, func() (interface{}, error) {
		ret := cs.Series.MinStr()
		return ret, nil
	})
	return ret.(string)
}

func (cs cacheAbleSeries) Quantile(p float64) float64 {
	cacheKey := fmt.Sprintf("%s_Quantile(%f)", cs.cacheKey, p)
	ret, _ := cacheOrExecute(cacheKey, func() (interface{}, error) {
		ret := cs.Series.Quantile(p)
		return ret, nil
	})
	return ret.(float64)
}

func (cs cacheAbleSeries) Map(f MapFunction) Series {
	cacheKey := fmt.Sprintf("%s_Map(%v)", cs.cacheKey, (*(*int64)(unsafe.Pointer(&f))))
	ret, _ := cacheOrExecute(cacheKey, func() (interface{}, error) {
		ret := cs.Series.Map(f)
		return ret, nil
	})
	return ret.(Series)
}

func (cs cacheAbleSeries) Shift(periods int) Series {
	cacheKey := fmt.Sprintf("%s_Shift(%d)", cs.cacheKey, periods)
	ret, _ := cacheOrExecute(cacheKey, func() (interface{}, error) {
		res := cs.Series.Shift(periods)
		res.SetName(cacheKey)
		ret := res.CacheAble()
		return ret, nil
	})
	return ret.(Series)
}

func (cs cacheAbleSeries) CumProd() Series {
	cacheKey := cs.cacheKey + "_CumProd"
	ret, _ := cacheOrExecute(cacheKey, func() (interface{}, error) {
		res := cs.Series.CumProd()
		res.SetName(cacheKey)
		ret := res.CacheAble()
		return ret, nil
	})
	return ret.(Series)
}

func (cs cacheAbleSeries) Prod() float64 {
	cacheKey := cs.cacheKey + "_Prod"
	ret, _ := cacheOrExecute(cacheKey, func() (interface{}, error) {
		ret := cs.Series.Prod()
		return ret, nil
	})
	return ret.(float64)
}

func (cs cacheAbleSeries) AddConst(c float64) Series {
	cacheKey := fmt.Sprintf("%s_AddConst(%f)", cs.cacheKey, c)
	ret, _ := cacheOrExecute(cacheKey, func() (interface{}, error) {
		res := cs.Series.AddConst(c)
		res.SetName(cacheKey)
		ret := res.CacheAble()
		return ret, nil
	})
	return ret.(Series)
}

func (cs cacheAbleSeries) MulConst(c float64) Series {
	cacheKey := fmt.Sprintf("%s_MulConst(%f)", cs.cacheKey, c)
	ret, _ := cacheOrExecute(cacheKey, func() (interface{}, error) {
		res := cs.Series.MulConst(c)
		res.SetName(cacheKey)
		ret := res.CacheAble()
		return ret, nil
	})
	return ret.(Series)
}

func (cs cacheAbleSeries) DivConst(c float64) Series {
	cacheKey := fmt.Sprintf("%s_DivConst(%f)", cs.cacheKey, c)
	ret, _ := cacheOrExecute(cacheKey, func() (interface{}, error) {
		res := cs.Series.DivConst(c)
		res.SetName(cacheKey)
		ret := res.CacheAble()
		return ret, nil
	})
	return ret.(Series)
}

func (cs cacheAbleSeries) Add(c Series) Series {
	if len(c.Name()) == 0 {
		panic("series c must have a name")
	}
	cacheKey := fmt.Sprintf("%s_Add(%s)", cs.cacheKey, c.Name())
	ret, _ := cacheOrExecute(cacheKey, func() (interface{}, error) {
		res := cs.Series.Add(c)
		res.SetName(cacheKey)
		ret := res.CacheAble()
		return ret, nil
	})
	return ret.(Series)
}

func (cs cacheAbleSeries) Sub(c Series) Series {
	if len(c.Name()) == 0 {
		panic("series c must have a name")
	}
	cacheKey := fmt.Sprintf("%s_Sub(%s)", cs.cacheKey, c.Name())
	ret, _ := cacheOrExecute(cacheKey, func() (interface{}, error) {
		res := cs.Series.Sub(c)
		res.SetName(cacheKey)
		ret := res.CacheAble()
		return ret, nil
	})
	return ret.(Series)
}

func (cs cacheAbleSeries) Mul(c Series) Series {
	if len(c.Name()) == 0 {
		panic("series c must have a name")
	}
	cacheKey := fmt.Sprintf("%s_Mul(%s)", cs.cacheKey, c.Name())
	ret, _ := cacheOrExecute(cacheKey, func() (interface{}, error) {
		res := cs.Series.Mul(c)
		res.SetName(cacheKey)
		ret := res.CacheAble()
		return ret, nil
	})
	return ret.(Series)
}

func (cs cacheAbleSeries) Div(c Series) Series {
	if len(c.Name()) == 0 {
		panic("series c must have a name")
	}
	cacheKey := fmt.Sprintf("%s_Div(%s)", cs.cacheKey, c.Name())
	ret, _ := cacheOrExecute(cacheKey, func() (interface{}, error) {
		res := cs.Series.Div(c)
		res.SetName(cacheKey)
		ret := res.CacheAble()
		return ret, nil
	})
	return ret.(Series)
}

func (cs cacheAbleSeries) Abs() Series {
	cacheKey := cs.cacheKey + "_Abs"
	ret, _ := cacheOrExecute(cacheKey, func() (interface{}, error) {
		res := cs.Series.Abs()
		res.SetName(cacheKey)
		ret := res.CacheAble()
		return ret, nil
	})
	return ret.(Series)
}

func (cs cacheAbleSeries) Sum() float64 {
	cacheKey := cs.cacheKey + "_Sum"
	ret, _ := cacheOrExecute(cacheKey, func() (interface{}, error) {
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
	cacheKey := fmt.Sprintf("%s_Concat(%s)", cs.cacheKey, x.Name())
	res := cs.Series.Concat(x)
	res.SetName(cacheKey)
	ret := res.CacheAble()
	return ret
}

func (cs cacheAbleSeries) Copy() Series {
	cacheKey := fmt.Sprintf("%s_Copy{%s}", cs.cacheKey, uuid.NewV4().String())
	res := cs.Series.Copy()
	res.SetName(cacheKey)
	ret := res.CacheAble()
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
	cacheKey := fmt.Sprintf("%s_Slice(%d,%d)", cs.cacheKey, start, end)
	res := cs.Series.Slice(start, end)
	res.SetName(cacheKey)
	ret := res.CacheAble()
	return ret
}

func (cs *cacheAbleSeries) CacheAble() Series {
	return cs
}

func (cs *cacheAbleSeries) Set(indexes Indexes, newvalues Series) Series {
	c.DelByKeyPrefix(cs.cacheKey)
	return cs.Series.Set(indexes, newvalues)
}

func (cs *cacheAbleSeries) FillNaN(value ElementValue) {
	c.DelByKeyPrefix(cs.cacheKey)
	cs.Series.FillNaN(value)
}
func (cs *cacheAbleSeries) FillNaNForward() {
	c.DelByKeyPrefix(cs.cacheKey)
	cs.Series.FillNaNForward()
}
func (cs *cacheAbleSeries) FillNaNBackward() {
	c.DelByKeyPrefix(cs.cacheKey)
	cs.Series.FillNaNBackward()
}

func (cs *cacheAbleSeries) Append(values interface{}) {
	c.DelByKeyPrefix(cs.cacheKey)
	cs.Series.Append(values)
}

func (cs *cacheAbleSeries) And(in interface{}) Series {
	var cacheKey string
	switch v := in.(type) {
	case Series:
		if len(v.Name()) == 0 {
			panic("series must have a name")
		}
		cacheKey = fmt.Sprintf("%s_And(%s)", cs.cacheKey, v.Name())
	default:
		switch reflect.TypeOf(in).Kind() {
		case reflect.Slice:
			res := cs.Series.And(in)
			return res
		default:
			cacheKey = fmt.Sprintf("%s_And(%v)", cs.cacheKey, in)
		}
	}

	ret, _ := cacheOrExecute(cacheKey, func() (interface{}, error) {
		res := cs.Series.And(in)
		res.SetName(cacheKey)
		ret := res.CacheAble()
		return ret, nil
	})
	return ret.(Series)
}

func (cs *cacheAbleSeries) Or(in interface{}) Series {
	var cacheKey string
	switch v := in.(type) {
	case Series:
		if len(v.Name()) == 0 {
			panic("series must have a name")
		}
		cacheKey = fmt.Sprintf("%s_Or(%s)", cs.cacheKey, v.Name())
	default:
		switch reflect.TypeOf(in).Kind() {
		case reflect.Slice:
			res := cs.Series.Or(in)
			return res
		default:
			cacheKey = fmt.Sprintf("%s_Or(%v)", cs.cacheKey, in)
		}
	}

	ret, _ := cacheOrExecute(cacheKey, func() (interface{}, error) {
		res := cs.Series.Or(in)
		res.SetName(cacheKey)
		ret := res.CacheAble()
		return ret, nil
	})
	return ret.(Series)
}
