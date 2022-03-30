package series

import (
	"fmt"
	"unsafe"
)

type cacheAbleRollingSeries struct {
	RollingSeries
	cacheKey string
}
// NewCacheAbleRollingSeries. You should make sure that the Series will not be modified.
func NewCacheAbleRollingSeries(window int, minPeriods int, s Series) RollingSeries {
	if len(s.Name()) == 0 {
		panic("series must have a name")
	}
	if c == nil {
		InitCache(nil)
	}
	cr := cacheAbleRollingSeries{
		RollingSeries: NewRollingSeries(window, minPeriods, s.Copy()),
		cacheKey:   fmt.Sprintf("%s|%d|%d|%d", s.Name(), s.Len(), window, minPeriods),
	}
	return cr
}

func(rc cacheAbleRollingSeries) Max() Series{
	cacheKey := rc.cacheKey + "_max"
	if ret, found := c.Get(cacheKey); found {
		return ret.(Series)
	}
	ret := rc.RollingSeries.Max()
	c.Set(cacheKey, ret)
	return ret
}
func(rc cacheAbleRollingSeries) Min() Series{
	cacheKey := rc.cacheKey + "_min"
	if ret, found := c.Get(cacheKey); found {
		return ret.(Series)
	}
	ret := rc.RollingSeries.Min()
	c.Set(cacheKey, ret)
	return ret

}
func(rc cacheAbleRollingSeries) Mean() Series{
	cacheKey := rc.cacheKey + "_mean"
	if ret, found := c.Get(cacheKey); found {
		return ret.(Series)
	}
	ret := rc.RollingSeries.Mean()
	c.Set(cacheKey, ret)
	return ret
}
func(rc cacheAbleRollingSeries) MeanByWeights(weights []float64) Series{
	cacheKey := fmt.Sprintf("%s_meanByWeights(%v)", rc.cacheKey, weights)
	if ret, found := c.Get(cacheKey); found {
		return ret.(Series)
	}
	ret := rc.RollingSeries.MeanByWeights(weights)
	c.Set(cacheKey, ret)
	return ret
}
func(rc cacheAbleRollingSeries) Quantile(p float64) Series{
	cacheKey := fmt.Sprintf("%s_quantile(%f)", rc.cacheKey, p)
	if ret, found := c.Get(cacheKey); found {
		return ret.(Series)
	}
	ret := rc.RollingSeries.Quantile(p)
	c.Set(cacheKey, ret)
	return ret
}
func(rc cacheAbleRollingSeries) Median() Series{
	cacheKey := rc.cacheKey + "_median"
	if ret, found := c.Get(cacheKey); found {
		return ret.(Series)
	}
	ret := rc.RollingSeries.Median()
	c.Set(cacheKey, ret)
	return ret
}
func(rc cacheAbleRollingSeries) StdDev() Series{
	cacheKey := rc.cacheKey + "_stdDev"
	if ret, found := c.Get(cacheKey); found {
		return ret.(Series)
	}
	ret := rc.RollingSeries.StdDev()
	c.Set(cacheKey, ret)
	return ret
}
func(rc cacheAbleRollingSeries) Apply(f func(window Series, windowIndex int) interface{}, t Type) Series{
	cacheKey := fmt.Sprintf("%s_apply(%v, %s)", rc.cacheKey, (*(*int64)(unsafe.Pointer(&f))), t)
	if ret, found := c.Get(cacheKey); found {
		return ret.(Series)
	}
	ret := rc.RollingSeries.Apply(f, t)
	c.Set(cacheKey, ret)
	return ret
}