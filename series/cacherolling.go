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
		cacheKey:      fmt.Sprintf("%s(%d)|[w%d,p%d]", s.Name(), s.Len(), window, minPeriods),
	}
	return cr
}

func cacheOrExecuteRolling(cacheKey string, f func() Series) Series {
	if ret, found := c.Get(cacheKey); found {
		return ret.(Series)
	}
	res := f()
	if res == nil {
		return nil
	}
	res.SetName(cacheKey)
	ret := newCacheAbleSeries(res)
	c.Set(cacheKey, ret)
	return ret
}

func (rc cacheAbleRollingSeries) Max() Series {
	cacheKey := rc.cacheKey + "_RMax"
	ret := cacheOrExecuteRolling(cacheKey, func() Series {
		return rc.RollingSeries.Max()
	})
	return ret
}
func (rc cacheAbleRollingSeries) Min() Series {
	cacheKey := rc.cacheKey + "_RMin"
	ret := cacheOrExecuteRolling(cacheKey, func() Series {
		return rc.RollingSeries.Min()
	})
	return ret

}
func (rc cacheAbleRollingSeries) Mean() Series {
	cacheKey := rc.cacheKey + "_RMean"
	ret := cacheOrExecuteRolling(cacheKey, func() Series {
		return rc.RollingSeries.Mean()
	})
	return ret
}
func (rc cacheAbleRollingSeries) MeanByWeights(weights []float64) Series {
	cacheKey := fmt.Sprintf("%s_RMeanByWeights(%v)", rc.cacheKey, weights)
	ret := cacheOrExecuteRolling(cacheKey, func() Series {
		return rc.RollingSeries.MeanByWeights(weights)
	})
	return ret
}
func (rc cacheAbleRollingSeries) Quantile(p float64) Series {
	cacheKey := fmt.Sprintf("%s_RQuantile(%f)", rc.cacheKey, p)
	ret := cacheOrExecuteRolling(cacheKey, func() Series {
		return rc.RollingSeries.Quantile(p)
	})
	return ret
}
func (rc cacheAbleRollingSeries) Median() Series {
	cacheKey := rc.cacheKey + "_RMedian"
	ret := cacheOrExecuteRolling(cacheKey, func() Series {
		return rc.RollingSeries.Median()
	})
	return ret
}
func (rc cacheAbleRollingSeries) StdDev() Series {
	cacheKey := rc.cacheKey + "_RStdDev"
	ret := cacheOrExecuteRolling(cacheKey, func() Series {
		return rc.RollingSeries.StdDev()
	})
	return ret
}
func (rc cacheAbleRollingSeries) Apply(f func(window Series, windowIndex int) interface{}, t Type) Series {
	cacheKey := fmt.Sprintf("%s_RApply(%v, %s)", rc.cacheKey, (*(*int64)(unsafe.Pointer(&f))), t)
	
	ret := cacheOrExecuteRolling(cacheKey, func() Series {
		return rc.RollingSeries.Apply(f, t)
	})
	return ret
}
