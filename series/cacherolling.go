package series

import (
	"fmt"
)

type cacheAbleRollingSeries struct {
	RollingSeries
	c Cache
}

func (rc *cacheAbleRollingSeries) cacheOrExecuteRolling(cacheKey string, f func() Series) Series {
	if ret, found := rc.c.Get(cacheKey); found {
		return ret.(Series)
	}
	res := f()
	if res == nil {
		return nil
	}
	res.SetName(cacheKey)
	ret := res.Immutable()
	rc.c.Set(cacheKey, ret)
	return ret
}

func (rc cacheAbleRollingSeries) Max() Series {
	cacheKey := "RMax"
	ret := rc.cacheOrExecuteRolling(cacheKey, func() Series {
		return rc.RollingSeries.Max()
	})
	return ret
}
func (rc cacheAbleRollingSeries) Min() Series {
	cacheKey := "RMin"
	ret := rc.cacheOrExecuteRolling(cacheKey, func() Series {
		return rc.RollingSeries.Min()
	})
	return ret

}
func (rc cacheAbleRollingSeries) Mean() Series {
	cacheKey := "RMean"
	ret := rc.cacheOrExecuteRolling(cacheKey, func() Series {
		return rc.RollingSeries.Mean()
	})
	return ret
}
func (rc cacheAbleRollingSeries) MeanByWeights(weights []float64) Series {
	cacheKey := fmt.Sprintf("RMeanByWeights(%v)", weights)
	ret := rc.cacheOrExecuteRolling(cacheKey, func() Series {
		return rc.RollingSeries.MeanByWeights(weights)
	})
	return ret
}
func (rc cacheAbleRollingSeries) Quantile(p float64) Series {
	cacheKey := fmt.Sprintf("RQuantile(%f)", p)
	ret := rc.cacheOrExecuteRolling(cacheKey, func() Series {
		return rc.RollingSeries.Quantile(p)
	})
	return ret
}
func (rc cacheAbleRollingSeries) Median() Series {
	cacheKey := "RMedian"
	ret := rc.cacheOrExecuteRolling(cacheKey, func() Series {
		return rc.RollingSeries.Median()
	})
	return ret
}
func (rc cacheAbleRollingSeries) StdDev() Series {
	cacheKey := "RStdDev"
	ret := rc.cacheOrExecuteRolling(cacheKey, func() Series {
		return rc.RollingSeries.StdDev()
	})
	return ret
}
