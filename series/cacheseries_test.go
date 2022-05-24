package series

import (
	"reflect"
	"sync"
	"testing"
)

type mockCache struct {
	setCount   int
	getCount   int
	hitCount   int
	innerCache Cache
}

func (mc *mockCache) Set(k string, v interface{}) {
	mc.innerCache.Set(k, v)
	mc.setCount++
}

func (mc *mockCache) Get(k string) (interface{}, bool) {
	mc.getCount++
	v, ok := mc.innerCache.Get(k)
	if ok {
		mc.hitCount++
	}
	return v, ok
}

func (mc *mockCache) Clear() {
	mc.innerCache.Clear()
	mc.setCount = 0
	mc.getCount = 0
	mc.hitCount = 0
}

func (mc *mockCache) Delete(keyPrefix string) {
	mc.innerCache.Delete(keyPrefix)
}

func (dc *mockCache) Size() int {
	return dc.innerCache.Size()
}

func (dc *mockCache) Copy() Cache {
	nc := &mockCache{
		setCount:   dc.setCount,
		getCount:   dc.getCount,
		hitCount:   dc.hitCount,
		innerCache: dc.innerCache.Copy(),
	}
	return nc
}

func TestMain(m *testing.M) {
	CacheFactory = func() Cache {
		testCache := &mockCache{
			innerCache: &seriesCache{
				c:  map[string]interface{}{},
				mu: sync.RWMutex{},
			},
		}
		return testCache
	}
	m.Run()
}

func TestCacheSeries_Add(t *testing.T) {
	tests := []struct {
		series   Series
		addConst float64
		expected Series
	}{
		{
			Floats([]float64{1.5, -3.23, -0.33, -0.38, 1.6, 34.}),
			1,
			Floats([]float64{2.5, -2.23, 0.67, 0.62, 2.6, 35.}),
		},
		{
			Ints([]int{23, 13, 101, -6, -3}),
			2,
			Ints([]int{25, 15, 103, -4, -1}),
		},
	}

	for testnum, test := range tests {

		tmpSeries := test.series.CacheAble()

		expected := test.expected
		_ = tmpSeries.AddConst(test.addConst)

		received := tmpSeries.AddConst(test.addConst)

		exp := expected.Records()
		rev := received.Records()

		if !reflect.DeepEqual(exp, rev) {
			t.Errorf(
				"Test:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, exp, rev,
			)
		}

	}

}
