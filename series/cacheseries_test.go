package series

import (
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/patrickmn/go-cache"
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

func (mc *mockCache) DelByKeyPrefix(keyPrefix string) {
	mc.innerCache.DelByKeyPrefix(keyPrefix)
}

var testCache = &mockCache{
	innerCache: &defaultCache{
		c: cache.New(5*time.Minute, 10*time.Minute),
	},
}

func TestMain(m *testing.M) {
	ClearCache()
	InitCache(func() Cache {
		return testCache
	})
	m.Run()
	ClearCache()
}

func TestCacheSeries_Map(t *testing.T) {
	tests := []struct {
		series   Series
		expected Series
	}{
		{
			Bools([]bool{false, true, false, false, true}),
			Bools([]bool{false, true, false, false, true}),
		},
		{
			Floats([]float64{1.5, -3.23, -0.337397, -0.380079, 1.60979, 34.}),
			Floats([]float64{3, -6.46, -0.674794, -0.760158, 3.21958, 68.}),
		},
		{
			Floats([]float64{math.Pi, math.Phi, math.SqrtE, math.Cbrt(64)}),
			Floats([]float64{2 * math.Pi, 2 * math.Phi, 2 * math.SqrtE, 2 * math.Cbrt(64)}),
		},
		{
			Strings([]string{"XyZApple", "XyZBanana", "XyZCitrus", "XyZDragonfruit"}),
			Strings([]string{"Apple", "Banana", "Citrus", "Dragonfruit"}),
		},
		{
			Strings([]string{"San Francisco", "XyZTokyo", "MoscowXyZ", "XyzSydney"}),
			Strings([]string{"San Francisco", "Tokyo", "MoscowXyZ", "XyzSydney"}),
		},
		{
			Ints([]int{23, 13, 101, -64, -3}),
			Ints([]int{28, 18, 106, -59, 2}),
		},
		{
			Ints([]string{"morning", "noon", "afternoon", "evening", "night"}),
			Ints([]int{5, 5, 5, 5, 5}),
		},
	}

	doubleFloat64 := func(e Element, index int) Element {
		result := e.Copy()
		result.Set(result.Float() * 2)
		return Element(result)
	}

	// and two booleans
	and := func(e Element, index int) Element {
		result := e.Copy()
		b, err := result.Bool()
		if err != nil {
			t.Errorf("%v", err)
			return Element(nil)
		}
		result.Set(b && true)
		return Element(result)
	}

	// add constant (+5) to value (v)
	add5Int := func(e Element, index int) Element {
		result := e.Copy()
		i, err := result.Int()
		if err != nil {
			return Element(&intElement{
				e:   +5,
				nan: false,
			})
		}
		result.Set(i + 5)
		return Element(result)
	}

	// trim (XyZ) prefix from string
	trimXyZPrefix := func(e Element, index int) Element {
		result := e.Copy()
		result.Set(strings.TrimPrefix(result.String(), "XyZ"))
		return Element(result)
	}

	setCount := 0
	getCount := 0
	hitCount := 0
	ClearCache()
	for testnum, test := range tests {
		test.series.SetName(fmt.Sprintf("Name-%d", testnum))
		tmpSeries := test.series.CacheAble()
		var received Series
		switch test.series.Type() {
		case Bool:
			expected := test.expected
			received = tmpSeries.Map(and)
			getCount++
			setCount++
			received = tmpSeries.Map(and)
			getCount++
			hitCount++
			for i := 0; i < expected.Len(); i++ {
				e, _ := expected.Elem(i).Bool()
				r, _ := received.Elem(i).Bool()

				if e != r {
					t.Errorf(
						"Test:%v\nExpected:\n%v\nReceived:\n%v",
						testnum, expected, received,
					)
				}
			}

		case Float:
			expected := test.expected
			received = tmpSeries.Map(doubleFloat64)
			getCount++
			setCount++
			received = tmpSeries.Map(doubleFloat64)
			getCount++
			hitCount++
			for i := 0; i < expected.Len(); i++ {
				if !compareFloats(expected.Elem(i).Float(),
					received.Elem(i).Float(), 6) {
					t.Errorf(
						"Test:%v\nExpected:\n%v\nReceived:\n%v",
						testnum, expected, received,
					)
				}
			}
		case Int:
			expected := test.expected
			received = tmpSeries.Map(add5Int)
			getCount++
			setCount++
			received = tmpSeries.Map(add5Int)
			getCount++
			hitCount++
			for i := 0; i < expected.Len(); i++ {
				e, _ := expected.Elem(i).Int()
				r, _ := received.Elem(i).Int()
				if e != r {
					t.Errorf(
						"Test:%v\nExpected:\n%v\nReceived:\n%v",
						testnum, expected, received,
					)
				}
			}
		case String:
			expected := test.expected
			received = tmpSeries.Map(trimXyZPrefix)
			getCount++
			setCount++
			received = tmpSeries.Map(trimXyZPrefix)
			getCount++
			hitCount++
			for i := 0; i < expected.Len(); i++ {
				if strings.Compare(expected.Elem(i).String(),
					received.Elem(i).String()) != 0 {
					t.Errorf(
						"Test:%v\nExpected:\n%v\nReceived:\n%v",
						testnum, expected, received,
					)
				}
			}
		default:
		}
	}
	if setCount != testCache.setCount {
		t.Errorf("CacheInfo[setCount]:\nExpected:%v\nActual:%v", setCount, testCache.setCount)
	}
	if getCount != testCache.getCount {
		t.Errorf("CacheInfo[getCount]:\nExpected:%v\nActual:%v", getCount, testCache.getCount)
	}
	if hitCount != testCache.hitCount {
		t.Errorf("CacheInfo[hitCount]:\nExpected:%v\nActual:%v", hitCount, testCache.hitCount)
	}

}

func TestCacheSeries_Compare(t *testing.T) {
	table := []struct {
		series     Series
		comparator Comparator
		comparando interface{}
		expected   Series
	}{
		{
			Strings([]string{"A", "B", "C", "B", "D", "BADA"}),
			Eq,
			"B",
			Bools([]bool{false, true, false, true, false, false}),
		},
		{
			Strings([]string{"A", "B", "C", "B", "D", "BADA"}),
			Eq,
			[]string{"B", "B", "C", "D", "A", "A"},
			Bools([]bool{false, true, true, false, false, false}),
		},
		{
			Ints([]int{0, 2, 1, 5, 9}),
			Eq,
			"2",
			Bools([]bool{false, true, false, false, false}),
		},
		{
			Ints([]int{0, 2, 1, 5, 9}),
			Eq,
			[]int{0, 2, 0, 5, 10},
			Bools([]bool{true, true, false, true, false}),
		},
		{
			Floats([]float64{0.1, 2, 1, 5, 9}),
			Eq,
			"2",
			Bools([]bool{false, true, false, false, false}),
		},
		{
			Floats([]float64{0.1, 2, 1, 5, 9}),
			Eq,
			[]float64{0.1, 2, 0, 5, 10},
			Bools([]bool{true, true, false, true, false}),
		},
		{
			Bools([]bool{true, true, false}),
			Eq,
			"true",
			Bools([]bool{true, true, false}),
		},
		{
			Bools([]bool{true, true, false}),
			Eq,
			[]bool{true, false, false},
			Bools([]bool{true, false, true}),
		},
		{
			Strings([]string{"A", "B", "C", "B", "D", "BADA"}),
			Neq,
			"B",
			Bools([]bool{true, false, true, false, true, true}),
		},
		{
			Strings([]string{"A", "B", "C", "B", "D", "BADA"}),
			Neq,
			[]string{"B", "B", "C", "D", "A", "A"},
			Bools([]bool{true, false, false, true, true, true}),
		},
		{
			Ints([]int{0, 2, 1, 5, 9}),
			Neq,
			"2",
			Bools([]bool{true, false, true, true, true}),
		},
		{
			Ints([]int{0, 2, 1, 5, 9}),
			Neq,
			[]int{0, 2, 0, 5, 10},
			Bools([]bool{false, false, true, false, true}),
		},
		{
			Floats([]float64{0.1, 2, 1, 5, 9}),
			Neq,
			"2",
			Bools([]bool{true, false, true, true, true}),
		},
		{
			Floats([]float64{0.1, 2, 1, 5, 9}),
			Neq,
			[]float64{0.1, 2, 0, 5, 10},
			Bools([]bool{false, false, true, false, true}),
		},
		{
			Bools([]bool{true, true, false}),
			Neq,
			"true",
			Bools([]bool{false, false, true}),
		},
		{
			Bools([]bool{true, true, false}),
			Neq,
			[]bool{true, false, false},
			Bools([]bool{false, true, false}),
		},
		{
			Strings([]string{"A", "B", "C", "B", "D", "BADA"}),
			Greater,
			"B",
			Bools([]bool{false, false, true, false, true, true}),
		},
		{
			Strings([]string{"A", "B", "C", "B", "D", "BADA"}),
			Greater,
			[]string{"B", "B", "C", "D", "A", "A"},
			Bools([]bool{false, false, false, false, true, true}),
		},
		{
			Ints([]int{0, 2, 1, 5, 9}),
			Greater,
			"2",
			Bools([]bool{false, false, false, true, true}),
		},
		{
			Ints([]int{0, 2, 1, 5, 9}),
			Greater,
			[]int{0, 2, 0, 5, 10},
			Bools([]bool{false, false, true, false, false}),
		},
		{
			Floats([]float64{0.1, 2, 1, 5, 9}),
			Greater,
			"2",
			Bools([]bool{false, false, false, true, true}),
		},
		{
			Floats([]float64{0.1, 2, 1, 5, 9}),
			Greater,
			[]float64{0.1, 2, 0, 5, 10},
			Bools([]bool{false, false, true, false, false}),
		},
		{
			Bools([]bool{true, true, false}),
			Greater,
			"true",
			Bools([]bool{false, false, false}),
		},
		{
			Bools([]bool{true, true, false}),
			Greater,
			[]bool{true, false, false},
			Bools([]bool{false, true, false}),
		},
		{
			Strings([]string{"A", "B", "C", "B", "D", "BADA"}),
			GreaterEq,
			"B",
			Bools([]bool{false, true, true, true, true, true}),
		},
		{
			Strings([]string{"A", "B", "C", "B", "D", "BADA"}),
			GreaterEq,
			[]string{"B", "B", "C", "D", "A", "A"},
			Bools([]bool{false, true, true, false, true, true}),
		},
		{
			Ints([]int{0, 2, 1, 5, 9}),
			GreaterEq,
			"2",
			Bools([]bool{false, true, false, true, true}),
		},
		{
			Ints([]int{0, 2, 1, 5, 9}),
			GreaterEq,
			[]int{0, 2, 0, 5, 10},
			Bools([]bool{true, true, true, true, false}),
		},
		{
			Floats([]float64{0.1, 2, 1, 5, 9}),
			GreaterEq,
			"2",
			Bools([]bool{false, true, false, true, true}),
		},
		{
			Floats([]float64{0.1, 2, 1, 5, 9}),
			GreaterEq,
			[]float64{0.1, 2, 0, 5, 10},
			Bools([]bool{true, true, true, true, false}),
		},
		{
			Bools([]bool{true, true, false}),
			GreaterEq,
			"true",
			Bools([]bool{true, true, false}),
		},
		{
			Bools([]bool{true, true, false}),
			GreaterEq,
			[]bool{true, false, false},
			Bools([]bool{true, true, true}),
		},
		{
			Strings([]string{"A", "B", "C", "B", "D", "BADA"}),
			Less,
			"B",
			Bools([]bool{true, false, false, false, false, false}),
		},
		{
			Strings([]string{"A", "B", "C", "B", "D", "BADA"}),
			Less,
			[]string{"B", "B", "C", "D", "A", "A"},
			Bools([]bool{true, false, false, true, false, false}),
		},
		{
			Ints([]int{0, 2, 1, 5, 9}),
			Less,
			"2",
			Bools([]bool{true, false, true, false, false}),
		},
		{
			Ints([]int{0, 2, 1, 5, 9}),
			Less,
			[]int{0, 2, 0, 5, 10},
			Bools([]bool{false, false, false, false, true}),
		},
		{
			Floats([]float64{0.1, 2, 1, 5, 9}),
			Less,
			"2",
			Bools([]bool{true, false, true, false, false}),
		},
		{
			Floats([]float64{0.1, 2, 1, 5, 9}),
			Less,
			[]float64{0.1, 2, 0, 5, 10},
			Bools([]bool{false, false, false, false, true}),
		},
		{
			Bools([]bool{true, true, false}),
			Less,
			"true",
			Bools([]bool{false, false, true}),
		},
		{
			Bools([]bool{true, true, false}),
			Less,
			[]bool{true, false, false},
			Bools([]bool{false, false, false}),
		},
		{
			Strings([]string{"A", "B", "C", "B", "D", "BADA"}),
			LessEq,
			"B",
			Bools([]bool{true, true, false, true, false, false}),
		},
		{
			Strings([]string{"A", "B", "C", "B", "D", "BADA"}),
			LessEq,
			[]string{"B", "B", "C", "D", "A", "A"},
			Bools([]bool{true, true, true, true, false, false}),
		},
		{
			Ints([]int{0, 2, 1, 5, 9}),
			LessEq,
			"2",
			Bools([]bool{true, true, true, false, false}),
		},
		{
			Ints([]int{0, 2, 1, 5, 9}),
			LessEq,
			[]int{0, 2, 0, 5, 10},
			Bools([]bool{true, true, false, true, true}),
		},
		{
			Floats([]float64{0.1, 2, 1, 5, 9}),
			LessEq,
			"2",
			Bools([]bool{true, true, true, false, false}),
		},
		{
			Floats([]float64{0.1, 2, 1, 5, 9}),
			LessEq,
			[]float64{0.1, 2, 0, 5, 10},
			Bools([]bool{true, true, false, true, true}),
		},
		{
			Bools([]bool{true, true, false}),
			LessEq,
			"true",
			Bools([]bool{true, true, true}),
		},
		{
			Bools([]bool{true, true, false}),
			LessEq,
			[]bool{true, false, false},
			Bools([]bool{true, false, true}),
		},
		{
			Strings([]string{"A", "B", "C", "B", "D", "BADA"}),
			In,
			"B",
			Bools([]bool{false, true, false, true, false, false}),
		},
		{
			Strings([]string{"Hello", "world", "this", "is", "a", "test"}),
			In,
			[]string{"cat", "world", "hello", "a"},
			Bools([]bool{false, true, false, false, true, false}),
		},
		{
			Ints([]int{0, 2, 1, 5, 9}),
			In,
			"2",
			Bools([]bool{false, true, false, false, false}),
		},
		{
			Ints([]int{0, 2, 1, 5, 9}),
			In,
			[]int{2, 99, 1234, 9},
			Bools([]bool{false, true, false, false, true}),
		},
		{
			Floats([]float64{0.1, 2, 1, 5, 9}),
			In,
			"2",
			Bools([]bool{false, true, false, false, false}),
		},
		{
			Floats([]float64{0.1, 2, 1, 5, 9}),
			In,
			[]float64{2, 99, 1234, 9},
			Bools([]bool{false, true, false, false, true}),
		},
		{
			Bools([]bool{true, true, false}),
			In,
			"true",
			Bools([]bool{true, true, false}),
		},
		{
			Bools([]bool{true, true, false}),
			In,
			[]bool{false, false, false},
			Bools([]bool{false, false, true}),
		},
	}
	ClearCache()
	for testnum, test := range table {
		test.series.SetName(fmt.Sprintf("Name-%d", testnum))
		a := test.series.CacheAble()
		b := a.Compare(test.comparator, test.comparando)
		b = a.Compare(test.comparator, test.comparando)
		if err := b.Error(); err != nil {
			t.Errorf("Test:%v\nError:%v", testnum, err)
		}
		expected := test.expected.Records()
		received := b.Records()
		if !reflect.DeepEqual(expected, received) {
			t.Errorf(
				"Test:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, expected, received,
			)
		}
		if err := checkTypes(b); err != nil {
			t.Errorf(
				"Test:%v\nError:%v",
				testnum, err,
			)
		}
	}

	fmt.Printf("getCount:%d, setCount:%d, hitCount:%d \n", testCache.getCount, testCache.setCount, testCache.hitCount)
}
