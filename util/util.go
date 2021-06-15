package util

import "github.com/shopspring/decimal"

func MakeFloatSlice(size int, defaultValue float64) []float64 {
	fs := make([]float64, size)
	for i := 0; i < size; i++ {
		fs[i] = defaultValue
	}
	return fs
}

func MakeFloatSliceRange(size int, start float64, step float64) []float64 {
	fs := make([]float64, size)
	for i := 0; i < size; i++ {
		fs[i], _ = decimal.NewFromFloat(start).Add(decimal.NewFromFloat(step).Mul(decimal.NewFromInt32(int32(i)))).Float64()
			
	}
	return fs
}