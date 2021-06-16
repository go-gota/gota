package util

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
		fs[i] = start + step * float64(i)
	}
	return fs
}