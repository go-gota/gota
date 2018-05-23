package series

import (
	"math"
	"sort"
)

// _variance finds the variance for population and sample data
func _variance(data []float64, issample bool) float64 {

	mean := Mean(data)

	variance := 0.0
	for _, f := range data {
		v := (f - mean)
		variance += v * v
	}
	l := len(data)
	if issample {
		l--
	}

	return variance / float64(l)
}

// Variance the amount of population variation in the float64 slice
func Variance(input []float64) float64 {
	return _variance(input, false)
}

// SampleVariance finds the amount of variance within sample float64 slice
func SampleVariance(input []float64) float64 {
	return _variance(input, true)
}

// Covariance is a measure of how much two sets of data change
func Covariance(data1, data2 []float64) (float64, error) {

	l1 := len(data1)
	l2 := len(data2)

	if l1 != l2 {
		return math.NaN(), ErrSizeDiffer
	}
	m1 := Mean(data1)
	m2 := Mean(data2)

	var ss float64
	for i := 0; i < l1; i++ {
		delta1 := (data1[i] - m1)
		delta2 := (data2[i] - m2)
		ss += (delta1*delta2 - ss) / float64(i+1)
	}
	return ss * float64(l1) / float64(l1-1), nil
}

// CovariancePopulation covariance for entire population between two float64 slices
func CovariancePopulation(data1, data2 []float64) (float64, error) {

	l1 := len(data1)
	l2 := len(data2)

	if l1 != l2 {
		return math.NaN(), ErrSizeDiffer
	}
	m1 := Mean(data1)
	m2 := Mean(data2)

	var ss float64
	for i := 0; i < l1; i++ {
		delta1 := (data1[i] - m1)
		delta2 := (data2[i] - m2)
		ss += delta1 * delta2
	}
	return ss / float64(l1), nil
}

//Quartile returns the three quartile points from the float64 slice
func Quartile(data []float64) []float64 {

	l := len(data)
	cdata := make([]float64, l)
	copy(cdata, data)
	sort.Float64s(cdata)
	var c1, c2 int

	if l%2 == 0 {
		c1 = l / 2
		c2 = l / 2

	} else {
		c1 = (l - 1) / 2
		c2 = c1 + 1
	}
	q1 := Median(cdata[:c1])
	q2 := Median(data) //set data sa it is orderred inside
	q3 := Median(cdata[c2:])

	return []float64{q1, q2, q3}
}

func Outliers(data []float64) []float64 {
	quartiles := Quartile(data)
	iqr := 1.5 * (quartiles[2] - quartiles[0])
	low := quartiles[0] - iqr
	high := quartiles[2] + iqr
	var r []float64
	for _, v := range data {
		if v < low || v > high {
			r = append(r, v)
		}
	}
	return r
}

//Median finds the number in slice
func Median(data []float64) float64 {
	l := len(data)
	if l == 0 {
		return math.NaN()
	}

	if l == 1 {
		return data[0]
	}

	cdata := make([]float64, l)
	copy(cdata, data)
	sort.Float64s(cdata)

	l2 := int(l / 2)
	var median float64
	if l%2 == 0 {
		median = Mean(cdata[l2-1 : l2+1])
	} else {
		median = cdata[l2]
	}
	return median
}

// GeometricMean finds geometric mean for slice
func GeometricMean(data []float64) float64 {
	p := 0.0
	for _, f := range data {
		if p == 0 {
			p = f
		} else {
			p *= f
		}
	}

	return math.Pow(p, 1/float64(len(data)))
}

//Mean finds the mean of the slice
func Mean(data []float64) float64 {
	return Sum(data) / float64(len(data))
}

//Sum finds the sum of elements
func Sum(data []float64) float64 {
	sum := 0.0

	for _, f := range data {
		sum += f
	}
	return sum
}

func Stats(data []float64) (count int, min, max, sum, mean float64) {
	if len(data) < 1 {
		return 0, 0., 0., 0., 0.
	}
	sum = 0.0
	count = len(data)
	min = data[0]
	max = data[0]

	for _, f := range data {
		sum += f
		if f < min {
			min = f
		}

		if f > max {
			max = f
		}
	}
	mean = sum / float64(count)
	return
}
