package series

import "math"

// Variance the amount of population variation in the Series
func (s Series) Variance() float64 {
	data := getFloats(s, true)
	return _variance(data, false)
}

// SampleVariance finds the amount of variance within sample Series
func (s Series) SampleVariance() float64 {
	data := getFloats(s, true)
	return _variance(data, true)
}

//Covariance computes covariance fo both Series
func (s Series) Covariance(so Series) (float64, error) {
	data1 := getFloats(s, true)
	data2 := getFloats(so, true)
	return Covariance(data1, data2)
}

// CovariancePopulation computes covariance of entire population between both Series
func (s Series) CovariancePopulation(so Series) (float64, error) {
	data1 := getFloats(s, true)
	data2 := getFloats(so, true)
	return CovariancePopulation(data1, data2)
}

// Quartile computes three quartile points
func (s Series) Quartile() Series {
	data := getFloats(s, true)
	quartiles := Quartile(data)
	return Floats(quartiles)
}

//Median computes median value
func (s Series) Median() float64 {
	data := getFloats(s, true)
	return Median(data)
}

func (s Series) Outliers() []float64 {
	data := getFloats(s, true)
	return Outliers(data)
}

//GeometricMean computes geometric mean
func (s Series) GeometricMean() float64 {
	data := getFloats(s, true)
	return GeometricMean(data)
}

//Mean computes the mean value
func (s Series) Mean() float64 {
	data := getFloats(s, true)
	return Mean(data)
}

//Sum sums of elements
func (s Series) Sum() float64 {
	data := getFloats(s, true)
	return Sum(data)
}

func (s Series) Stats() (count int, min, max, sum, mean float64) {
	data := getFloats(s, true)
	count, min, max, sum, mean = Stats(data)
	return
}

//Min finds the smalest Element in Series
func Min(input Series) (min Element, err error) {
	if input.Len() == 0 {
		return nil, ErrEmptyInput
	}

	idx, min := FirstNonNan(input)
	if min == nil {
		return nil, ErrAllNA
	}

	for i := idx; i < input.Len(); i++ {
		if input.Elem(i).Less(min) {
			min = input.Elem(i)
		}
	}
	return min, nil
}

// Max finds the highest Element in Series
func Max(input Series) (max Element, err error) {
	if input.Len() == 0 {
		return nil, ErrEmptyInput
	}

	idx, max := FirstNonNan(input)
	if max == nil {
		return nil, ErrAllNA
	}

	for i := idx; i < input.Len(); i++ {
		if input.Elem(i).Greater(max) {
			max = input.Elem(i)
		}
	}
	return max, nil
}

//FirstNonNan finds first non NaN Element
// returns it index and Element itself
func FirstNonNan(series Series) (int, Element) {
	for idx, el := range series.elements {
		if !el.IsNA() {
			return idx, el
		}
	}

	return 0, nil
}

func getFloats(input Series, rmNA bool) []float64 {

	if !rmNA {
		return input.Float()
	}

	data := input.Float()
	var result []float64
	for _, f := range data {
		if !math.IsNaN(f) && !math.IsInf(f, 0) {
			result = append(result, f)
		}
	}
	return result
}
