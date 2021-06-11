package series

type Rolling interface{
	Max() ElementValue
	Min() ElementValue
	Mean() float64
	Quantile(p float64) float64
	Median() float64
	StdDev() float64
}