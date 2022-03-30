package series

type Series interface {
	Rolling(window int, minPeriods int) RollingSeries
	HasNaN() bool
	IsNaN() []bool
	IsNotNaN() []bool
	Compare(comparator Comparator, comparando interface{}) Series
	Float() []float64
	Bool() ([]bool, error)
	Int() ([]int, error)

	Order(reverse bool) []int
	StdDev() float64
	Mean() float64
	Median() float64
	Max() float64
	MaxStr() string
	Min() float64
	MinStr() string
	Quantile(p float64) float64
	Map(f MapFunction) Series
	Shift(periods int) Series
	CumProd() Series
	Prod() float64
	AddConst(c float64) Series
	MulConst(c float64) Series
	DivConst(c float64) Series
	Add(c Series) Series
	Sub(c Series) Series
	Mul(c Series) Series
	Div(c Series) Series
	Abs() Series
	Sum() float64

	Empty() Series
	Error() error
	Subset(indexes Indexes) Series

	Concat(x Series) Series
	Copy() Series
	
	Records() []string
	Type() Type
	Len() int
	String() string
	Str() string
	Val(i int) interface{}
	Elem(i int) Element
	Slice(start, end int) Series
	FillNaN(value ElementValue)
	FillNaNForward()
	FillNaNBackward()
	CacheAble() Series
	Set(indexes Indexes, newvalues Series) Series
	Append(values interface{})
	Name() string
	SetName(name string)
	SetErr(err error)
	And(in interface{}) Series
	Or(in interface{}) Series
}
