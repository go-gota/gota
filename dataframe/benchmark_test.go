package dataframe_test

import (
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
)

func generateSeries(n, rep int) (data []series.Series) {
	rand.Seed(100)
	for j := 0; j < rep; j++ {
		var is []int
		var bs []bool
		var fs []float64
		var ss []string
		for i := 0; i < n; i++ {
			is = append(is, rand.Int())
		}
		for i := 0; i < n; i++ {
			fs = append(fs, rand.Float64())
		}
		for i := 0; i < n; i++ {
			ss = append(ss, strconv.Itoa(rand.Int()))
		}
		for i := 0; i < n; i++ {
			r := rand.Intn(2)
			b := false
			if r == 1 {
				b = true
			}
			bs = append(bs, b)
		}
		data = append(data, series.Ints(is))
		data = append(data, series.Bools(bs))
		data = append(data, series.Floats(fs))
		data = append(data, series.Strings(ss))
	}
	return
}

func generateSeriesRandomType(numOfRows, numOfCols int) (data []series.Series) {
	rand.Seed(100)
	colTypes := []series.Type{series.Int, series.Float}
	cols := make([]series.Series, numOfCols)
	for i := 0; i < numOfCols; i++ {
		colIdx := rand.Intn(len(colTypes))
		colType := colTypes[colIdx]
		vals := make([]interface{}, numOfRows)
		for j := 0; j < numOfRows; j++ {
			var val interface{}
			if colType == series.Int {
				val = rand.Intn(100)
			} else {
				val = rand.Float64()
			}
			vals[j] = val
		}
		cols[i] = series.New(vals, colType, "")
	}
	return cols
}

func generateIntsN(n, k int) (data []int) {
	for i := 0; i < n; i++ {
		data = append(data, rand.Intn(k))
	}
	return
}

func BenchmarkNew(b *testing.B) {
	table := []struct {
		name string
		data []series.Series
	}{
		{
			"100000x4",
			generateSeries(100000, 1),
		},
		{
			"100000x40",
			generateSeries(100000, 10),
		},
		{
			"100000x400",
			generateSeries(100000, 100),
		},
		{
			"1000x40",
			generateSeries(1000, 10),
		},
		{
			"1000x4000",
			generateSeries(1000, 1000),
		},
		{
			"1000x40000",
			generateSeries(1000, 10000),
		},
	}
	for _, test := range table {
		b.Run(test.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				dataframe.New(test.data...)
			}
		})
	}
}

func BenchmarkDataFrame_Arrange(b *testing.B) {
	data := dataframe.New(generateSeries(100000, 5)...)
	table := []struct {
		name string
		data dataframe.DataFrame
		key  []dataframe.Order
	}{
		{
			"100000x20_1",
			data,
			[]dataframe.Order{dataframe.Sort("X0")},
		},
		{
			"100000x20_2",
			data,
			[]dataframe.Order{
				dataframe.Sort("X0"),
				dataframe.Sort("X1"),
			},
		},
		{
			"100000x20_3",
			data,
			[]dataframe.Order{
				dataframe.Sort("X0"),
				dataframe.Sort("X1"),
				dataframe.Sort("X2"),
			},
		},
	}
	for _, test := range table {
		b.Run(test.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				test.data.Arrange(test.key...)
			}
		})
	}
}

func BenchmarkDataFrame_Subset(b *testing.B) {
	b.ReportAllocs()
	data1000x20 := dataframe.New(generateSeries(1000, 5)...)
	data1000x200 := dataframe.New(generateSeries(1000, 50)...)
	data1000x2000 := dataframe.New(generateSeries(1000, 500)...)
	data100000x20 := dataframe.New(generateSeries(100000, 5)...)
	data1000000x20 := dataframe.New(generateSeries(1000000, 5)...)
	idx10 := generateIntsN(10, 10)
	idx100 := generateIntsN(100, 100)
	idx1000 := generateIntsN(1000, 1000)
	idx10000 := generateIntsN(10000, 10000)
	idx100000 := generateIntsN(100000, 100000)
	idx1000000 := generateIntsN(1000000, 1000000)
	table := []struct {
		name    string
		data    dataframe.DataFrame
		indexes interface{}
	}{
		{
			"1000000x20_100",
			data1000000x20,
			idx100,
		},
		{
			"1000000x20_1000",
			data1000000x20,
			idx1000,
		},
		{
			"1000000x20_10000",
			data1000000x20,
			idx10000,
		},
		{
			"1000000x20_100000",
			data1000000x20,
			idx100000,
		},
		{
			"1000000x20_1000000",
			data1000000x20,
			idx1000000,
		},
		{
			"100000x20_100",
			data100000x20,
			idx100,
		},
		{
			"100000x20_1000",
			data100000x20,
			idx1000,
		},
		{
			"100000x20_10000",
			data100000x20,
			idx10000,
		},
		{
			"100000x20_100000",
			data100000x20,
			idx100000,
		},
		{
			"1000x20_10",
			data1000x20,
			idx10,
		},
		{
			"1000x20_100",
			data1000x20,
			idx100,
		},
		{
			"1000x20_1000",
			data1000x20,
			idx1000,
		},
		{
			"1000x200_10",
			data1000x200,
			idx10,
		},
		{
			"1000x200_100",
			data1000x200,
			idx100,
		},
		{
			"1000x200_1000",
			data1000x200,
			idx1000,
		},
		{
			"1000x2000_10",
			data1000x2000,
			idx10,
		},
		{
			"1000x2000_100",
			data1000x2000,
			idx100,
		},
		{
			"1000x2000_1000",
			data1000x2000,
			idx1000,
		},
	}
	for _, test := range table {
		b.Run(test.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				test.data.Subset(test.indexes)
			}
		})
	}
}

func BenchmarkDataFrame_Elem(b *testing.B) {
	data := dataframe.New(generateSeries(100000, 5)...)
	table := []struct {
		name string
		data dataframe.DataFrame
	}{
		{
			"100000x20_ALL",
			data,
		},
	}
	for _, test := range table {
		b.Run(test.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				for k := 0; k < 100000; k++ {
					test.data.Elem(k, 0)
				}
			}
		})
	}
}

type joinColRowOrdering int

const (
	sorted joinColRowOrdering = iota
	reversed
	random
)

func shuffleSlice(slice []int) {
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(slice), func(i, j int) { slice[i], slice[j] = slice[j], slice[i] })
}

func generateJoinSeries(
	colName string,
	leftRowCount,
	rightRowCount,
	relationScaleLeftToRight,
	relationScaleRightToLeft int,
	joinRowOrdering joinColRowOrdering) (series.Series, series.Series) {

	leftVals := make([]int, leftRowCount)
	for i := 0; i < leftRowCount*relationScaleLeftToRight; i++ {
		if i >= leftRowCount {
			break
		}
		val := i / relationScaleLeftToRight
		leftVals[i] = val + 1
	}
	rightVals := make([]int, rightRowCount)
	for i := 0; i < rightRowCount*relationScaleRightToLeft; i++ {
		// for j := 0; j < relationScaleRightToLeft; j++ {
		if i >= rightRowCount {
			break
		}
		val := i / relationScaleRightToLeft
		if joinRowOrdering == reversed {
			rightVals[i] = rightRowCount - val
			continue
		}
		rightVals[i] = val + 1

	}
	if joinRowOrdering == random {
		shuffleSlice(leftVals)
		shuffleSlice(rightVals)
	}
	leftSeries := series.New(leftVals, series.Int, colName)
	rightSeries := series.New(rightVals, series.Int, colName)

	return leftSeries, rightSeries
}

func BenchmarkDataFrame_InnerJoinOptimized(b *testing.B) {
	table := []struct {
		name                     string
		joinRows                 []string
		leftRowCount             int
		leftColCount             int
		rightRowCount            int
		rightColCount            int
		joinColOrdering          joinColRowOrdering
		relationScaleLeftToRight int
		relationScaleRightToLeft int
	}{
		{
			name:                     "10 rows - 10 rows, 1 to 1 join, 2 rows, random",
			joinRows:                 []string{"join_row1", "join_row2"},
			leftRowCount:             10,
			leftColCount:             4,
			rightRowCount:            10,
			rightColCount:            12,
			joinColOrdering:          random,
			relationScaleLeftToRight: 1,
			relationScaleRightToLeft: 1,
		},
		{
			name:                     "10 rows - 10 rows, 1 to 2 join, 2 rows, random",
			joinRows:                 []string{"join_row1", "join_row2"},
			leftRowCount:             10,
			leftColCount:             4,
			rightRowCount:            10,
			rightColCount:            12,
			joinColOrdering:          random,
			relationScaleLeftToRight: 1,
			relationScaleRightToLeft: 2,
		},
		{
			name:                     "50 rows - 50 rows, 1 to 1 join, random",
			joinRows:                 []string{"join_row"},
			leftRowCount:             50,
			leftColCount:             4,
			rightRowCount:            50,
			rightColCount:            12,
			joinColOrdering:          random,
			relationScaleLeftToRight: 1,
			relationScaleRightToLeft: 1,
		},
		{
			name:                     "50 rows - 50 rows, 1 to 1 join, 2 join rows, random",
			joinRows:                 []string{"join_row1", "join_row2"},
			leftRowCount:             50,
			leftColCount:             4,
			rightRowCount:            50,
			rightColCount:            12,
			joinColOrdering:          random,
			relationScaleLeftToRight: 1,
			relationScaleRightToLeft: 1,
		},
		{
			name:                     "100 rows - 100 rows, 1 to 1 join, random",
			joinRows:                 []string{"join_row"},
			leftRowCount:             100,
			leftColCount:             4,
			rightRowCount:            100,
			rightColCount:            12,
			joinColOrdering:          random,
			relationScaleLeftToRight: 1,
			relationScaleRightToLeft: 1,
		},
		{
			name:                     "300 rows - 300 rows, 1 to 1 join, random",
			joinRows:                 []string{"join_row"},
			leftRowCount:             300,
			leftColCount:             4,
			rightRowCount:            300,
			rightColCount:            12,
			joinColOrdering:          random,
			relationScaleLeftToRight: 1,
			relationScaleRightToLeft: 1,
		},
		{
			name:                     "300 rows - 300 rows, 1 to 1 join, 2 join rows, random",
			joinRows:                 []string{"join_row1", "join_row2"},
			leftRowCount:             300,
			leftColCount:             4,
			rightRowCount:            300,
			rightColCount:            12,
			joinColOrdering:          random,
			relationScaleLeftToRight: 1,
			relationScaleRightToLeft: 1,
		},
		{
			name:                     "900 rows - 900 rows, 1 to 1 join, 2 join rows, random",
			joinRows:                 []string{"join_row1", "join_row2"},
			leftRowCount:             900,
			leftColCount:             4,
			rightRowCount:            900,
			rightColCount:            12,
			joinColOrdering:          random,
			relationScaleLeftToRight: 1,
			relationScaleRightToLeft: 1,
		},
		{
			name:                     "1000 rows - 1000 rows, 1 to 1 join, 2 join rows, random",
			joinRows:                 []string{"join_row1", "join_row2"},
			leftRowCount:             1000,
			leftColCount:             4,
			rightRowCount:            1000,
			rightColCount:            12,
			joinColOrdering:          random,
			relationScaleLeftToRight: 1,
			relationScaleRightToLeft: 1,
		},
	}
	for _, t := range table {
		b.StopTimer()
		leftCols := generateSeriesRandomType(t.leftRowCount, t.leftColCount)
		rightCols := generateSeriesRandomType(t.rightRowCount, t.rightColCount)

		for _, joinKey := range t.joinRows {
			leftJoinSeries, rightJoinSeries := generateJoinSeries(joinKey, t.leftRowCount, t.rightRowCount, t.relationScaleLeftToRight, t.relationScaleRightToLeft, t.joinColOrdering)
			leftCols = append(leftCols, leftJoinSeries)
			rightCols = append(rightCols, rightJoinSeries)
		}
		leftTbl := dataframe.New(leftCols...)
		rightTbl := dataframe.New(rightCols...)

		b.StartTimer()
		b.Run(t.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = leftTbl.InnerJoin(rightTbl, t.joinRows...)
			}
		})
	}
}

func BenchmarkDataFrame_LeftJoinOptimized(b *testing.B) {
	table := []struct {
		name                     string
		joinRows                 []string
		leftRowCount             int
		leftColCount             int
		rightRowCount            int
		rightColCount            int
		joinColOrdering          joinColRowOrdering
		relationScaleLeftToRight int
		relationScaleRightToLeft int
	}{
		{
			name:                     "10 rows - 10 rows, 1 to 1 join, 2 rows, random",
			joinRows:                 []string{"join_row1", "join_row2"},
			leftRowCount:             10,
			leftColCount:             4,
			rightRowCount:            10,
			rightColCount:            12,
			joinColOrdering:          random,
			relationScaleLeftToRight: 1,
			relationScaleRightToLeft: 1,
		},
		{
			name:                     "10 rows - 10 rows, 1 to 2 join, 2 rows, random",
			joinRows:                 []string{"join_row1", "join_row2"},
			leftRowCount:             10,
			leftColCount:             4,
			rightRowCount:            10,
			rightColCount:            12,
			joinColOrdering:          random,
			relationScaleLeftToRight: 1,
			relationScaleRightToLeft: 2,
		},
		{
			name:                     "50 rows - 50 rows, 1 to 1 join, random",
			joinRows:                 []string{"join_row"},
			leftRowCount:             50,
			leftColCount:             4,
			rightRowCount:            50,
			rightColCount:            12,
			joinColOrdering:          random,
			relationScaleLeftToRight: 1,
			relationScaleRightToLeft: 1,
		},
		{
			name:                     "50 rows - 50 rows, 1 to 1 join, 2 join rows, random",
			joinRows:                 []string{"join_row1", "join_row2"},
			leftRowCount:             50,
			leftColCount:             4,
			rightRowCount:            50,
			rightColCount:            12,
			joinColOrdering:          random,
			relationScaleLeftToRight: 1,
			relationScaleRightToLeft: 1,
		},
		{
			name:                     "100 rows - 100 rows, 1 to 1 join, random",
			joinRows:                 []string{"join_row"},
			leftRowCount:             100,
			leftColCount:             4,
			rightRowCount:            100,
			rightColCount:            12,
			joinColOrdering:          random,
			relationScaleLeftToRight: 1,
			relationScaleRightToLeft: 1,
		},
		{
			name:                     "300 rows - 300 rows, 1 to 1 join, random",
			joinRows:                 []string{"join_row"},
			leftRowCount:             300,
			leftColCount:             4,
			rightRowCount:            300,
			rightColCount:            12,
			joinColOrdering:          random,
			relationScaleLeftToRight: 1,
			relationScaleRightToLeft: 1,
		},
		{
			name:                     "300 rows - 300 rows, 1 to 1 join, 2 join rows, random",
			joinRows:                 []string{"join_row1", "join_row2"},
			leftRowCount:             300,
			leftColCount:             4,
			rightRowCount:            300,
			rightColCount:            12,
			joinColOrdering:          random,
			relationScaleLeftToRight: 1,
			relationScaleRightToLeft: 1,
		},
		{
			name:                     "900 rows - 900 rows, 1 to 1 join, 2 join rows, random",
			joinRows:                 []string{"join_row1", "join_row2"},
			leftRowCount:             900,
			leftColCount:             4,
			rightRowCount:            900,
			rightColCount:            12,
			joinColOrdering:          random,
			relationScaleLeftToRight: 1,
			relationScaleRightToLeft: 1,
		},
		{
			name:                     "1000 rows - 1000 rows, 1 to 1 join, 2 join rows, random",
			joinRows:                 []string{"join_row1", "join_row2"},
			leftRowCount:             1000,
			leftColCount:             4,
			rightRowCount:            1000,
			rightColCount:            12,
			joinColOrdering:          random,
			relationScaleLeftToRight: 1,
			relationScaleRightToLeft: 1,
		},
	}
	for _, t := range table {
		b.StopTimer()
		leftCols := generateSeriesRandomType(t.leftRowCount, t.leftColCount)
		rightCols := generateSeriesRandomType(t.rightRowCount, t.rightColCount)

		for _, joinKey := range t.joinRows {
			leftJoinSeries, rightJoinSeries := generateJoinSeries(joinKey, t.leftRowCount, t.rightRowCount, t.relationScaleLeftToRight, t.relationScaleRightToLeft, t.joinColOrdering)
			leftCols = append(leftCols, leftJoinSeries)
			rightCols = append(rightCols, rightJoinSeries)
		}
		leftTbl := dataframe.New(leftCols...)
		rightTbl := dataframe.New(rightCols...)

		b.StartTimer()
		b.Run(t.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = leftTbl.LeftJoin(rightTbl, t.joinRows...)
			}
		})
	}
}

func BenchmarkDataFrame_RightJoinOptimized(b *testing.B) {
	table := []struct {
		name                     string
		joinRows                 []string
		leftRowCount             int
		leftColCount             int
		rightRowCount            int
		rightColCount            int
		joinColOrdering          joinColRowOrdering
		relationScaleLeftToRight int
		relationScaleRightToLeft int
	}{
		{
			name:                     "10 rows - 10 rows, 1 to 1 join, 2 rows, random",
			joinRows:                 []string{"join_row1", "join_row2"},
			leftRowCount:             10,
			leftColCount:             4,
			rightRowCount:            10,
			rightColCount:            12,
			joinColOrdering:          random,
			relationScaleLeftToRight: 1,
			relationScaleRightToLeft: 1,
		},
		{
			name:                     "10 rows - 10 rows, 1 to 2 join, 2 rows, random",
			joinRows:                 []string{"join_row1", "join_row2"},
			leftRowCount:             10,
			leftColCount:             4,
			rightRowCount:            10,
			rightColCount:            12,
			joinColOrdering:          random,
			relationScaleLeftToRight: 1,
			relationScaleRightToLeft: 2,
		},
		{
			name:                     "50 rows - 50 rows, 1 to 1 join, random",
			joinRows:                 []string{"join_row"},
			leftRowCount:             50,
			leftColCount:             4,
			rightRowCount:            50,
			rightColCount:            12,
			joinColOrdering:          random,
			relationScaleLeftToRight: 1,
			relationScaleRightToLeft: 1,
		},
		{
			name:                     "50 rows - 50 rows, 1 to 1 join, 2 join rows, random",
			joinRows:                 []string{"join_row1", "join_row2"},
			leftRowCount:             50,
			leftColCount:             4,
			rightRowCount:            50,
			rightColCount:            12,
			joinColOrdering:          random,
			relationScaleLeftToRight: 1,
			relationScaleRightToLeft: 1,
		},
		{
			name:                     "100 rows - 100 rows, 1 to 1 join, random",
			joinRows:                 []string{"join_row"},
			leftRowCount:             100,
			leftColCount:             4,
			rightRowCount:            100,
			rightColCount:            12,
			joinColOrdering:          random,
			relationScaleLeftToRight: 1,
			relationScaleRightToLeft: 1,
		},
		{
			name:                     "300 rows - 300 rows, 1 to 1 join, random",
			joinRows:                 []string{"join_row"},
			leftRowCount:             300,
			leftColCount:             4,
			rightRowCount:            300,
			rightColCount:            12,
			joinColOrdering:          random,
			relationScaleLeftToRight: 1,
			relationScaleRightToLeft: 1,
		},
		{
			name:                     "300 rows - 300 rows, 1 to 1 join, 2 join rows, random",
			joinRows:                 []string{"join_row1", "join_row2"},
			leftRowCount:             300,
			leftColCount:             4,
			rightRowCount:            300,
			rightColCount:            12,
			joinColOrdering:          random,
			relationScaleLeftToRight: 1,
			relationScaleRightToLeft: 1,
		},
		{
			name:                     "900 rows - 900 rows, 1 to 1 join, 2 join rows, random",
			joinRows:                 []string{"join_row1", "join_row2"},
			leftRowCount:             900,
			leftColCount:             4,
			rightRowCount:            900,
			rightColCount:            12,
			joinColOrdering:          random,
			relationScaleLeftToRight: 1,
			relationScaleRightToLeft: 1,
		},
		{
			name:                     "1000 rows - 1000 rows, 1 to 1 join, 2 join rows, random",
			joinRows:                 []string{"join_row1", "join_row2"},
			leftRowCount:             1000,
			leftColCount:             4,
			rightRowCount:            1000,
			rightColCount:            12,
			joinColOrdering:          random,
			relationScaleLeftToRight: 1,
			relationScaleRightToLeft: 1,
		},
	}
	for _, t := range table {
		b.StopTimer()
		leftCols := generateSeriesRandomType(t.leftRowCount, t.leftColCount)
		rightCols := generateSeriesRandomType(t.rightRowCount, t.rightColCount)

		for _, joinKey := range t.joinRows {
			leftJoinSeries, rightJoinSeries := generateJoinSeries(joinKey, t.leftRowCount, t.rightRowCount, t.relationScaleLeftToRight, t.relationScaleRightToLeft, t.joinColOrdering)
			leftCols = append(leftCols, leftJoinSeries)
			rightCols = append(rightCols, rightJoinSeries)
		}
		leftTbl := dataframe.New(leftCols...)
		rightTbl := dataframe.New(rightCols...)

		b.StartTimer()
		b.Run(t.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = leftTbl.RightJoin(rightTbl, t.joinRows...)
			}
		})
	}
}

func BenchmarkDataFrame_OuterJoinOptimized(b *testing.B) {
	table := []struct {
		name                     string
		joinRows                 []string
		leftRowCount             int
		leftColCount             int
		rightRowCount            int
		rightColCount            int
		joinColOrdering          joinColRowOrdering
		relationScaleLeftToRight int
		relationScaleRightToLeft int
	}{
		{
			name:                     "10 rows - 10 rows, 1 to 1 join, 2 rows, random",
			joinRows:                 []string{"join_row1", "join_row2"},
			leftRowCount:             10,
			leftColCount:             4,
			rightRowCount:            10,
			rightColCount:            12,
			joinColOrdering:          random,
			relationScaleLeftToRight: 1,
			relationScaleRightToLeft: 1,
		},
		{
			name:                     "10 rows - 10 rows, 1 to 2 join, 2 rows, random",
			joinRows:                 []string{"join_row1", "join_row2"},
			leftRowCount:             10,
			leftColCount:             4,
			rightRowCount:            10,
			rightColCount:            12,
			joinColOrdering:          random,
			relationScaleLeftToRight: 1,
			relationScaleRightToLeft: 2,
		},
		{
			name:                     "50 rows - 50 rows, 1 to 1 join, random",
			joinRows:                 []string{"join_row"},
			leftRowCount:             50,
			leftColCount:             4,
			rightRowCount:            50,
			rightColCount:            12,
			joinColOrdering:          random,
			relationScaleLeftToRight: 1,
			relationScaleRightToLeft: 1,
		},
		{
			name:                     "50 rows - 50 rows, 1 to 1 join, 2 join rows, random",
			joinRows:                 []string{"join_row1", "join_row2"},
			leftRowCount:             50,
			leftColCount:             4,
			rightRowCount:            50,
			rightColCount:            12,
			joinColOrdering:          random,
			relationScaleLeftToRight: 1,
			relationScaleRightToLeft: 1,
		},
		{
			name:                     "100 rows - 100 rows, 1 to 1 join, random",
			joinRows:                 []string{"join_row"},
			leftRowCount:             100,
			leftColCount:             4,
			rightRowCount:            100,
			rightColCount:            12,
			joinColOrdering:          random,
			relationScaleLeftToRight: 1,
			relationScaleRightToLeft: 1,
		},
		{
			name:                     "300 rows - 300 rows, 1 to 1 join, random",
			joinRows:                 []string{"join_row"},
			leftRowCount:             300,
			leftColCount:             4,
			rightRowCount:            300,
			rightColCount:            12,
			joinColOrdering:          random,
			relationScaleLeftToRight: 1,
			relationScaleRightToLeft: 1,
		},
		{
			name:                     "300 rows - 300 rows, 1 to 1 join, 2 join rows, random",
			joinRows:                 []string{"join_row1", "join_row2"},
			leftRowCount:             300,
			leftColCount:             4,
			rightRowCount:            300,
			rightColCount:            12,
			joinColOrdering:          random,
			relationScaleLeftToRight: 1,
			relationScaleRightToLeft: 1,
		},
		{
			name:                     "900 rows - 900 rows, 1 to 1 join, 2 join rows, random",
			joinRows:                 []string{"join_row1", "join_row2"},
			leftRowCount:             900,
			leftColCount:             4,
			rightRowCount:            900,
			rightColCount:            12,
			joinColOrdering:          random,
			relationScaleLeftToRight: 1,
			relationScaleRightToLeft: 1,
		},
		{
			name:                     "1000 rows - 1000 rows, 1 to 1 join, 2 join rows, random",
			joinRows:                 []string{"join_row1", "join_row2"},
			leftRowCount:             1000,
			leftColCount:             4,
			rightRowCount:            1000,
			rightColCount:            12,
			joinColOrdering:          random,
			relationScaleLeftToRight: 1,
			relationScaleRightToLeft: 1,
		},
	}
	for _, t := range table {
		b.StopTimer()
		leftCols := generateSeriesRandomType(t.leftRowCount, t.leftColCount)
		rightCols := generateSeriesRandomType(t.rightRowCount, t.rightColCount)

		for _, joinKey := range t.joinRows {
			leftJoinSeries, rightJoinSeries := generateJoinSeries(joinKey, t.leftRowCount, t.rightRowCount, t.relationScaleLeftToRight, t.relationScaleRightToLeft, t.joinColOrdering)
			leftCols = append(leftCols, leftJoinSeries)
			rightCols = append(rightCols, rightJoinSeries)
		}
		leftTbl := dataframe.New(leftCols...)
		rightTbl := dataframe.New(rightCols...)

		b.StartTimer()
		b.Run(t.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = leftTbl.OuterJoin(rightTbl, t.joinRows...)
			}
		})
	}
}
