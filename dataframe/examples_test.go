package dataframe_test

import (
	"fmt"
	"math"
	"strings"

	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
)

func ExampleNew() {
	df := dataframe.New(
		series.New([]string{"b", "a"}, series.String, "COL.1"),
		series.New([]int{1, 2}, series.Int, "COL.2"),
		series.New([]float64{3.0, 4.0}, series.Float, "COL.3"),
	)
	fmt.Println(df)
}

func ExampleLoadStructs() {
	type User struct {
		Name     string
		Age      int
		Accuracy float64
	}
	users := []User{
		{"Aram", 17, 0.2},
		{"Juan", 18, 0.8},
		{"Ana", 22, 0.5},
	}
	df := dataframe.LoadStructs(users)
	fmt.Println(df)
}

func ExampleLoadRecords() {
	df := dataframe.LoadRecords(
		[][]string{
			{"A", "B", "C", "D"},
			{"a", "4", "5.1", "true"},
			{"k", "5", "7.0", "true"},
			{"k", "4", "6.0", "true"},
			{"a", "2", "7.1", "false"},
		},
	)
	fmt.Println(df)
}

func ExampleLoadRecords_options() {
	df := dataframe.LoadRecords(
		[][]string{
			{"A", "B", "C", "D"},
			{"a", "4", "5.1", "true"},
			{"k", "5", "7.0", "true"},
			{"k", "4", "6.0", "true"},
			{"a", "2", "7.1", "false"},
		},
		dataframe.DetectTypes(false),
		dataframe.DefaultType(series.Float),
		dataframe.WithTypes(map[string]series.Type{
			"A": series.String,
			"D": series.Bool,
		}),
	)
	fmt.Println(df)
}

func ExampleLoadMaps() {
	df := dataframe.LoadMaps(
		[]map[string]interface{}{
			{
				"A": "a",
				"B": 1,
				"C": true,
				"D": 0,
			},
			{
				"A": "b",
				"B": 2,
				"C": true,
				"D": 0.5,
			},
		},
	)
	fmt.Println(df)
}

func ExampleReadCSV() {
	csvStr := `
Country,Date,Age,Amount,Id
"United States",2012-02-01,50,112.1,01234
"United States",2012-02-01,32,321.31,54320
"United Kingdom",2012-02-01,17,18.2,12345
"United States",2012-02-01,32,321.31,54320
"United Kingdom",2012-02-01,NA,18.2,12345
"United States",2012-02-01,32,321.31,54320
"United States",2012-02-01,32,321.31,54320
Spain,2012-02-01,66,555.42,00241
`
	df := dataframe.ReadCSV(strings.NewReader(csvStr))
	fmt.Println(df)
}

func ExampleReadJSON() {
	jsonStr := `[{"COL.2":1,"COL.3":3},{"COL.1":5,"COL.2":2,"COL.3":2},{"COL.1":6,"COL.2":3,"COL.3":1}]`
	df := dataframe.ReadJSON(strings.NewReader(jsonStr))
	fmt.Println(df)
}

func ExampleDataFrame_Subset() {
	df := dataframe.LoadRecords(
		[][]string{
			{"A", "B", "C", "D"},
			{"a", "4", "5.1", "true"},
			{"k", "5", "7.0", "true"},
			{"k", "4", "6.0", "true"},
			{"a", "2", "7.1", "false"},
		},
	)
	sub := df.Subset([]int{0, 2})
	fmt.Println(sub)
}

func ExampleDataFrame_Select() {
	df := dataframe.LoadRecords(
		[][]string{
			{"A", "B", "C", "D"},
			{"a", "4", "5.1", "true"},
			{"k", "5", "7.0", "true"},
			{"k", "4", "6.0", "true"},
			{"a", "2", "7.1", "false"},
		},
	)
	sel1 := df.Select([]int{0, 2})
	sel2 := df.Select([]string{"A", "C"})
	fmt.Println(sel1)
	fmt.Println(sel2)
}

func ExampleDataFrame_Filter() {
	df := dataframe.LoadRecords(
		[][]string{
			{"A", "B", "C", "D"},
			{"a", "4", "5.1", "true"},
			{"k", "5", "7.0", "true"},
			{"k", "4", "6.0", "true"},
			{"a", "2", "7.1", "false"},
		},
	)
	fil := df.Filter(
		dataframe.F{
			Colname:    "A",
			Comparator: series.Eq,
			Comparando: "a",
		},
		dataframe.F{
			Colname:    "B",
			Comparator: series.Greater,
			Comparando: 4,
		},
	)
	fil2 := fil.Filter(
		dataframe.F{
			Colname:    "D",
			Comparator: series.Eq,
			Comparando: true,
		},
	)
	fmt.Println(fil)
	fmt.Println(fil2)
}

func ExampleDataFrame_Mutate() {
	df := dataframe.LoadRecords(
		[][]string{
			{"A", "B", "C", "D"},
			{"a", "4", "5.1", "true"},
			{"k", "5", "7.0", "true"},
			{"k", "4", "6.0", "true"},
			{"a", "2", "7.1", "false"},
		},
	)
	// Change column C with a new one
	mut := df.Mutate(
		series.New([]string{"a", "b", "c", "d"}, series.String, "C"),
	)
	// Add a new column E
	mut2 := df.Mutate(
		series.New([]string{"a", "b", "c", "d"}, series.String, "E"),
	)
	fmt.Println(mut)
	fmt.Println(mut2)
}

func ExampleDataFrame_InnerJoin() {
	df := dataframe.LoadRecords(
		[][]string{
			{"A", "B", "C", "D"},
			{"a", "4", "5.1", "true"},
			{"k", "5", "7.0", "true"},
			{"k", "4", "6.0", "true"},
			{"a", "2", "7.1", "false"},
		},
	)
	df2 := dataframe.LoadRecords(
		[][]string{
			{"A", "F", "D"},
			{"1", "1", "true"},
			{"4", "2", "false"},
			{"2", "8", "false"},
			{"5", "9", "false"},
		},
	)
	join := df.InnerJoin(df2, "D")
	fmt.Println(join)
}

func ExampleDataFrame_Set() {
	df := dataframe.LoadRecords(
		[][]string{
			{"A", "B", "C", "D"},
			{"a", "4", "5.1", "true"},
			{"k", "5", "7.0", "true"},
			{"k", "4", "6.0", "true"},
			{"a", "2", "7.1", "false"},
		},
	)
	df2 := df.Set(
		series.Ints([]int{0, 2}),
		dataframe.LoadRecords(
			[][]string{
				{"A", "B", "C", "D"},
				{"b", "4", "6.0", "true"},
				{"c", "3", "6.0", "false"},
			},
		),
	)
	fmt.Println(df2)
}

func ExampleDataFrame_Arrange() {
	df := dataframe.LoadRecords(
		[][]string{
			{"A", "B", "C", "D"},
			{"a", "4", "5.1", "true"},
			{"b", "4", "6.0", "true"},
			{"c", "3", "6.0", "false"},
			{"a", "2", "7.1", "false"},
		},
	)
	sorted := df.Arrange(
		dataframe.Sort("A"),
		dataframe.RevSort("B"),
	)
	fmt.Println(sorted)
}

func ExampleDataFrame_Describe() {
	df := dataframe.LoadRecords(
		[][]string{
			{"A", "B", "C", "D"},
			{"a", "4", "5.1", "true"},
			{"b", "4", "6.0", "true"},
			{"c", "3", "6.0", "false"},
			{"a", "2", "7.1", "false"},
		},
	)
	fmt.Println(df.Describe())
}

func ExampleDataFrame_FindElem() {
	df := dataframe.New(
		series.New([]string{"e", "Pi", "Phi", "Sqrt2", "Ln2"}, series.String, "Strings"),
		series.New([]int{1, 3, 5, 7, 11}, series.Int, "Ints"),
		series.New([]float64{2.718, 3.142, 1.618, 1.414, 0.693}, series.Float, "Floats"),
		series.New([]bool{false, true, false, false, false}, series.Bool, "Bools"),
	)

	if f, ok := df.FindElem("Strings", "Pi", "Floats"); ok {
		fmt.Printf("The value of Pi is %f\n", f.Float())
	}
}

func ExampleDataFrame_Math() {
	/*  `input` is a 5x4 DataFrame:

	   Strings  Floats   Primes Naturals
	0: e        2.718000 1      1
	1: Pi       3.142000 3      2
	2: Phi      1.618000 5      3
	3: Sqrt2    1.414000 7      4
	4: Ln2      0.693000 11     5
	   <string> <float>  <int>  <int>
	*/
	df := dataframe.New(
		series.New([]string{"e", "Pi", "Phi", "Sqrt2", "Ln2"}, series.String, "Strings"),
		series.New([]float64{2.718, 3.142, 1.618, 1.414, 0.693}, series.Float, "Floats"),
		series.New([]int{1, 3, 5, 7, 11}, series.Int, "Primes"),
		series.New([]int{1, 2, 3, 4, 5}, series.Int, "Naturals"),
	)

	// `Math` takes a new column name, an operator (string or func) and at least one column name
	withNewDiffColumn := df.Math("Diff", "-", "Floats", "Primes")

	// New `DataFrame` now has a column named "Diff" which is
	// the result of subtracting Primes from Floats.
	fmt.Println(withNewDiffColumn)

	/*
	      Strings  Floats   Primes Naturals Diff
	   0: e        2.718000 1      1        1.718000
	   1: Pi       3.142000 3      2        0.142000
	   2: Phi      1.618000 5      3        -3.382000
	   3: Sqrt2    1.414000 7      4        -5.586000
	   4: Ln2      0.693000 11     5        -10.307000
	      <string> <float>  <int>  <int>    <float>
	*/

	// Also supports passing unary, binary, or trinary functions of
	// int or float64, e.g., for functions from Go's `math` package.
	// (Note here that `dataframe.Math` supports specifying many
	// column names depending on the given operator, and also that
	// it automatically coerces int to float64 when `op` is a
	// function on float64.)
	withNewFMACol := df.Math("FMA", math.FMA, "Floats", "Primes", "Naturals")

	fmt.Println(withNewFMACol)
}
