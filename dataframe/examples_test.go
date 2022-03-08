package dataframe_test

import (
	"fmt"
	"strings"

	"github.com/mqy527/gota/dataframe"
	"github.com/mqy527/gota/series"
)

func ExampleNew() {
	df := dataframe.New(
		series.New([]string{"b", "a"}, series.String, "COL.1"),
		series.New([]int{1, 2}, series.Int, "COL.2"),
		series.New([]float64{3.0, 4.0}, series.Float, "COL.3"),
	)
	fmt.Println(df)

	// Output:
	// [2x3] DataFrame
	//
	//     COL.1    COL.2 COL.3
	//  0: b        1     3.000000
	//  1: a        2     4.000000
	//     <string> <int> <float>

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

	// Output:
	// [3x3] DataFrame
	//
	//     Name     Age   Accuracy
	//  0: Aram     17    0.200000
	//  1: Juan     18    0.800000
	//  2: Ana      22    0.500000
	//     <string> <int> <float>

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

	// Output:
	// [4x4] DataFrame
	//
	//     A        B     C        D
	//  0: a        4     5.100000 true
	//  1: k        5     7.000000 true
	//  2: k        4     6.000000 true
	//  3: a        2     7.100000 false
	//     <string> <int> <float>  <bool>

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

	// Output:
	// [4x4] DataFrame
	//
	//     A        B        C        D
	//  0: a        4.000000 5.100000 true
	//  1: k        5.000000 7.000000 true
	//  2: k        4.000000 6.000000 true
	//  3: a        2.000000 7.100000 false
	//     <string> <float>  <float>  <bool>

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

	// Otput:
	// [2x4] DataFrame
	//
	//     A        B     C      D
	//  0: a        1     true   0.000000
	//  1: b        2     true   0.500000
	//     <string> <int> <bool> <float>

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

	// Output:
	// [8x5] DataFrame
	//
	//     Country        Date       Age   Amount     Id
	//  0: United States  2012-02-01 50    112.100000 1234
	//  1: United States  2012-02-01 32    321.310000 54320
	//  2: United Kingdom 2012-02-01 17    18.200000  12345
	//  3: United States  2012-02-01 32    321.310000 54320
	//  4: United Kingdom 2012-02-01 NaN   18.200000  12345
	//  5: United States  2012-02-01 32    321.310000 54320
	//  6: United States  2012-02-01 32    321.310000 54320
	//  7: Spain          2012-02-01 66    555.420000 241
	//     <string>       <string>   <int> <float>    <int>

}

func ExampleReadJSON() {
	jsonStr := `[{"COL.2":1,"COL.3":3},{"COL.1":5,"COL.2":2,"COL.3":2},{"COL.1":6,"COL.2":3,"COL.3":1}]`
	df := dataframe.ReadJSON(strings.NewReader(jsonStr))
	fmt.Println(df)

	// Output:
	// [3x3] DataFrame
	//
	//     COL.1 COL.2 COL.3
	//  0: NaN   1     3
	//  1: 5     2     2
	//  2: 6     3     1
	//     <int> <int> <int>

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

	// Output:
	// [2x4] DataFrame
	//
	//     A        B     C        D
	//  0: a        4     5.100000 true
	//  1: k        4     6.000000 true
	//     <string> <int> <float>  <bool>

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

	// Output:
	// [4x2] DataFrame
	//
	//     A        C
	//  0: a        5.100000
	//  1: k        7.000000
	//  2: k        6.000000
	//  3: a        7.100000
	//     <string> <float>
	//
	// [4x2] DataFrame
	//
	//     A        C
	//  0: a        5.100000
	//  1: k        7.000000
	//  2: k        6.000000
	//  3: a        7.100000
	//     <string> <float>

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

	// Output:
	// [3x4] DataFrame
	//
	//     A        B     C        D
	//  0: a        4     5.100000 true
	//  1: k        5     7.000000 true
	//  2: a        2     7.100000 false
	//     <string> <int> <float>  <bool>
	//
	// [2x4] DataFrame
	//
	//     A        B     C        D
	//  0: a        4     5.100000 true
	//  1: k        5     7.000000 true
	//     <string> <int> <float>  <bool>

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

	// Output:
	//   [4x4] DataFrame
	//
	//     A        B     C        D
	//  0: a        4     a        true
	//  1: k        5     b        true
	//  2: k        4     c        true
	//  3: a        2     d        false
	//     <string> <int> <string> <bool>

	// [4x5] DataFrame
	//
	//     A        B     C        D      E
	//  0: a        4     5.100000 true   a
	//  1: k        5     7.000000 true   b
	//  2: k        4     6.000000 true   c
	//  3: a        2     7.100000 false  d
	//     <string> <int> <float>  <bool> <string>

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

	// Output:
	// [6x6] DataFrame
	//
	//     D      A_0      B     C        A_1   F
	//  0: true   a        4     5.100000 1     1
	//  1: true   k        5     7.000000 1     1
	//  2: true   k        4     6.000000 1     1
	//  3: false  a        2     7.100000 4     2
	//  4: false  a        2     7.100000 2     8
	//  5: false  a        2     7.100000 5     9
	//     <bool> <string> <int> <float>  <int> <int>

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

	// Output:
	// [4x4] DataFrame
	//
	//     A        B     C        D
	//  0: b        4     6.000000 true
	//  1: k        5     7.000000 true
	//  2: c        3     6.000000 false
	//  3: a        2     7.100000 false
	//     <string> <int> <float>  <bool>

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

	// Output:
	// [4x4] DataFrame
	//
	//     A        B     C        D
	//  0: a        4     5.100000 true
	//  1: a        2     7.100000 false
	//  2: b        4     6.000000 true
	//  3: c        3     6.000000 false
	//     <string> <int> <float>  <bool>

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

	// Output:
	// [8x5] DataFrame
	//
	//     column   A        B        C        D
	//  0: mean     -        3.250000 6.050000 0.500000
	//  1: median   -        3.500000 6.000000 NaN
	//  2: stddev   -        0.957427 0.818535 0.577350
	//  3: min      a        2.000000 5.100000 0.000000
	//  4: 25%      -        2.000000 5.100000 0.000000
	//  5: 50%      -        3.000000 6.000000 0.000000
	//  6: 75%      -        4.000000 6.000000 1.000000
	//  7: max      c        4.000000 7.100000 1.000000
	//     <string> <string> <float>  <float>  <float>

}
