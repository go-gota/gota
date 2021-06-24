package dataframe

import (
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/go-gota/gota/series"
)

func TestMath(t *testing.T) {
	/*  Input is a 5x4 DataFrame

	   Strings  Floats   Primes Naturals
	0: e        2.718000 1      1
	1: Pi       3.142000 3      2
	2: Phi      1.618000 5      3
	3: Sqrt2    1.414000 7      4
	4: Ln2      0.693000 11     5
	   <string> <float>  <int>  <int>
	*/
	input := New(
		series.New([]string{"e", "Pi", "Phi", "Sqrt2", "Ln2"}, series.String, "Strings"),
		series.New([]float64{2.718, 3.142, 1.618, 1.414, 0.693}, series.Float, "Floats"),
		series.New([]int{1, 3, 5, 7, 11}, series.Int, "Primes"),
		series.New([]int{1, 2, 3, 4, 5}, series.Int, "Naturals"),
	)

	table := testTable{
		// Sums
		{
			fut: func(df DataFrame) DataFrame {
				df = df.Math("Sum", "+", "Floats", "Primes")
				return df
			},
			selection: []string{"Sum"},
			expected: New(
				series.New([]float64{3.718, 6.142, 6.618, 8.414, 11.693}, series.Float, "Sum"),
			),
		},
		{
			fut: func(df DataFrame) DataFrame {
				df = df.Math("IntSum", "+", "Primes", "Naturals")
				return df
			},
			selection: []string{"IntSum"},
			expected: New(
				series.New([]int{2, 5, 8, 11, 16}, series.Int, "IntSum"),
			),
		},

		// Differences
		{
			fut: func(df DataFrame) DataFrame {
				df = df.Math("Difference", "-", "Floats", "Primes")
				return df
			},
			selection: []string{"Difference"},
			expected: New(
				series.New([]float64{1.718000, 0.142000, -3.382000, -5.586000, -10.307000}, series.Float, "Difference"),
			),
		},
		{
			fut: func(df DataFrame) DataFrame {
				df = df.Math("IntDifference", "-", "Primes", "Naturals")
				return df
			},
			selection: []string{"IntDifference"},
			expected: New(
				series.New([]int{0, 1, 2, 3, 6}, series.Int, "IntDifference"),
			),
		},

		// Products
		{
			fut: func(df DataFrame) DataFrame {
				df = df.Math("Product", "*", "Floats", "Primes")
				return df
			},
			selection: []string{"Product"},
			expected: New(
				series.New([]float64{2.718000, 9.426000, 8.090000, 9.898000, 7.623000}, series.Float, "Product"),
			),
		},
		{
			fut: func(df DataFrame) DataFrame {
				df = df.Math("IntProduct", "*", "Primes", "Naturals")
				return df
			},
			selection: []string{"IntProduct"},
			expected: New(
				series.New([]int{1, 6, 15, 28, 55}, series.Int, "IntProduct"),
			),
		},

		// Quotients
		{
			fut: func(df DataFrame) DataFrame {
				df = df.Math("Quotient", "/", "Floats", "Primes")
				return df
			},
			selection: []string{"Quotient"},
			expected: New(
				series.New([]float64{2.718000, 1.047333, 0.323600, 0.202000, 0.063000}, series.Float, "Quotient"),
			),
		},
		{
			fut: func(df DataFrame) DataFrame {
				df = df.Math("IntQuotient", "/", "Primes", "Naturals")
				return df
			},
			selection: []string{"IntQuotient"},
			expected: New(
				series.New([]int{1, 1, 1, 1, 2}, series.Int, "IntQuotient"),
			),
		},
		{
			fut: func(df DataFrame) DataFrame {
				df = df.Math("Modulo", "%", "Primes", "Naturals")
				return df
			},
			selection: []string{"Modulo"},
			expected: New(
				series.New([]int{0, 1, 2, 3, 1}, series.Int, "Modulo"),
			),
		},
		{
			fut: func(df DataFrame) DataFrame {
				df = df.Math("ModuloSelf", "%", "Primes", "Primes")
				return df
			},
			selection: []string{"ModuloSelf"},
			expected: New(
				series.New([]int{0, 0, 0, 0, 0}, series.Int, "ModuloSelf"),
			),
		},

		// >2 operands
		{
			fut: func(df DataFrame) DataFrame {
				df = df.Math("MultiSum", "+", "Floats", "Floats", "Primes", "Primes")
				return df
			},
			selection: []string{"MultiSum"},
			expected: New(
				series.New([]float64{7.436000, 12.284000, 13.236000, 16.828000, 23.386000}, series.Float, "MultiSum"),
			),
		},
		{
			fut: func(df DataFrame) DataFrame {
				df = df.Math("MultiDifference", "-", "Floats", "Floats", "Primes", "Primes")
				return df
			},
			selection: []string{"MultiDifference"},
			expected: New(
				series.New([]float64{-2.000000, -6.000000, -10.000000, -14.000000, -22.000000}, series.Float, "MultiDifference"),
			),
		},
		{
			fut: func(df DataFrame) DataFrame {
				df = df.Math("MultiProduct", "*", "Floats", "Floats", "Primes", "Primes")
				return df
			},
			selection: []string{"MultiProduct"},
			expected: New(
				series.New([]float64{7.387524, 88.849476, 65.448100, 97.970404, 58.110129}, series.Float, "MultiProduct"),
			),
		},
		{
			fut: func(df DataFrame) DataFrame {
				df = df.Math("MultiQuotient", "/", "Floats", "Floats", "Primes", "Primes")
				return df
			},
			selection: []string{"MultiQuotient"},
			expected: New(
				series.New([]float64{1.000000, 0.111111, 0.040000, 0.020408, 0.008264}, series.Float, "MultiQuotient"),
			),
		},

		// Arbitrary float functions
		{
			fut: func(df DataFrame) DataFrame {
				df = df.Math("UnaryFloatFunc", math.Cos, "Floats")
				return df
			},
			selection: []string{"UnaryFloatFunc"},
			expected: New(
				series.New([]float64{-0.911618, -1.000000, -0.047186, 0.156155, 0.769333}, series.Float, "UnaryFloatFunc"),
			),
		},
		{
			fut: func(df DataFrame) DataFrame {
				df = df.Math("BinaryFloatFunc", math.Hypot, "Floats", "Floats")
				return df
			},
			selection: []string{"BinaryFloatFunc"},
			expected: New(
				series.New([]float64{3.843832, 4.443459, 2.288198, 1.999698, 0.980050}, series.Float, "BinaryFloatFunc"),
			),
		},
		{
			fut: func(df DataFrame) DataFrame {
				df = df.Math("TrinaryFloatFunc", math.FMA, "Floats", "Floats", "Floats")
				return df
			},
			selection: []string{"TrinaryFloatFunc"},
			expected: New(
				series.New([]float64{10.105524, 13.014164, 4.235924, 3.413396, 1.173249}, series.Float, "TrinaryFloatFunc"),
			),
		},

		// Arbitrary int functions
		{
			fut: func(df DataFrame) DataFrame {
				df = df.Math("UnaryIntFunc", func(i int) int { return i*2 + 1 }, "Primes")
				return df
			},
			selection: []string{"UnaryIntFunc"},
			expected: New(
				series.New([]int{3, 7, 11, 15, 23}, series.Int, "UnaryIntFunc"),
			),
		},
		{
			fut: func(df DataFrame) DataFrame {
				df = df.Math("BinaryIntFunc", func(x, y int) int { return x * y }, "Naturals", "Primes")
				return df
			},
			selection: []string{"BinaryIntFunc"},
			expected: New(
				series.New([]int{1, 6, 15, 28, 55}, series.Int, "BinaryIntFunc"),
			),
		},
		{
			fut: func(df DataFrame) DataFrame {
				df = df.Math(
					"TrinaryIntFunc",
					func(x, y, z int) int { return x * y * z },
					"Naturals", "Naturals", "Primes")
				return df
			},
			selection: []string{"TrinaryIntFunc"},
			expected: New(
				series.New([]int{1, 12, 45, 112, 275}, series.Int, "TrinaryIntFunc"),
			),
		},
	}

	runTestTable(table, input, t)

}

func TestMathErrors(t *testing.T) {
	expectError("at least one operand", func(df DataFrame) DataFrame {
		return df.Math("Empty operands", "+")
	}, t)

	expectError("cannot perform arithmetic with column of type string", func(df DataFrame) DataFrame {
		return df.Math("Non-numeric type", "+", "Strings")
	}, t)

	expectError("unknown arithmetic operator", func(df DataFrame) DataFrame {
		return df.Math("unknown operator", "!", "Primes")
	}, t)

	expectError("integer divide by zero", func(df DataFrame) DataFrame {
		return df.Math("Divide by zero", "/", "Primes", "Naturals0")
	}, t)

	// reciprocal
	expectError("integer divide by zero", func(df DataFrame) DataFrame {
		return df.Math("Divide by zero", "/", "Naturals0")
	}, t)

	// modulo 0
	expectError("integer divide by zero", func(df DataFrame) DataFrame {
		return df.Math("Divide by zero", "%", "Primes", "Naturals0")
	}, t)

	// catch panic on unknown op
}

// Test helpers

type testTable []struct {
	fut       func(DataFrame) DataFrame
	selection interface{}
	expected  DataFrame
}

func runTestTable(table testTable, input DataFrame, t *testing.T) {

	for tidx, test := range table {
		observed := test.fut(input).Select(test.selection)

		// Check that the types are the same between both DataFrames
		if !reflect.DeepEqual(test.expected.Types(), observed.Types()) {
			t.Errorf("Test: %d\nDifferent types:\nA:%v\nB:%v", tidx, test.expected.Types(), observed.Types())
		}
		// Check that the colnames are the same between both DataFrames
		if !reflect.DeepEqual(test.expected.Names(), observed.Names()) {
			t.Errorf("Test: %d\nDifferent colnames:\nA:%v\nB:%v", tidx, test.expected.Names(), observed.Names())
		}
		// Check that the values are the same between both DataFrames
		if !reflect.DeepEqual(test.expected.Records(), observed.Records()) {
			t.Fatalf("Test: %d\nDifferent values:\nExpected:%v\nObserved:%v", tidx, test.expected.Records(), observed.Records())
		}
	}
}

func expectError(message string, fut func(DataFrame) DataFrame, t *testing.T) {
	df := New(
		series.New([]string{"e", "Pi", "Phi", "Sqrt2", "Ln2"}, series.String, "Strings"),
		series.New([]float64{2.718, 3.142, 1.618, 1.414, 0.693}, series.Float, "Floats"),
		series.New([]int{1, 3, 5, 7, 11}, series.Int, "Primes"),
		series.New([]int{0, 1, 2, 3, 4}, series.Int, "Naturals0"),
	)
	df = fut(df)
	if !strings.Contains(df.Err.Error(), message) {
		t.Fatalf("expected error to contain '%s', but got %v", message, df.Err)
	}
}
