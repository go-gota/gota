package dataframe

import (
	"fmt"
	"testing"

	"github.com/go-gota/gota/series"
)

var df DataFrame = New(
	series.New([]string{"e", "Pi", "Phi", "Sqrt2", "Ln2"}, series.String, "Strings"),
	series.New([]float64{2.718, 3.142, 1.618, 1.414, 0.693}, series.Float, "Floats"),
	series.New([]int{1, 3, 5, 7, 11}, series.Int, "Ints"),
)

func TestFloatOps(t *testing.T) {
	fmt.Println(df)
	t.Fail()
}

// Test cast to float
