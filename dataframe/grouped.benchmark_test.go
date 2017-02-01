package dataframe

import (
	"os"
	"testing"

	"github.com/isuruceanu/gota/series"
)

func BenchmarkSummarize(b *testing.B) {
	df := readCvsFile("testData.csv")

	b.Run("Summarize on 3100 x 8 df group by Gender and Department", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			df.Group("Gender", "Department").Summarize(calcStat("WageHour"))
		}
	})
}

func BenchmarkParseInternal(b *testing.B) {
	df := readCvsFile("testData.csv")

	b.Run("parseInternal", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			df.Group("Gender", "Department").parseInternal()
		}
	})
}

func readCvsFile(file string) (df DataFrame) {

	if csvfile, err := os.Open(file); err != nil {
		return DataFrame{Err: err}
	} else {
		defer csvfile.Close()
		return ReadCSV(csvfile)
	}

}

func calcStat(hexoColumn string) func(DataFrame) series.Series {
	f := func(df DataFrame) series.Series {
		wage := df.Col(hexoColumn)
		count := float64(wage.Len())
		mean := wage.Mean()
		median := wage.Median()
		max, _ := series.Max(wage)
		min, _ := series.Min(wage)

		return series.Floats([]float64{count, mean, median, max.Float(), min.Float()})
	}
	return f
}
