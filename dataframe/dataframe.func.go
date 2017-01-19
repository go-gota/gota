package dataframe

import "github.com/isuruceanu/gota/series"

//Summarize runs a series of functions on a column and returns result in new DataFrame
func (df DataFrame) Summarize(colname string) func(...func(series.Series) (series.Element, error)) DataFrame {
	serie := df.Col(colname)

	return func(funcs ...func(series.Series) (series.Element, error)) DataFrame {
		columns := make([]series.Series, len(funcs))
		for idx, f := range funcs {
			if r, e := f(serie); e != nil {
				return DataFrame{Err: e}
			} else {
				columns[idx] = series.New(r, series.Float, "")
			}
		}

		return New(columns...)
	}
}

//Levels gets groups
func (df DataFrame) Levels(col string) map[string]int {
	serie := df.Col(col)
	result := make(map[string]int)

	for _, v := range serie.Records() {
		result[v]++
	}

	return result
}
