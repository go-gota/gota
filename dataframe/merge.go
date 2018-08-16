package dataframe

import "github.com/isuruceanu/gota/series"

// Merge struct definition
type Merge struct {
	a           DataFrame
	b           DataFrame
	keys        []string
	combine     bool
	sameSerieFn func(aSerie, bSerie series.Series) bool
}

// Merge returns a Merge struct for containing ifo about merge
func (df DataFrame) Merge(b DataFrame, keys ...string) Merge {
	return Merge{a: df, b: b, keys: keys}
}

// WithCombine specify to merge same columns into one
func (m Merge) WithCombine(fn func(aSerie, bSerie series.Series) bool) Merge {
	m.combine = true
	m.sameSerieFn = fn
	return m
}

func (m Merge) InnerJoin() DataFrame {
	if !m.combine {
		return m.a.InnerJoin(m.b, m.keys...)
	}

	return m.a
}
