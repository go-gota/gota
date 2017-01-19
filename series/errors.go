package series

type seriesError struct {
	err string
}

func (s seriesError) Error() string {
	return s.err
}

// Package wide errors
var (
	ErrEmptyInput             = seriesError{"Input must not be empty"}
	ErrAllNA                  = seriesError{"All Elemements are NA"}
	ErrNotMeaningfulForString = seriesError{"Not meaningful for String"}
	ErrSizeDiffer             = seriesError{"Size of Series differes"}
)
