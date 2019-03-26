package series

import "fmt"

type seriesError struct {
	err string
}

func (s seriesError) Error() string {
	return s.err
}

// Package wide errors
var (
	ErrEmptyInput             = seriesError{"Input must not be empty"}
	ErrAllNA                  = seriesError{"All Elements are NA"}
	ErrNotMeaningfulForString = seriesError{"Not meaningful for String"}
	ErrSizeDiffer             = seriesError{"Size of Series differs"}
	ErrBounds                 = seriesError{"Input is outside of range."}

	ErrSize = seriesError{"Must be the same length."}

	ErrBoundsVal = func(val interface{}) seriesError {
		return seriesError{err: fmt.Sprintf("Input %v is outside of range", val)}
	}
)
