package errors

import "fmt"

var (
	ErrEmptyDataFrame          = fmt.Errorf("empty DataFrame")
	ErrDimensionsDiffers       = fmt.Errorf("arguments have different dimensions")
	ErrUnknownColumn           = fmt.Errorf("unknown column name")
	ErrJoinKeysAreNotSpecified = fmt.Errorf("join keys not specified")
	ErrNoArgs                  = fmt.Errorf("no arguments")
	ErrTooManyCols             = fmt.Errorf("too many column names")
	ErrTooFewCols              = fmt.Errorf("not enough column names")
	ErrUnknownIndexingMode     = fmt.Errorf("indexing error: unknown indexing mode")
)
