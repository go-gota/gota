package dataframe

import (
	"io"
)

//Reader interface for reading from a io.Reader
type Reader interface {
	Read(r io.Reader, options ...LoadOption) DataFrame
}
