package dataframe

import (
	"encoding/csv"
	"io"
)

//CsvReader read a csv file
type CsvReader struct {
}

func (cr CsvReader) Read(r io.Reader, options ...LoadOption) DataFrame {
	csvReader := csv.NewReader(r)
	records, err := csvReader.ReadAll()
	if err != nil {
		return DataFrame{Err: err}
	}

	return LoadRecords(records, options...)
}
