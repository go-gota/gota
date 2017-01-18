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

//CsvWriter CSV Writer
type CsvWriter struct {
	w io.Writer
}

//NewCsvWriter creates new instance of CsvWriter
func NewCsvWriter(w io.Writer) CsvWriter {
	return CsvWriter{w: w}
}

func (w *CsvWriter) Write(df DataFrame) error {
	records := df.Records()
	return csv.NewWriter(w.w).WriteAll(records)
}
