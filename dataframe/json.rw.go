package dataframe

import (
	"encoding/json"
	"io"
)

//JSONReader read a JSON to DataFrame
type JSONReader struct {
}

func (jr JSONReader) Read(r io.Reader, options ...LoadOption) DataFrame {
	var m []map[string]interface{}
	err := json.NewDecoder(r).Decode(&m)
	if err != nil {
		return DataFrame{Err: err}
	}
	return LoadMaps(m, options...)
}

//JSONWriter JSON Writer definition
type JSONWriter struct {
	w io.Writer
}

//MakeJSONWriter creates a new instance of JSONWriter
func MakeJSONWriter(w io.Writer) JSONWriter {
	return JSONWriter{w: w}
}

func (w *JSONWriter) Write(df DataFrame) error {
	m := df.Maps()
	return json.NewEncoder(w.w).Encode(m)
}
