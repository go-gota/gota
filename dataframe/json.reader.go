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
