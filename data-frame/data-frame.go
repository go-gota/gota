package DataFrame

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

// DataFrame Definition
// ====================
type DataFrame struct {
	columns  map[string]Column
	colnames []string
	nCols    int
	nRows    int
}

// DataFrame Methods
// =================
func (df *DataFrame) LoadAndParse(records [][]string, types interface{}) error {
	// Initialize the DataFrame with all columns as string type
	err := df.LoadData(records)
	if err != nil {
		return err
	}

	// Parse the DataFrame columns acording to the given types
	switch types.(type) {
	case []string:
		types := types.([]string)
		if df.nCols != len(types) {
			return errors.New("Number of columns different from number of types")
		}
		for k, v := range df.colnames {
			col := df.columns[v]
			col.ParseType(types[k])
			col.colType = types[k]
			df.columns[v] = col
		}
	case map[string]string:
		types := types.(map[string]string)
		for k, v := range types {
			col := df.columns[k]
			col.ParseType(v)
			col.colType = v
			df.columns[k] = col
		}
	}

	return nil
}

func (df DataFrame) SubsetColumns(columns []string) (*DataFrame, error) {
	// Generate a df to store the temporary values
	newDf := DataFrame{
		columns:  make(map[string]Column),
		nRows:    df.nRows,
		colnames: []string{},
	}

	// Initialize variables to store possible errors
	noCols := []string{}
	dupedCols := []string{}

	// Select the desired subset of columns
	for _, v := range columns {
		if col, ok := df.columns[v]; ok {
			if _, ok := newDf.columns[v]; ok {
				dupedCols = append(dupedCols, v)
			}
			newDf.colnames = append(newDf.colnames, v)
			newDf.columns[v] = col
		} else {
			noCols = append(noCols, v)
		}
	}

	if len(dupedCols) != 0 {
		errStr := "The following columns appear more than once:\n" + strings.Join(dupedCols, ", ")
		return nil, errors.New(errStr)
	}
	if len(noCols) != 0 {
		errStr := "The following columns are not present on the DataFrame:\n" + strings.Join(noCols, ", ")
		return nil, errors.New(errStr)
	}

	newDf.nCols = len(newDf.colnames)

	return &newDf, nil
}

func (df *DataFrame) LoadData(records [][]string) error {
	// Calculate DataFrame dimensions
	nRows := len(records) - 1
	if nRows <= 0 {
		return errors.New("Empty dataframe")
	}
	nCols := len(records[0])

	// Generate a df to store the temporary values
	newDf := DataFrame{
		columns:  make(map[string]Column),
		nRows:    nRows,
		nCols:    nCols,
		colnames: records[0],
	}

	// Fill the columns on the DataFrame
	for j := 0; j < nCols; j++ {
		col := []string{}
		for i := 1; i < nRows+1; i++ {
			col = append(col, records[i][j])
		}
		column := Column{}
		column.numChars = len(records[0][j])
		column.FillColumn(col)
		newDf.columns[records[0][j]] = column
	}

	*df = newDf
	return nil
}

func (df DataFrame) String() (str string) {
	addLeftPadding := func(s string, nchar int) string {
		if len(s) < nchar {
			return strings.Repeat(" ", nchar-len(s)) + s
		}
		return s
	}
	addRightPadding := func(s string, nchar int) string {
		if len(s) < nchar {
			return s + strings.Repeat(" ", nchar-len(s))
		}
		return s
	}

	nRowsPadding := len(fmt.Sprint(df.nRows))
	if len(df.colnames) != 0 {
		str += addLeftPadding("  ", nRowsPadding+2)
		for _, v := range df.colnames {
			str += addRightPadding(v, df.columns[v].numChars)
			str += "  "
		}
		str += "\n"
		str += "\n"
	}
	for i := 0; i < df.nRows; i++ {
		str += addLeftPadding(strconv.Itoa(i+1)+": ", nRowsPadding+2)
		for _, v := range df.colnames {
			str += addRightPadding(fmt.Sprint(df.columns[v].row[i]), df.columns[v].numChars)
			str += "  "
		}
		str += "\n"
	}
	return str
}

// Column Definition
// =================
type Column struct {
	row      []interface{}
	colType  string
	numChars int
}

// Column Methods
// ==============
func (c Column) String() string {
	return fmt.Sprint(c.row)
}

func (c *Column) ParseType(t string) error {
	var newRows interface{}
	switch t {
	case "int":
		newRows = []int{}
	case "float":
		newRows = []float64{}
	case "string":
		newRows = []string{}
	}
	for _, v := range c.row {
		r := fmt.Sprint(v)
		switch t {
		case "int":
			i, err := strconv.Atoi(r)
			if err != nil {
				return err
			}
			newRows = append(newRows.([]int), i)
		case "float":
			i, err := strconv.ParseFloat(r, 64)
			if err != nil {
				return err
			}
			newRows = append(newRows.([]float64), i)
		case "string":
			newRows = append(newRows.([]string), r)
		}
	}
	c.FillColumn(newRows)
	return nil
}

// Use reflection to fill the column with the given values
func (c *Column) FillColumn(values interface{}) {
	switch reflect.TypeOf(values).Kind() {
	case reflect.Slice:
		s := reflect.ValueOf(values)
		c.row = make([]interface{}, 0)
		for i := 0; i < s.Len(); i++ {
			c.row = append(c.row, s.Index(i).Interface())
			c.colType = fmt.Sprint(s.Index(i).Type())
			rowStr := fmt.Sprint(s.Index(i).Interface())
			if len(rowStr) > c.numChars {
				c.numChars = len(rowStr)
			}
		}
	}
}
