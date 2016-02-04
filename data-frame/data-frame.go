package df

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// NOTE: The concept of NA is represented by nil pointers

// ----------------------------------------------------------------------
// Type Definitions
// ----------------------------------------------------------------------

// DataFrame is the base data structure
type DataFrame struct {
	Columns  Columns
	colNames []string
	nCols    int
	nRows    int
}

// Column is a column inside a DataFrame
type Column struct {
	row      []interface{}
	colType  string
	colName  string
	numChars int
}

// Subset represent a distance from a number to another
type Subset struct {
	From int
	To   int
}

// Columns is an alias for multiple columns
type Columns map[string]Column

// T is used to represent the association between a column and it't type
type T map[string]string

// ----------------------------------------------------------------------
// Constant definitions
// ----------------------------------------------------------------------

const defaultDateFormat = "2006-01-02"

// ----------------------------------------------------------------------
// DataFrame methods
// ----------------------------------------------------------------------

// LoadData will load the data from a multidimensional array of strings into
// a DataFrame object.
func (df *DataFrame) LoadData(records [][]string) error {
	// Calculate DataFrame dimensions
	nRows := len(records) - 1
	if nRows <= 0 {
		return errors.New("Empty dataframe")
	}
	colnames := records[0]
	nCols := len(colnames)

	// If colNames has empty elements we must fill it with unique colnames
	colnamesMap := make(map[string]bool)
	auxCounter := 0
	// Get unique columnenames
	for _, v := range colnames {
		if v != "" {
			if _, ok := colnamesMap[v]; !ok {
				colnamesMap[v] = true
			} else {
				return errors.New("Duplicated column names: " + v)
			}
		}
	}
	for k, v := range colnames {
		if v == "" {
			for {
				newColname := fmt.Sprint("V", auxCounter)
				auxCounter++
				if _, ok := colnamesMap[newColname]; !ok {
					colnames[k] = newColname
					colnamesMap[newColname] = true
					break
				}
			}
		}
	}

	// Generate a df to store the temporary values
	newDf := DataFrame{
		Columns:  make(map[string]Column),
		nRows:    nRows,
		nCols:    nCols,
		colNames: colnames,
	}

	// Fill the columns on the DataFrame
	for j := 0; j < nCols; j++ {
		col := []string{}
		for i := 1; i < nRows+1; i++ {
			col = append(col, records[i][j])
		}
		colName := colnames[j]
		column := Column{}
		column.colName = colName
		column.numChars = len(colName)
		column.FillColumn(col)
		newDf.Columns[colName] = column
	}

	*df = newDf
	return nil
}

// LoadAndParse will load the data from a multidimensional array of strings and
// parse it accordingly with the given types element. The types element can be
// a string array with matching dimensions to the number of columns or
// a DataFrame.T object.
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
		for k, v := range df.colNames {
			col := df.Columns[v]
			err := col.ParseType(types[k])
			if err != nil {
				return err
			}
			col.colType = types[k]
			df.Columns[v] = col
		}
	case T:
		types := types.(T)
		for k, v := range types {
			col := df.Columns[k]
			err := col.ParseType(v)
			if err != nil {
				return err
			}
			col.colType = v
			df.Columns[k] = col
		}
	}

	return nil
}

// SubsetColumns will return a DataFrame that contains only the columns named
// after the given columns.
func (df DataFrame) SubsetColumns(subset interface{}) (*DataFrame, error) {
	// Generate a DataFrame to store the temporary values
	newDf := DataFrame{
		Columns:  make(map[string]Column),
		nRows:    df.nRows,
		colNames: []string{},
	}

	switch subset.(type) {
	case Subset:
		s := subset.(Subset)
		// Check for errors
		if s.From > s.To {
			return nil, errors.New("Bad subset: Start greater than Beginning")
		}
		if s.From == s.To {
			return nil, errors.New("Empty subset")
		}
		if s.To > df.nCols || s.To < 0 || s.From < 0 {
			return nil, errors.New("Subset out of range")
		}

		newDf.nCols = s.To - s.From
		newDf.colNames = df.colNames[s.From:s.To]
		for _, v := range df.colNames[s.From:s.To] {
			col := df.Columns[v]
			newDf.Columns[v] = col
		}
	case []string:
		columns := subset.([]string)
		// Initialize variables to store possible errors
		noCols := []string{}
		dupedCols := []string{}

		// Select the desired subset of columns
		for _, v := range columns {
			if col, ok := df.Columns[v]; ok {
				if _, ok := newDf.Columns[v]; ok {
					dupedCols = append(dupedCols, v)
				}
				newDf.colNames = append(newDf.colNames, v)
				newDf.Columns[v] = col
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
	}

	newDf.nCols = len(newDf.colNames)

	return &newDf, nil
}

// SubsetRows will return a DataFrame that contains only the selected rows
func (df DataFrame) SubsetRows(subset interface{}) (*DataFrame, error) {
	// Generate a DataFrame to store the temporary values
	newDf := DataFrame{
		Columns:  make(map[string]Column),
		nCols:    df.nCols,
		colNames: df.colNames,
	}

	switch subset.(type) {
	case []int:
		rowNums := subset.([]int)

		// Check for errors
		for _, v := range rowNums {
			if v >= df.nRows || v < 0 {
				return nil, errors.New("Subset out of range")
			}
		}

		newDf.nRows = len(rowNums)
		for _, v := range df.colNames {
			col := df.Columns[v]
			var row []interface{}
			for _, v := range rowNums {
				row = append(row, col.row[v])
			}
			col.FillColumn(row)
			newDf.Columns[v] = col
		}
	case Subset:
		s := subset.(Subset)
		// Check for errors
		if s.From > s.To {
			return nil, errors.New("Bad subset: Start greater than Beginning")
		}
		if s.From == s.To {
			return nil, errors.New("Empty subset")
		}
		if s.To > df.nRows || s.To < 0 || s.From < 0 {
			return nil, errors.New("Subset out of range")
		}

		newDf.nRows = s.To - s.From
		for _, v := range df.colNames {
			col := df.Columns[v]
			col.FillColumn(col.row[s.From:s.To])
			newDf.Columns[v] = col
		}
	}

	return &newDf, nil
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
	if len(df.colNames) != 0 {
		str += addLeftPadding("  ", nRowsPadding+2)
		for _, v := range df.colNames {
			str += addRightPadding(v, df.Columns[v].numChars)
			str += "  "
		}
		str += "\n"
		str += "\n"
	}
	for i := 0; i < df.nRows; i++ {
		str += addLeftPadding(strconv.Itoa(i)+": ", nRowsPadding+2)
		for _, v := range df.colNames {
			switch df.Columns[v].colType {
			case "int":
				s := df.Columns[v].row[i].(*int)
				if s != nil {
					str += addRightPadding(fmt.Sprint(*s), df.Columns[v].numChars)
				} else {
					str += addRightPadding("NA", df.Columns[v].numChars)
				}
			case "float64":
				s := df.Columns[v].row[i].(*float64)
				if s != nil {
					str += addRightPadding(fmt.Sprint(*s), df.Columns[v].numChars)
				} else {
					str += addRightPadding("NA", df.Columns[v].numChars)
				}
			case "date":
				s := df.Columns[v].row[i].(*time.Time)
				if s != nil {
					str += addRightPadding(fmt.Sprint(*s), df.Columns[v].numChars)
				} else {
					str += addRightPadding("NA", df.Columns[v].numChars)
				}
			default:
				str += addRightPadding(fmt.Sprint(df.Columns[v].row[i]), df.Columns[v].numChars)
			}
			str += "  "
		}
		str += "\n"
	}
	return str
}

// ----------------------------------------------------------------------
// Column Methods
// ----------------------------------------------------------------------

// FillColumn will use reflection to fill the column with the given values
func (c *Column) FillColumn(values interface{}) {
	switch reflect.TypeOf(values).Kind() {
	case reflect.Slice:
		s := reflect.ValueOf(values)
		c.row = make([]interface{}, 0)
		for i := 0; i < s.Len(); i++ {
			cell := s.Index(i).Interface()
			c.row = append(c.row, cell)
			rowStr := ""
			switch cell.(type) {
			case *int:
				if cell.(*int) != nil {
					rowStr = fmt.Sprint(*cell.(*int))
				}
				c.colType = "int"
			case *float64:
				if cell.(*float64) != nil {
					rowStr = fmt.Sprint(*cell.(*float64))
				}
				c.colType = "float64"
			case *time.Time:
				if cell.(*time.Time) != nil {
					rowStr = fmt.Sprint(*cell.(*time.Time))
				}
				c.colType = "date"
			default:
				rowStr = fmt.Sprint(cell)
				c.colType = "string"
			}
			if len(rowStr) > c.numChars {
				c.numChars = len(rowStr)
			}
		}
	}
}

// ParseType will parse the column based on the given type
func (c *Column) ParseType(t string) error {
	var newRows interface{}
	switch t {
	case "int":
		newRows = []*int{}
	case "float64":
		newRows = []*float64{}
	case "string":
		newRows = []string{}
	case "date":
		newRows = []*time.Time{}
	}

	// TODO: Retrieve all formatting errors to return it as warnings and in case
	// of errors we use NA by default

	c.numChars = len(c.colName)
	for _, v := range c.row {
		r := fmt.Sprint(v)
		if len(r) > c.numChars {
			c.numChars = len(r)
		}
		switch t {
		case "int":
			i, err := strconv.Atoi(r)
			if err != nil {
				newRows = append(newRows.([]*int), nil)
			} else {
				newRows = append(newRows.([]*int), &i)
			}
		case "float64":
			i, err := strconv.ParseFloat(r, 64)
			if err != nil {
				newRows = append(newRows.([]*float64), nil)
			} else {
				newRows = append(newRows.([]*float64), &i)
			}
		case "string":
			newRows = append(newRows.([]string), r)
		case "date":
			i, err := time.Parse(defaultDateFormat, r)
			if err != nil {
				newRows = append(newRows.([]*time.Time), nil)
			} else {
				newRows = append(newRows.([]*time.Time), &i)
			}
		default:
			return errors.New("Unknown type")
		}
	}
	c.FillColumn(newRows)
	return nil
}

func (c Column) String() string {
	strArray := []string{}
	for _, v := range c.row {
		switch c.colType {
		case "int":
			cell := v.(*int)
			if cell != nil {
				strArray = append(strArray, fmt.Sprint(*cell))
			} else {
				strArray = append(strArray, "NA")
			}
		case "float64":
			cell := v.(*float64)
			if cell != nil {
				strArray = append(strArray, fmt.Sprint(*cell))
			} else {
				strArray = append(strArray, "NA")
			}
		case "date":
			cell := v.(*time.Time)
			if cell != nil {
				strArray = append(strArray, fmt.Sprint(*cell))
			} else {
				strArray = append(strArray, "NA")
			}
		default:
			strArray = append(strArray, fmt.Sprint(v))
		}
	}

	return fmt.Sprintln(c.colName, "(", c.colType, "):\n", strings.Join(strArray, "\n "))
}
