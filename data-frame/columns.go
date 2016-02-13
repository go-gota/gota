package df

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

// column represents a column inside a DataFrame
type column struct {
	cells    cells
	colType  string
	colName  string
	numChars int
}

// newCol is the constructor for a new Column with the given colName and elements
func newCol(colName string, elements cells) (*column, error) {
	col := column{
		colName: colName,
	}
	col, err := col.append(elements...)
	if err != nil {
		return nil, err
	}

	return &col, nil
}

// Implementing the Stringer interface for Column
func (col column) String() string {
	strArray := []string{}

	for i := 0; i < len(col.cells); i++ {
		strArray = append(strArray, col.cells[i].String())
	}

	return fmt.Sprintln(
		col.colName,
		"(", col.colType, "):\n",
		strings.Join(strArray, "\n "),
	)
}

func parseColumn(col column, t string) (*column, error) {
	switch t {
	case "string":
		newcells := Strings(col.cells)
		newcol, err := newCol(col.colName, newcells)
		return newcol, err
	case "int":
		newcells := Ints(col.cells)
		newcol, err := newCol(col.colName, newcells)
		return newcol, err
	case "float":
	case "date":
	}
	return nil, errors.New("Can't parse the given type")
}

// Append will add a value or values to a column
func (col column) append(values ...cell) (column, error) {
	numChars := 0
	if col.numChars == 0 {
		numChars = len(col.colName)
	}

	if len(values) == 0 {
		col.numChars = numChars
		return col, nil
	}

	for _, v := range values {
		t := reflect.TypeOf(v).String()
		if col.colType == "" {
			col.colType = t
		} else {
			if t != col.colType {
				return col, errors.New("Can't have elements of different type on the same column")
			}
		}
		cellStr := formatCell(v)
		if len(cellStr) > numChars {
			numChars = len(cellStr)
		}

		col.cells = append(col.cells, v)
	}

	col.numChars = numChars

	return col, nil
}
