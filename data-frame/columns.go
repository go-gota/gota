package df

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

// Column is a column inside a DataFrame, err
type Column struct {
	row      interface{}
	colType  string
	colName  string
	numChars int
}

// Columns is an alias for multiple columns
type Columns map[string]Column

// Len returns the length of the rows slice
func (c Column) Len() int {
	var l int
	switch c.row.(type) {
	case nil:
		l = 0
	default:
		if reflect.TypeOf(c.row).Kind() == reflect.Slice {
			v := reflect.ValueOf(c.row)
			l = v.Len()
		}
	}

	return l
}

// NewCol is the constructor for a new Column with the given colName and elements
func NewCol(colName string, elements interface{}) (*Column, error) {
	col := &Column{
		colName: colName,
	}
	err := col.FillColumn(elements)
	if err != nil {
		return nil, err
	}

	return col, nil
}

// FillColumn will use reflection to fill the column with the given values
func (c *Column) FillColumn(values interface{}) error {
	switch values.(type) {
	case nil:
		return errors.New("Can't create empty column")
	}

	rowableType := reflect.TypeOf((*rowable)(nil)).Elem()
	numChars := len(c.colName)
	switch reflect.TypeOf(values).Kind() {
	case reflect.Slice:
		s := reflect.ValueOf(values)
		if s.Len() == 0 {
			return errors.New("Can't create empty column")
		}

		// The given elements should implement the rowable interface
		if s.Index(0).Type().Implements(rowableType) {
			sarr := reflect.MakeSlice(
				reflect.SliceOf(s.Index(0).Type()),
				0,
				s.Len(),
			)
			t := s.Index(0).Type()
			for i := 0; i < s.Len(); i++ {
				// Check that all the elements on a column hsarre the same type
				if t != s.Index(i).Type() {
					return errors.New("Can't use different types on a column")
				}

				// Update Column.numChars if necessary
				rowStr := formatCell(s.Index(i).Interface())
				if len(rowStr) > numChars {
					numChars = len(rowStr)
				}
				sarr = reflect.Append(sarr, s.Index(i))
			}

			// Update column variables on success
			c.row = sarr.Interface()
			c.colType = t.String()
			c.numChars = numChars
		} else {
			return errors.New("The given values don't comply with the rowable interface")
		}
	default:
		s := reflect.ValueOf(values)
		if s.Type().Implements(rowableType) {
			sarr := reflect.MakeSlice(reflect.SliceOf(s.Type()), 0, 1)
			rowStr := formatCell(s.Interface())
			if len(rowStr) > numChars {
				numChars = len(rowStr)
			}
			sarr = reflect.Append(sarr, s)

			// Update column variables on success
			c.row = sarr.Interface()
			c.colType = s.Type().String()
			c.numChars = numChars
		} else {
			return errors.New("The given values don't comply with the rowable interface")
		}
	}

	return nil
}

// Index will return the element at a given index
func (c Column) Index(i int) (interface{}, error) {
	if c.row == nil {
		return nil, errors.New("Empty column")
	}
	s := reflect.ValueOf(c.row)
	if i > s.Len() {
		return nil, errors.New(fmt.Sprint("Index out of bounds", i))
	}

	return s.Index(i).Interface(), nil
}

// Implementing the Stringer interface for Column
func (c Column) String() string {
	strArray := []string{}
	s := reflect.ValueOf(c.row)

	for i := 0; i < s.Len(); i++ {
		strArray = append(strArray, formatCell(s.Index(i).Interface()))
	}

	return fmt.Sprintln(
		c.colName,
		"(", c.colType, "):\n",
		strings.Join(strArray, "\n "),
	)
}

func parseColumn(col Column, t string) (*Column, error) {
	switch t {
	case "string":
		newrows := Strings(col.row)
		newcol, err := NewCol(col.colName, newrows)
		return newcol, err
	case "int":
		newrows := Ints(col.row)
		newcol, err := NewCol(col.colName, newrows)
		return newcol, err
	case "float":
	case "time":
	}
	return nil, errors.New("Can't parse the given type")
}

// Append will add a value or values to a column
func Append(col Column, values interface{}) error {

	return nil
}
