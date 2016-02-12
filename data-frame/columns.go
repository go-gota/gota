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

func (c Column) elementAtIndex(i int) (interface{}, error) {
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

func parseColumn(col Column, t string, options interface{}) (*Column, error) {
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
	return nil, nil
}

//// AddValues will add a value or values to a column
//func (c *Column) AddValues(values interface{}) error {
//if len(c.row) == 0 {
//c.FillColumn(values)
//return nil
//}
//var cell interface{}
//checkColumnType := func() error {
//rowStr := ""
//switch cell.(type) {
//case *int:
//if c.colType != "int" {
//return errors.New("Wrong type passed to column, 'int' expected")
//}
//if cell.(*int) != nil {
//rowStr = fmt.Sprint(*cell.(*int))
//}
//case *float64:
//if c.colType != "float64" {
//return errors.New("Wrong type passed to column, 'float64' expected")
//}
//if cell.(*float64) != nil {
//rowStr = fmt.Sprint(*cell.(*float64))
//}
//case *time.Time:
//if c.colType != "date" {
//return errors.New("Wrong type passed to column, 'date' expected")
//}
//if cell.(*time.Time) != nil {
//rowStr = fmt.Sprint(*cell.(*time.Time))
//}
//case string:
//rowStr = fmt.Sprint(cell)
//default:
//return errors.New("Unknown type")
//}

//// Adjust c.numChars if necessary
//if len(rowStr) > c.numChars {
//c.numChars = len(rowStr)
//}

//return nil
//}
//switch reflect.TypeOf(values).Kind() {
//case reflect.Slice:
//s := reflect.ValueOf(values)
//for i := 0; i < s.Len(); i++ {
//cell = s.Index(i).Interface()
//checkColumnType()
//c.row = append(c.row, cell)
//}
//default:
//s := reflect.ValueOf(values)
//cell = s.Interface()
//checkColumnType()
//c.row = append(c.row, cell)
//}

//return nil
//}

//// ParseType will parse the column based on the given type
//func (c *Column) ParseType(t string) error {
//var newRows interface{}
//switch t {
//case "int":
//newRows = []*int{}
//case "float64":
//newRows = []*float64{}
//case "string":
//newRows = []string{}
//case "date":
//newRows = []*time.Time{}
//default:
//return errors.New("Unknown type")
//}

//// TODO: Retrieve all formatting errors to return it as warnings and in case
//// of errors we use NA by default

//c.numChars = len(c.colName)
//for _, v := range c.row {
//r := fmt.Sprint(v)
//if len(r) > c.numChars {
//c.numChars = len(r)
//}
//switch t {
//case "int":
//i, err := strconv.Atoi(r)
//if err != nil {
//newRows = append(newRows.([]*int), nil)
//} else {
//newRows = append(newRows.([]*int), &i)
//}
//case "float64":
//i, err := strconv.ParseFloat(r, 64)
//if err != nil {
//newRows = append(newRows.([]*float64), nil)
//} else {
//newRows = append(newRows.([]*float64), &i)
//}
//case "string":
//newRows = append(newRows.([]string), r)
//case "date":
//i, err := time.Parse(defaultDateFormat, r)
//if err != nil {
//newRows = append(newRows.([]*time.Time), nil)
//} else {
//newRows = append(newRows.([]*time.Time), &i)
//}
//default:
//return errors.New("Unknown type")
//}
//}
//c.FillColumn(newRows)
//return nil
//}
