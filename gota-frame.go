package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

func main() {
	// Test 02
	//textColumn := []string{"One", "Two", "Three"}
	//intColumn := []int{1, 2, 3}
	//c1 := Column{}
	//c1.fillColumn(textColumn)
	//c2 := Column{}
	//c2.fillColumn(intColumn)
	//df := DataFrame{
	//columns:  []Column{c1, c2},
	//nCols:    2,
	//nRows:    3,
	//colnames: []string{"Text", "Ints"},
	//}
	//fmt.Println(df)

	// Test 01
	in := `A,B,C,D
1,2,3,4
5,6,7,8`
	df := DataFrame{}
	r := csv.NewReader(strings.NewReader(in))
	records, err := r.ReadAll()
	if err != nil {
		panic(err)
	}
	df.loadData(records)

	for _, v := range df.columns {
		fmt.Println(v)
	}
	fmt.Println(df)
}

// DataFrame Definition
// ====================
type DataFrame struct {
	columns  []Column
	colnames []string
	nCols    int
	nRows    int
}

// DataFrame Methods
// =================
func (df *DataFrame) loadData(records [][]string) error {
	// TODO: Check if empty records

	// Get DataFrame dimensions
	nRows := len(records) - 1
	if nRows == 0 {
		return errors.New("Empty dataframe")
	}
	nCols := len(records[0])

	// Generate a virtual df to store the temporary values
	newDf := DataFrame{
		columns:  []Column{},
		colnames: records[0],
		nRows:    nRows,
		nCols:    nCols,
	}

	for j := 0; j < nCols; j++ {
		col := []string{}
		for i := 1; i < nRows+1; i++ {
			col = append(col, records[i][j])
		}
		column := Column{}
		column.fillColumn(col)
		newDf.columns = append(newDf.columns, column)
	}
	*df = newDf
	return nil
}

func (df DataFrame) String() string {
	str := ""
	if len(df.colnames) != 0 {
		str += "\t"
		for _, v := range df.colnames {
			str += v
			str += "\t"
		}
		str += "\n"
		str += "\n"
	}
	for i := 0; i < df.nRows; i++ {
		str += strconv.Itoa(i+1) + ":\t"
		for j := 0; j < df.nCols; j++ {
			str += fmt.Sprint(df.columns[j].row[i])
			str += "\t"
		}
		str += "\n"
	}
	return str
}

// Column Definition
// =================
type Column struct {
	row     []interface{}
	colType string
}

// Column Methods
// ==============
func (c Column) String() string {
	return fmt.Sprint(c.row)
}

// TODO: Should this return an error?
func (c *Column) fillColumn(values interface{}) {
	switch reflect.TypeOf(values).Kind() {
	case reflect.Slice:
		s := reflect.ValueOf(values)
		for i := 0; i < s.Len(); i++ {
			c.row = append(c.row, s.Index(i).Interface())
			c.colType = fmt.Sprint(s.Index(i).Type())
		}
	}
}
