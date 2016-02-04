package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strconv"
)

// TODO: Write tests!
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
	//in := `A,B,C,D
	//1,2,3,4
	//5,6,7,8`
	//df := DataFrame{}
	//r := csv.NewReader(strings.NewReader(in))
	//records, err := r.ReadAll()
	//if err != nil {
	//panic(err)
	//}
	//err = df.loadAndParse(records, []string{"string", "int", "string", "int"})
	//if err != nil {
	//panic(err)
	//}

	//for _, v := range df.columns {
	//fmt.Println(v)
	//}
	//fmt.Println(df)

	// Test 03
	df := DataFrame{}
	csvfile, err := os.Open("example.csv")
	if err != nil {
		fmt.Println(err)
		return
	}
	r := csv.NewReader(csvfile)
	records, err := r.ReadAll()
	if err != nil {
		fmt.Println(err)
		return
	}
	df.loadData(records)
	fmt.Println(df)
}

// DataFrame Definition
// ====================
type DataFrame struct {
	columns  map[string]Column
	colNames []string
	nCols    int
	nRows    int
}

// DataFrame Methods
// =================
//func (df *DataFrame) loadAndParse(records [][]string, types []string) error {
//err := df.loadData(records)
//if err != nil {
//return err
//}
//if df.nCols != len(types) {
//return errors.New("Number of columns different from number of types")
//}
//for k, v := range df.columns {
//v.parseType(types[k])
//df.columns[k].colType = types[k]
//}
//return nil
//}

func (df DataFrame) subsetColumns(columns []string) (DataFrame, error) {
	newDf := DataFrame{}
	return newDf, nil
}

func (df *DataFrame) loadData(records [][]string) error {
	// Get DataFrame dimensions
	nRows := len(records) - 1
	if nRows <= 0 {
		return errors.New("Empty dataframe")
	}
	nCols := len(records[0])

	// Generate a virtual df to store the temporary values
	newDf := DataFrame{
		columns:  make(map[string]Column),
		nRows:    nRows,
		nCols:    nCols,
		colNames: records[0],
	}

	for j := 0; j < nCols; j++ {
		col := []string{}
		for i := 1; i < nRows+1; i++ {
			col = append(col, records[i][j])
		}
		column := Column{}
		column.maxCharLength = len(records[0][j])
		column.fillColumn(col)
		newDf.columns[records[0][j]] = column
	}
	*df = newDf
	return nil
}

//func (df DataFrame) colnames() (colnames []string) {
//for _, v := range df.columns {
//colnames = append(colnames, v.colName)
//}
//return
//}

// TODO: Truncate output for the same tabular format?
func (df DataFrame) String() (str string) {
	addPadding := func(s string, nchar int) string {
		for {
			if len(s) >= nchar {
				return s
			}
			s += " "
		}
	}
	colnames := df.colNames
	if len(colnames) != 0 {
		str += "   "
		for _, v := range colnames {
			str += addPadding(v, df.columns[v].maxCharLength)
			str += "  "
		}
		str += "\n"
		str += "\n"
	}
	for i := 0; i < df.nRows; i++ {
		str += strconv.Itoa(i+1) + ": "
		for _, v := range colnames {
			str += addPadding(fmt.Sprint(df.columns[v].row[i]), df.columns[v].maxCharLength)
			str += "  "
		}
		str += "\n"
	}
	return str
}

// Column Definition
// =================
type Column struct {
	row           []interface{}
	colType       string
	maxCharLength int
}

// Column Methods
// ==============
func (c Column) String() string {
	return fmt.Sprint(c.row)
}

func (c *Column) parseType(t string) error {
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
	c.fillColumn(newRows)
	return nil
}

// TODO: Should this return an error?
func (c *Column) fillColumn(values interface{}) {
	switch reflect.TypeOf(values).Kind() {
	case reflect.Slice:
		s := reflect.ValueOf(values)
		for i := 0; i < s.Len(); i++ {
			c.row = append(c.row, s.Index(i).Interface())
			c.colType = fmt.Sprint(s.Index(i).Type())
			rowStr := fmt.Sprint(s.Index(i).Interface())
			if len(rowStr) > c.maxCharLength {
				c.maxCharLength = len(rowStr)
			}
		}
	}
}
