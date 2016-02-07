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
	colTypes []string
	nCols    int
	nRows    int
	keys     []string
}

type C struct {
	Colname  string
	Elements interface{}
}

func New(cols ...C) {
	for _, v := range cols {
		fmt.Println(v.Colname)
	}
}

// Column is a column inside a DataFrame
type Column struct {
	row      []interface{}
	colType  string
	colName  string
	numChars int
}

// Row represents a single row on a DataFrame
type Row struct {
	Columns  Columns
	colNames []string
	colTypes []string
	nCols    int
}

// R represent a range from a number to another
type R struct {
	From int
	To   int
}

// u represents if an element is unique or if it appears on more than one place in
// addition to the index where it appears.
type u struct {
	unique  bool
	appears []int
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
		Columns:  initColumns(colnames),
		nRows:    nRows,
		nCols:    nCols,
		colNames: colnames,
		colTypes: []string{},
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
		newDf.colTypes = append(newDf.colTypes, column.colType)
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
			df.colTypes[k] = types[k]
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
			colIndex, _ := df.colIndex(k)
			df.colTypes[*colIndex] = v
			df.Columns[k] = col
		}
	}

	return nil
}

// SaveRecords will save data to records in [][]string format
func (df DataFrame) SaveRecords() [][]string {
	if df.nCols == 0 {
		return make([][]string, 0)
	}
	if df.nRows == 0 {
		records := make([][]string, 1)
		records[0] = df.colNames
		return records
	}

	var records [][]string

	records = append(records, df.colNames)
	for i := 0; i < df.nRows; i++ {
		r := []string{}
		for _, v := range df.colNames {
			r = append(r, df.Columns[v].getRowStr(i))
		}
		records = append(records, r)
	}

	return records
}

// TODO: Save to other formats. JSON? XML?

// Dim will return the current dimensions of the DataFrame in a two element array
// where the first element is the number of rows and the second the number of
// columns.
func (df DataFrame) Dim() (dim [2]int) {
	dim[0] = df.nRows
	dim[1] = df.nCols
	return
}

// colIndex tries to find the column index for a given column name
func (df DataFrame) colIndex(colname string) (*int, error) {
	for k, v := range df.colNames {
		if v == colname {
			return &k, nil
		}
	}
	return nil, errors.New("Can't find the given column")
}

// getRow tries to return the Row for a given row number
func (df DataFrame) getRow(i int) (*Row, error) {
	if i >= df.nRows {
		return nil, errors.New("Row out of range")
	}

	row := Row{
		Columns:  initColumns(df.colNames),
		colNames: df.colNames,
		colTypes: df.colTypes,
		nCols:    df.nCols,
	}
	for _, v := range df.colNames {
		col := Column{}
		r := df.Columns[v].row[i]
		col.FillColumn(r)
		row.Columns[v] = col
	}

	return &row, nil
}

// Subset will return a DataFrame that contains only the columns and rows contained
// on the given subset
func (df DataFrame) Subset(subsetCols interface{}, subsetRows interface{}) (*DataFrame, error) {
	dfA, err := df.SubsetColumns(subsetCols)
	if err != nil {
		return nil, err
	}
	dfB, err := dfA.SubsetRows(subsetRows)
	if err != nil {
		return nil, err
	}

	return dfB, nil
}

// SubsetColumns will return a DataFrame that contains only the columns contained
// on the given subset
func (df DataFrame) SubsetColumns(subset interface{}) (*DataFrame, error) {
	// Generate a DataFrame to store the temporary values
	newDf := DataFrame{
		Columns:  make(Columns),
		nRows:    df.nRows,
		colNames: []string{},
		colTypes: []string{},
	}

	switch subset.(type) {
	case R:
		s := subset.(R)
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
		newDf.colTypes = df.colTypes[s.From:s.To]
		for _, v := range df.colNames[s.From:s.To] {
			col := df.Columns[v]
			newDf.Columns[v] = col
		}
	case []int:
		colNums := subset.([]int)
		if len(colNums) == 0 {
			return nil, errors.New("Empty subset")
		}

		// Check for errors
		colNumsMap := make(map[int]bool)
		for _, v := range colNums {
			if v >= df.nCols || v < 0 {
				return nil, errors.New("Subset out of range")
			}
			if _, ok := colNumsMap[v]; !ok {
				colNumsMap[v] = true
			} else {
				return nil, errors.New("Duplicated column numbers")
			}
		}

		newDf.nCols = len(colNums)
		for _, v := range colNums {
			col := df.Columns[df.colNames[v]]
			newDf.Columns[df.colNames[v]] = col
			newDf.colNames = append(newDf.colNames, df.colNames[v])
			newDf.colTypes = append(newDf.colTypes, df.colTypes[v])
		}
	case []string:
		columns := subset.([]string)
		if len(columns) == 0 {
			return nil, errors.New("Empty subset")
		}

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
				colindex, err := df.colIndex(v)
				if err != nil {
					return nil, err
				}
				newDf.colTypes = append(newDf.colTypes, df.colTypes[*colindex])
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
	default:
		return nil, errors.New("Unknown subsetting option")
	}

	newDf.nCols = len(newDf.colNames)

	return &newDf, nil
}

// SubsetRows will return a DataFrame that contains only the selected rows
func (df DataFrame) SubsetRows(subset interface{}) (*DataFrame, error) {
	// Generate a DataFrame to store the temporary values
	newDf := DataFrame{
		Columns:  initColumns(df.colNames),
		nCols:    df.nCols,
		colNames: df.colNames,
		colTypes: df.colTypes,
	}

	switch subset.(type) {
	case R:
		s := subset.(R)
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
	case []int:
		rowNums := subset.([]int)

		if len(rowNums) == 0 {
			return nil, errors.New("Empty subset")
		}

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
	default:
		return nil, errors.New("Unknown subsetting option")
	}

	return &newDf, nil
}

// addRow adds a single Row to the DataFrame
func (df *DataFrame) addRow(row Row) error {
	// Check that the given row contains the same number of columns that the
	// current dataframe.
	if df.nCols != row.nCols {
		return errors.New("Different number of columns")
	}

	// Check that the names and the types of all columns are the same
	colNameTypeMap := make(map[string]string)
	for k, v := range df.colNames {
		colNameTypeMap[v] = df.colTypes[k]
	}
	for k, v := range row.colNames {
		if dfType, ok := colNameTypeMap[v]; ok {
			if dfType != row.colTypes[k] {
				return errors.New("Mismatching column types")
			}
		} else {
			return errors.New("Mismatching column names")
		}
	}

	cols := make(Columns)
	for _, v := range df.colNames {
		col := df.Columns[v]
		err := col.AddValues(row.Columns[v].row)
		if err != nil {
			return err
		}
		cols[v] = col
	}
	df.Columns = cols
	df.nRows++

	return nil
}

// Cbind combines the columns of two DataFrames
func Cbind(dfA DataFrame, dfB DataFrame) (*DataFrame, error) {
	// Check that the two DataFrames contains the same number of rows
	if dfA.nRows != dfB.nRows {
		return nil, errors.New("Different number of rows")
	}

	// Check that the column names are unique when combined
	colNameMap := make(map[string]bool)
	for _, v := range dfA.colNames {
		colNameMap[v] = true
	}

	for _, v := range dfB.colNames {
		if _, ok := colNameMap[v]; ok {
			return nil, errors.New("Conflicting column names")
		}
	}

	colnames := append(dfA.colNames, dfB.colNames...)
	coltypes := append(dfA.colTypes, dfB.colTypes...)

	newDf := DataFrame{
		Columns:  initColumns(colnames),
		nRows:    dfA.nRows,
		nCols:    len(colnames),
		colNames: colnames,
		colTypes: coltypes,
	}

	for _, v := range dfA.colNames {
		newDf.Columns[v] = dfA.Columns[v]
	}
	for _, v := range dfB.colNames {
		newDf.Columns[v] = dfB.Columns[v]
	}

	return &newDf, nil
}

// Rbind combines the rows of two dataframes
func Rbind(dfA DataFrame, dfB DataFrame) (*DataFrame, error) {
	// Check that the given DataFrame contains the same number of columns that the
	// current dataframe.
	if dfA.nCols != dfB.nCols {
		return nil, errors.New("Different number of columns")
	}

	// Check that the names and the types of all columns are the same
	colNameTypeMap := make(map[string]string)
	for k, v := range dfA.colNames {
		colNameTypeMap[v] = dfA.colTypes[k]
	}

	for k, v := range dfB.colNames {
		if dfType, ok := colNameTypeMap[v]; ok {
			if dfType != dfB.colTypes[k] {
				return nil, errors.New("Mismatching column types")
			}
		} else {
			return nil, errors.New("Mismatching column names")
		}
	}

	cols := make(Columns)
	for _, v := range dfA.colNames {
		col := dfA.Columns[v]
		err := col.AddValues(dfB.Columns[v].row)
		if err != nil {
			return nil, err
		}
		cols[v] = col
	}
	dfA.Columns = cols
	dfA.nRows += dfB.nRows

	return &dfA, nil
}

// uniqueRowsMap is a helper function that will get a map of unique or duplicated
// rows for a given DataFrame
func uniqueRowsMap(df DataFrame) map[string]u {
	uniqueRows := make(map[string]u)
	for i := 0; i < df.nRows; i++ {
		str := ""
		for _, v := range df.colNames {
			col := df.Columns[v]
			str += col.colType
			str += col.getRowStr(i)
		}
		if a, ok := uniqueRows[str]; ok {
			a.unique = false
			a.appears = append(a.appears, i)
			uniqueRows[str] = a
		} else {
			uniqueRows[str] = u{true, []int{i}}
		}
	}

	return uniqueRows
}

// Unique will return all unique rows inside a DataFrame. The order of the rows
// will not be preserved.
func (df DataFrame) Unique() (*DataFrame, error) {
	newDf := DataFrame{
		Columns:  initColumns(df.colNames),
		nCols:    df.nCols,
		colNames: df.colNames,
		colTypes: df.colTypes,
	}

	uniqueRows := uniqueRowsMap(df)
	for _, v := range uniqueRows {
		if v.unique {
			row, err := df.getRow(v.appears[0])
			if err != nil {
				return nil, err
			}
			newDf.addRow(*row)
		}
	}

	return &newDf, nil
}

// Duplicated will return all duplicated rows inside a DataFrame
func (df DataFrame) Duplicated() (*DataFrame, error) {
	newDf := DataFrame{
		Columns:  initColumns(df.colNames),
		nCols:    df.nCols,
		colNames: df.colNames,
		colTypes: df.colTypes,
	}

	type u struct {
		unique  bool
		appears []int
	}

	uniqueRows := uniqueRowsMap(df)
	for _, v := range uniqueRows {
		if !v.unique {
			for _, i := range v.appears {
				row, err := df.getRow(i)
				if err != nil {
					return nil, err
				}
				newDf.addRow(*row)
			}
		}
	}

	return &newDf, nil
}

// Implementing the Stringer interface for DataFrame
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

// AddValues will add a value or values to a column
func (c *Column) AddValues(values interface{}) error {
	if len(c.row) == 0 {
		c.FillColumn(values)
		return nil
	}
	var cell interface{}
	checkColumnType := func() error {
		rowStr := ""
		switch cell.(type) {
		case *int:
			if c.colType != "int" {
				return errors.New("Wrong type passed to column, 'int' expected")
			}
			if cell.(*int) != nil {
				rowStr = fmt.Sprint(*cell.(*int))
			}
		case *float64:
			if c.colType != "float64" {
				return errors.New("Wrong type passed to column, 'float64' expected")
			}
			if cell.(*float64) != nil {
				rowStr = fmt.Sprint(*cell.(*float64))
			}
		case *time.Time:
			if c.colType != "date" {
				return errors.New("Wrong type passed to column, 'date' expected")
			}
			if cell.(*time.Time) != nil {
				rowStr = fmt.Sprint(*cell.(*time.Time))
			}
		case string:
			rowStr = fmt.Sprint(cell)
		default:
			return errors.New("Unknown type")
		}

		// Adjust c.numChars if necessary
		if len(rowStr) > c.numChars {
			c.numChars = len(rowStr)
		}

		return nil
	}
	switch reflect.TypeOf(values).Kind() {
	case reflect.Slice:
		s := reflect.ValueOf(values)
		for i := 0; i < s.Len(); i++ {
			cell = s.Index(i).Interface()
			checkColumnType()
			c.row = append(c.row, cell)
		}
	default:
		fmt.Println("dong")
		s := reflect.ValueOf(values)
		cell = s.Interface()
		checkColumnType()
		c.row = append(c.row, cell)
	}

	return nil
}

// FillColumn will use reflection to fill the column with the given values
func (c *Column) FillColumn(values interface{}) {
	var cell interface{}
	setColumnType := func() {
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
		case string:
			rowStr = fmt.Sprint(cell)
			c.colType = "string"
		default:
			// TODO: ERROR?
			//rowStr = fmt.Sprint(cell)
			//c.colType = "string"
		}

		// Adjust c.numChars if necessary
		if len(rowStr) > c.numChars {
			c.numChars = len(rowStr)
		}
	}
	switch reflect.TypeOf(values).Kind() {
	case reflect.Slice:
		// TODO: What happens if the values is empty? Empty column? Error?
		s := reflect.ValueOf(values)
		c.row = make([]interface{}, 0)
		for i := 0; i < s.Len(); i++ {
			cell = s.Index(i).Interface()
			setColumnType()
			c.row = append(c.row, cell)
		}
	default:
		s := reflect.ValueOf(values)
		c.row = make([]interface{}, 0)
		cell = s.Interface()
		setColumnType()
		c.row = append(c.row, cell)
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
	default:
		return errors.New("Unknown type")
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

// getRowStr returns the string representation of a row on a given column
func (c Column) getRowStr(i int) string {
	var str string
	switch c.colType {
	case "int":
		row := c.row[i].(*int)
		if row != nil {
			str = fmt.Sprint(*row)
		} else {
			str = "NA"
		}
	case "float64":
		row := c.row[i].(*float64)
		if row != nil {
			str = fmt.Sprint(*row)
		} else {
			str = "NA"
		}
	case "string":
		row := c.row[i].(string)
		str = row
	case "date":
		row := c.row[i].(*time.Time)
		if row != nil {
			str = fmt.Sprint(*row)
		} else {
			str = "NA"
		}
	default:
		str = "NA"
	}
	return str
}

// Implementing the Stringer interface for Column
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

// ----------------------------------------------------------------------
// Columns methods
// ----------------------------------------------------------------------

// initColumns will initialize an empty Columns given an array of column names
func initColumns(names []string) Columns {
	c := make(Columns)
	for _, v := range names {
		c[v] = Column{
			colName:  v,
			numChars: len(v),
		}
	}

	return c
}
