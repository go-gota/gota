package df

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

// NOTE: The concept of NA is represented by nil pointers

// TODO: Constructors should allow options to set up:
//	DateFormat
//	TrimSpaces?

type rowable interface {
	String() string
}

type cell interface {
	String() string
}

type cells []cell

type tointeger interface {
	ToInteger() (int, error)
}

// DataFrame is the base data structure
type DataFrame struct {
	Columns  Columns
	colNames []string
	colTypes []string
	nCols    int
	nRows    int
	keys     []string
}

// C represents a way to pass Colname and Elements to a DF constructor
type C struct {
	Colname  string
	Elements interface{}
}

// New is a constructor for DataFrames
func New(colConst ...interface{}) (*DataFrame, error) {
	if len(colConst) == 0 {
		return nil, errors.New("Can't create empty DataFrame")
	}
	var colLength int
	colNames := []string{}
	colTypes := []string{}
	cols := make(Columns)
	for k, v := range colConst {
		switch v.(type) {
		case C:
			val := v.(C)
			col, err := NewCol(val.Colname, val.Elements)
			if err != nil {
				return nil, err
			}

			// Check that the length of all columns are the same
			if k == 0 {
				colLength = col.Len()
			} else {
				if colLength != col.Len() {
					return nil, errors.New("Columns don't have the same dimensions")
				}
			}
			cols[val.Colname] = *col
			colNames = append(colNames, col.colName)
			colTypes = append(colTypes, col.colType)
		}
	}
	df := &DataFrame{
		colNames: colNames,
		colTypes: colTypes,
		Columns:  cols,
		nRows:    colLength,
		nCols:    len(colNames),
	}

	return df, nil
}

//// Row represents a single row on a DataFrame
//type Row struct {
//Columns  Columns
//colNames []string
//colTypes []string
//nCols    int
//}

//// R represent a range from a number to another
//type R struct {
//From int
//To   int
//}

//// u represents if an element is unique or if it appears on more than one place in
//// addition to the index where it appears.
//type u struct {
//unique  bool
//appears []int
//}

// T is used to represent the association between a column and it't type
type T map[string]string

////type Error struct {
////errorType Err
////}

//const defaultDateFormat = "2006-01-02"

//// TODO: Implement a custom Error type that stores information about the type of
//// error and the severity of it (Warning vs Error)
//// Error types
////type Err int

////const (
////FormatError Err = iota
////UnknownType
////Warning
////Etc
////)

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
		nRows:    nRows,
		nCols:    nCols,
		colNames: colnames,
		colTypes: []string{},
	}

	cols := make(Columns)
	// Fill the columns on the DataFrame
	for j := 0; j < nCols; j++ {
		colstrarr := []string{}
		for i := 1; i < nRows+1; i++ {
			colstrarr = append(colstrarr, records[i][j])
		}

		col, err := NewCol(colnames[j], Strings(colstrarr))
		if err != nil {
			return err
		}

		cols[colnames[j]] = *col

		newDf.colTypes = append(newDf.colTypes, col.colType)
	}

	newDf.Columns = cols
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
			col, err := parseColumn(df.Columns[v], types[k])
			if err != nil {
				return err
			}
			df.colTypes[k] = col.colType
			df.Columns[v] = *col
		}
	case T:
		types := types.(T)
		for k, v := range types {
			col, err := parseColumn(df.Columns[k], v)
			if err != nil {
				return err
			}
			col.colType = v
			colIndex, _ := df.colIndex(k)
			df.colTypes[*colIndex] = col.colType
			df.Columns[k] = *col
		}
	}

	return nil
}

//// SaveRecords will save data to records in [][]string format
//func (df DataFrame) SaveRecords() [][]string {
//if df.nCols == 0 {
//return make([][]string, 0)
//}
//if df.nRows == 0 {
//records := make([][]string, 1)
//records[0] = df.colNames
//return records
//}

//var records [][]string

//records = append(records, df.colNames)
//for i := 0; i < df.nRows; i++ {
//r := []string{}
//for _, v := range df.colNames {
//r = append(r, df.Columns[v].getRowStr(i))
//}
//records = append(records, r)
//}

//return records
//}

//// TODO: Save to other formats. JSON? XML?

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

//// getRow tries to return the Row for a given row number
//func (df DataFrame) getRow(i int) (*Row, error) {
//if i >= df.nRows {
//return nil, errors.New("Row out of range")
//}

//row := Row{
//Columns:  initColumns(df.colNames),
//colNames: df.colNames,
//colTypes: df.colTypes,
//nCols:    df.nCols,
//}
//for _, v := range df.colNames {
//col := Column{}
//r := df.Columns[v].row[i]
//col.FillColumn(r)
//row.Columns[v] = col
//}

//return &row, nil
//}

//// Subset will return a DataFrame that contains only the columns and rows contained
//// on the given subset
//func (df DataFrame) Subset(subsetCols interface{}, subsetRows interface{}) (*DataFrame, error) {
//dfA, err := df.SubsetColumns(subsetCols)
//if err != nil {
//return nil, err
//}
//dfB, err := dfA.SubsetRows(subsetRows)
//if err != nil {
//return nil, err
//}

//return dfB, nil
//}

//// SubsetColumns will return a DataFrame that contains only the columns contained
//// on the given subset
//func (df DataFrame) SubsetColumns(subset interface{}) (*DataFrame, error) {
//// Generate a DataFrame to store the temporary values
//newDf := DataFrame{
//Columns:  make(Columns),
//nRows:    df.nRows,
//colNames: []string{},
//colTypes: []string{},
//}

//switch subset.(type) {
//case R:
//s := subset.(R)
//// Check for errors
//if s.From > s.To {
//return nil, errors.New("Bad subset: Start greater than Beginning")
//}
//if s.From == s.To {
//return nil, errors.New("Empty subset")
//}
//if s.To > df.nCols || s.To < 0 || s.From < 0 {
//return nil, errors.New("Subset out of range")
//}

//newDf.nCols = s.To - s.From
//newDf.colNames = df.colNames[s.From:s.To]
//newDf.colTypes = df.colTypes[s.From:s.To]
//for _, v := range df.colNames[s.From:s.To] {
//col := df.Columns[v]
//newDf.Columns[v] = col
//}
//case []int:
//colNums := subset.([]int)
//if len(colNums) == 0 {
//return nil, errors.New("Empty subset")
//}

//// Check for errors
//colNumsMap := make(map[int]bool)
//for _, v := range colNums {
//if v >= df.nCols || v < 0 {
//return nil, errors.New("Subset out of range")
//}
//if _, ok := colNumsMap[v]; !ok {
//colNumsMap[v] = true
//} else {
//return nil, errors.New("Duplicated column numbers")
//}
//}

//newDf.nCols = len(colNums)
//for _, v := range colNums {
//col := df.Columns[df.colNames[v]]
//newDf.Columns[df.colNames[v]] = col
//newDf.colNames = append(newDf.colNames, df.colNames[v])
//newDf.colTypes = append(newDf.colTypes, df.colTypes[v])
//}
//case []string:
//columns := subset.([]string)
//if len(columns) == 0 {
//return nil, errors.New("Empty subset")
//}

//// Initialize variables to store possible errors
//noCols := []string{}
//dupedCols := []string{}

//// Select the desired subset of columns
//for _, v := range columns {
//if col, ok := df.Columns[v]; ok {
//if _, ok := newDf.Columns[v]; ok {
//dupedCols = append(dupedCols, v)
//}
//newDf.colNames = append(newDf.colNames, v)
//colindex, err := df.colIndex(v)
//if err != nil {
//return nil, err
//}
//newDf.colTypes = append(newDf.colTypes, df.colTypes[*colindex])
//newDf.Columns[v] = col
//} else {
//noCols = append(noCols, v)
//}
//}

//if len(dupedCols) != 0 {
//errStr := "The following columns appear more than once:\n" + strings.Join(dupedCols, ", ")
//return nil, errors.New(errStr)
//}
//if len(noCols) != 0 {
//errStr := "The following columns are not present on the DataFrame:\n" + strings.Join(noCols, ", ")
//return nil, errors.New(errStr)
//}
//default:
//return nil, errors.New("Unknown subsetting option")
//}

//newDf.nCols = len(newDf.colNames)

//return &newDf, nil
//}

//// SubsetRows will return a DataFrame that contains only the selected rows
//func (df DataFrame) SubsetRows(subset interface{}) (*DataFrame, error) {
//// Generate a DataFrame to store the temporary values
//newDf := DataFrame{
//Columns:  initColumns(df.colNames),
//nCols:    df.nCols,
//colNames: df.colNames,
//colTypes: df.colTypes,
//}

//switch subset.(type) {
//case R:
//s := subset.(R)
//// Check for errors
//if s.From > s.To {
//return nil, errors.New("Bad subset: Start greater than Beginning")
//}
//if s.From == s.To {
//return nil, errors.New("Empty subset")
//}
//if s.To > df.nRows || s.To < 0 || s.From < 0 {
//return nil, errors.New("Subset out of range")
//}

//newDf.nRows = s.To - s.From
//for _, v := range df.colNames {
//col := df.Columns[v]
//col.FillColumn(col.row[s.From:s.To])
//newDf.Columns[v] = col
//}
//case []int:
//rowNums := subset.([]int)

//if len(rowNums) == 0 {
//return nil, errors.New("Empty subset")
//}

//// Check for errors
//for _, v := range rowNums {
//if v >= df.nRows || v < 0 {
//return nil, errors.New("Subset out of range")
//}
//}

//newDf.nRows = len(rowNums)
//for _, v := range df.colNames {
//col := df.Columns[v]
//var row []interface{}
//for _, v := range rowNums {
//row = append(row, col.row[v])
//}
//col.FillColumn(row)
//newDf.Columns[v] = col
//}
//default:
//return nil, errors.New("Unknown subsetting option")
//}

//return &newDf, nil
//}

//// addRow adds a single Row to the DataFrame
//func (df *DataFrame) addRow(row Row) error {
//// Check that the given row contains the same number of columns that the
//// current dataframe.
//if df.nCols != row.nCols {
//return errors.New("Different number of columns")
//}

//// Check that the names and the types of all columns are the same
//colNameTypeMap := make(map[string]string)
//for k, v := range df.colNames {
//colNameTypeMap[v] = df.colTypes[k]
//}
//for k, v := range row.colNames {
//if dfType, ok := colNameTypeMap[v]; ok {
//if dfType != row.colTypes[k] {
//return errors.New("Mismatching column types")
//}
//} else {
//return errors.New("Mismatching column names")
//}
//}

//cols := make(Columns)
//for _, v := range df.colNames {
//col := df.Columns[v]
//err := col.AddValues(row.Columns[v].row)
//if err != nil {
//return err
//}
//cols[v] = col
//}
//df.Columns = cols
//df.nRows++

//return nil
//}

//// Cbind combines the columns of two DataFrames
//func Cbind(dfA DataFrame, dfB DataFrame) (*DataFrame, error) {
//// Check that the two DataFrames contains the same number of rows
//if dfA.nRows != dfB.nRows {
//return nil, errors.New("Different number of rows")
//}

//// Check that the column names are unique when combined
//colNameMap := make(map[string]bool)
//for _, v := range dfA.colNames {
//colNameMap[v] = true
//}

//for _, v := range dfB.colNames {
//if _, ok := colNameMap[v]; ok {
//return nil, errors.New("Conflicting column names")
//}
//}

//colnames := append(dfA.colNames, dfB.colNames...)
//coltypes := append(dfA.colTypes, dfB.colTypes...)

//newDf := DataFrame{
//Columns:  initColumns(colnames),
//nRows:    dfA.nRows,
//nCols:    len(colnames),
//colNames: colnames,
//colTypes: coltypes,
//}

//for _, v := range dfA.colNames {
//newDf.Columns[v] = dfA.Columns[v]
//}
//for _, v := range dfB.colNames {
//newDf.Columns[v] = dfB.Columns[v]
//}

//return &newDf, nil
//}

//// Rbind combines the rows of two dataframes
//func Rbind(dfA DataFrame, dfB DataFrame) (*DataFrame, error) {
//// Check that the given DataFrame contains the same number of columns that the
//// current dataframe.
//if dfA.nCols != dfB.nCols {
//return nil, errors.New("Different number of columns")
//}

//// Check that the names and the types of all columns are the same
//colNameTypeMap := make(map[string]string)
//for k, v := range dfA.colNames {
//colNameTypeMap[v] = dfA.colTypes[k]
//}

//for k, v := range dfB.colNames {
//if dfType, ok := colNameTypeMap[v]; ok {
//if dfType != dfB.colTypes[k] {
//return nil, errors.New("Mismatching column types")
//}
//} else {
//return nil, errors.New("Mismatching column names")
//}
//}

//cols := make(Columns)
//for _, v := range dfA.colNames {
//col := dfA.Columns[v]
//err := col.AddValues(dfB.Columns[v].row)
//if err != nil {
//return nil, err
//}
//cols[v] = col
//}
//dfA.Columns = cols
//dfA.nRows += dfB.nRows

//return &dfA, nil
//}

//// uniqueRowsMap is a helper function that will get a map of unique or duplicated
//// rows for a given DataFrame
//func uniqueRowsMap(df DataFrame) map[string]u {
//uniqueRows := make(map[string]u)
//for i := 0; i < df.nRows; i++ {
//str := ""
//for _, v := range df.colNames {
//col := df.Columns[v]
//str += col.colType
//str += col.getRowStr(i)
//}
//if a, ok := uniqueRows[str]; ok {
//a.unique = false
//a.appears = append(a.appears, i)
//uniqueRows[str] = a
//} else {
//uniqueRows[str] = u{true, []int{i}}
//}
//}

//return uniqueRows
//}

//// Unique will return all unique rows inside a DataFrame. The order of the rows
//// will not be preserved.
//func (df DataFrame) Unique() (*DataFrame, error) {
//newDf := DataFrame{
//Columns:  initColumns(df.colNames),
//nCols:    df.nCols,
//colNames: df.colNames,
//colTypes: df.colTypes,
//}

//uniqueRows := uniqueRowsMap(df)
//for _, v := range uniqueRows {
//if v.unique {
//row, err := df.getRow(v.appears[0])
//if err != nil {
//return nil, err
//}
//newDf.addRow(*row)
//}
//}

//return &newDf, nil
//}

//// Duplicated will return all duplicated rows inside a DataFrame
//func (df DataFrame) Duplicated() (*DataFrame, error) {
//newDf := DataFrame{
//Columns:  initColumns(df.colNames),
//nCols:    df.nCols,
//colNames: df.colNames,
//colTypes: df.colTypes,
//}

//uniqueRows := uniqueRowsMap(df)
//for _, v := range uniqueRows {
//if !v.unique {
//for _, i := range v.appears {
//row, err := df.getRow(i)
//if err != nil {
//return nil, err
//}
//newDf.addRow(*row)
//}
//}
//}

//return &newDf, nil
//}

//// RemoveDuplicates will return all unique rows in a DataFrame and the first
//// appearance of all duplicated rows. The order of the rows will not be
//// preserved.
//func (df DataFrame) RemoveDuplicates() (*DataFrame, error) {
//newDf := DataFrame{
//Columns:  initColumns(df.colNames),
//nCols:    df.nCols,
//colNames: df.colNames,
//colTypes: df.colTypes,
//}

//uniqueRows := uniqueRowsMap(df)
//for _, v := range uniqueRows {
//row, err := df.getRow(v.appears[0])
//if err != nil {
//return nil, err
//}
//newDf.addRow(*row)
//}

//return &newDf, nil
//}

//// RemoveUnique will return the first appearance of the duplicated rows in
//// a DataFrame. The order of the rows will not be preserved.
//func (df DataFrame) RemoveUnique() (*DataFrame, error) {
//newDf := DataFrame{
//Columns:  initColumns(df.colNames),
//nCols:    df.nCols,
//colNames: df.colNames,
//colTypes: df.colTypes,
//}

//uniqueRows := uniqueRowsMap(df)
//for _, v := range uniqueRows {
//if !v.unique {
//row, err := df.getRow(v.appears[0])
//if err != nil {
//return nil, err
//}
//newDf.addRow(*row)
//}
//}

//return &newDf, nil
//}

// TODO: We should truncate the maximum length of shown columns and scape newline
// characters'
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
			elem, _ := df.Columns[v].Index(i)
			str += addRightPadding(formatCell(elem), df.Columns[v].numChars)
			str += "  "
		}
		str += "\n"
	}
	return str
}

// formatCell returns the value of a given element in string format. In case of
// a nil pointer the value returned will be NA.
func formatCell(cell interface{}) string {
	if reflect.TypeOf(cell).Kind() == reflect.Ptr {
		if reflect.ValueOf(cell).IsNil() {
			return "NA"
		}
		val := reflect.Indirect(reflect.ValueOf(cell)).Interface()
		return fmt.Sprint(val)
	}
	return fmt.Sprint(cell)
}
