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

// ----------------------------------------------------------------------
// Type Definitions
// ----------------------------------------------------------------------

type rowable interface {
	String() string
}

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
	// TODO: Check that it is not an empty dataframe, or should we allow it?
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

// String is an alias for string to be able to implement custom methods
type String struct {
	s string
}

// ToInteger returns the integer value of String
func (s String) ToInteger() (int, error) {
	str, err := strconv.Atoi(s.s)
	if err != nil {
		return 0, errors.New("Could't convert to int")
	}
	return str, nil
}

func (s String) String() string {
	return s.s
}

// Int is an alias for string to be able to implement custom methods
type Int struct {
	i *int
}

// ToInteger returns the integer value of Int
func (s Int) ToInteger() (int, error) {
	if s.i != nil {
		return *s.i, nil
	}
	return 0, errors.New("Could't convert to int")
}

func (s Int) String() string {
	return formatCell(s.i)
}

// Strings is a constructor for a String array
func Strings(args ...interface{}) []String {
	ret := make([]String, 0, len(args))
	for _, v := range args {
		switch v.(type) {
		case int:
			s := strconv.Itoa(v.(int))
			ret = append(ret, String{s})
		case float64:
			s := strconv.FormatFloat(v.(float64), 'f', 6, 64)
			ret = append(ret, String{s})
		case []int:
			varr := v.([]int)
			for k := range varr {
				s := strconv.Itoa(varr[k])
				ret = append(ret, String{s})
			}
		case []float64:
			varr := v.([]float64)
			for k := range varr {
				s := strconv.FormatFloat(varr[k], 'f', 6, 64)
				ret = append(ret, String{s})
			}
		case string:
			ret = append(ret, String{v.(string)})
		case []string:
			varr := v.([]string)
			for k := range varr {
				ret = append(ret, String{varr[k]})
			}
		case nil:
			ret = append(ret, String{""})
		default:
			// This should only happen if v (or its elements in case of a slice)
			// implements Stringer.
			stringer := reflect.TypeOf((*fmt.Stringer)(nil)).Elem()
			s := reflect.ValueOf(v)
			switch reflect.TypeOf(v).Kind() {
			case reflect.Slice:
				if s.Len() > 0 {
					for i := 0; i < s.Len(); i++ {
						if s.Index(i).Type().Implements(stringer) {
							ret = append(ret, String{fmt.Sprint(s.Index(i).Interface())})
						} else {
							ret = append(ret, String{"NA"})
						}
					}
				}
			default:
				if s.Type().Implements(stringer) {
					ret = append(ret, String{fmt.Sprint(v)})
				} else {
					ret = append(ret, String{"NA"})
				}
			}
		}
	}

	return ret
}

// Ints is a constructor for an Int array
func Ints(args ...interface{}) []Int {
	ret := make([]Int, 0, len(args))
	for _, v := range args {
		switch v.(type) {
		case int:
			i := v.(int)
			ret = append(ret, Int{&i})
		case float64:
			f := v.(float64)
			i := int(f)
			ret = append(ret, Int{&i})
		case []int:
			varr := v.([]int)
			for k := range varr {
				ret = append(ret, Int{&varr[k]})
			}
		case []float64:
			varr := v.([]float64)
			for k := range varr {
				f := varr[k]
				i := int(f)
				ret = append(ret, Int{&i})
			}
		case []string:
			varr := v.([]string)
			for k := range varr {
				s := varr[k]
				i, err := strconv.Atoi(s)
				if err != nil {
					ret = append(ret, Int{nil})
				} else {
					ret = append(ret, Int{&i})
				}
			}
		case string:
			i, err := strconv.Atoi(v.(string))
			if err != nil {
				ret = append(ret, Int{nil})
			} else {
				ret = append(ret, Int{&i})
			}
		case nil:
			ret = append(ret, Int{nil})
		default:
			s := reflect.ValueOf(v)
			tointer := reflect.TypeOf((*tointeger)(nil)).Elem()
			switch reflect.TypeOf(v).Kind() {
			case reflect.Slice:
				if s.Len() > 0 {
					for i := 0; i < s.Len(); i++ {
						if s.Index(i).Type().Implements(tointer) {
							m := s.Index(i).MethodByName("ToInteger")
							resolvedMethod := m.Call([]reflect.Value{})
							j := resolvedMethod[0].Interface().(int)
							err := resolvedMethod[1].Interface()
							if err != nil {
								ret = append(ret, Int{nil})
							} else {
								ret = append(ret, Int{&j})
							}
						} else {
							ret = append(ret, Int{nil})
						}
					}
				}
			default:
				ret = append(ret, Int{nil})
			}
		}
	}

	return ret
}

// Column is a column inside a DataFrame, err
type Column struct {
	row      interface{}
	colType  string
	colName  string
	numChars int
}

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

// Columns is an alias for multiple columns
type Columns map[string]Column

//// T is used to represent the association between a column and it't type
//type T map[string]string

////type Error struct {
////errorType Err
////}

//// ----------------------------------------------------------------------
//// Constant definitions
//// ----------------------------------------------------------------------

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

//// ----------------------------------------------------------------------
//// DataFrame methods
//// ----------------------------------------------------------------------

//// LoadData will load the data from a multidimensional array of strings into
//// a DataFrame object.
//func (df *DataFrame) LoadData(records [][]string) error {
//// Calculate DataFrame dimensions
//nRows := len(records) - 1
//if nRows <= 0 {
//return errors.New("Empty dataframe")
//}
//colnames := records[0]
//nCols := len(colnames)

//// If colNames has empty elements we must fill it with unique colnames
//colnamesMap := make(map[string]bool)
//auxCounter := 0
//// Get unique columnenames
//for _, v := range colnames {
//if v != "" {
//if _, ok := colnamesMap[v]; !ok {
//colnamesMap[v] = true
//} else {
//return errors.New("Duplicated column names: " + v)
//}
//}
//}
//for k, v := range colnames {
//if v == "" {
//for {
//newColname := fmt.Sprint("V", auxCounter)
//auxCounter++
//if _, ok := colnamesMap[newColname]; !ok {
//colnames[k] = newColname
//colnamesMap[newColname] = true
//break
//}
//}
//}
//}

//// Generate a df to store the temporary values
//newDf := DataFrame{
//Columns:  initColumns(colnames),
//nRows:    nRows,
//nCols:    nCols,
//colNames: colnames,
//colTypes: []string{},
//}

//// Fill the columns on the DataFrame
//for j := 0; j < nCols; j++ {
//col := []string{}
//for i := 1; i < nRows+1; i++ {
//col = append(col, records[i][j])
//}
//colName := colnames[j]
//column := Column{}
//column.colName = colName
//column.numChars = len(colName)
//column.FillColumn(col)
//newDf.colTypes = append(newDf.colTypes, column.colType)
//newDf.Columns[colName] = column
//}

//*df = newDf
//return nil
//}

//// LoadAndParse will load the data from a multidimensional array of strings and
//// parse it accordingly with the given types element. The types element can be
//// a string array with matching dimensions to the number of columns or
//// a DataFrame.T object.
//func (df *DataFrame) LoadAndParse(records [][]string, types interface{}) error {
//// Initialize the DataFrame with all columns as string type
//err := df.LoadData(records)
//if err != nil {
//return err
//}

//// Parse the DataFrame columns acording to the given types
//switch types.(type) {
//case []string:
//types := types.([]string)
//if df.nCols != len(types) {
//return errors.New("Number of columns different from number of types")
//}
//for k, v := range df.colNames {
//col := df.Columns[v]
//err := col.ParseType(types[k])
//if err != nil {
//return err
//}
//col.colType = types[k]
//df.colTypes[k] = types[k]
//df.Columns[v] = col
//}
//case T:
//types := types.(T)
//for k, v := range types {
//col := df.Columns[k]
//err := col.ParseType(v)
//if err != nil {
//return err
//}
//col.colType = v
//colIndex, _ := df.colIndex(k)
//df.colTypes[*colIndex] = v
//df.Columns[k] = col
//}
//}

//return nil
//}

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

//// Dim will return the current dimensions of the DataFrame in a two element array
//// where the first element is the number of rows and the second the number of
//// columns.
//func (df DataFrame) Dim() (dim [2]int) {
//dim[0] = df.nRows
//dim[1] = df.nCols
//return
//}

//// colIndex tries to find the column index for a given column name
//func (df DataFrame) colIndex(colname string) (*int, error) {
//for k, v := range df.colNames {
//if v == colname {
//return &k, nil
//}
//}
//return nil, errors.New("Can't find the given column")
//}

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

//// Implementing the Stringer interface for DataFrame
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
			elem, _ := df.Columns[v].elementAtIndex(i)
			str += addRightPadding(formatCell(elem), df.Columns[v].numChars)
			str += "  "
		}
		str += "\n"
	}
	return str
}

//// ----------------------------------------------------------------------
//// Column Methods
//// ----------------------------------------------------------------------

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

//// getRowStr returns the string representation of a row on a given column
//func (c Column) getRowStr(i int) string {
//return formatCell(c.row[i])
//}

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

//// ----------------------------------------------------------------------
//// Columns methods
//// ----------------------------------------------------------------------

//// initColumns will initialize an empty Columns given an array of column names
//func initColumns(names []string) Columns {
//c := make(Columns)
//for _, v := range names {
//c[v] = Column{
//colName:  v,
//numChars: len(v),
//}
//}

//return c
//}

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

//// TODO: Allow custom types support, we must be able to operate on them in the way
//// we do with basic types. Maybe we could do this with an interface? For sure they
//// must implement Stringer...
