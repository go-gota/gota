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

type comparator int

const (
	eq comparator = iota
	neq
	gt
	lt
	get
	let
)

// Cell is the interface that every cell in a DataFrame needs to comply with
type Cell interface {
	String() string
	Int() (*int, error)
	Float() (*float64, error)
	Bool() (*bool, error)
	NA() Cell
	IsNA() bool
	Checksum() [16]byte
	Copy() Cell
	Compare(Cell, comparator) (*bool, error)
}

// Cells is a wrapper for a slice of Cells
type Cells []Cell

type tointeger interface {
	Int() (*int, error)
}

type tofloat interface {
	Float() (*float64, error)
}

type tobool interface {
	Bool() (*bool, error)
}

// DataFrame is the base data structure
type DataFrame struct {
	Columns  columns
	colNames []string
	colTypes []string
	nCols    int
	nRows    int
}

// C represents a way to pass Colname and Elements to a DF constructor
type C struct {
	Colname  string
	Elements Cells
}

// T is used to represent the association between a column and it't type
type T map[string]string

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

//type Error struct {
//errorType Err
//}

//const defaultDateFormat = "2006-01-02"

// TODO: Implement a custom Error type that stores information about the type of
// error and the severity of it (Warning vs Error)
// Error types
//type Err int

//const (
//FormatError Err = iota
//UnknownType
//Warning
//Etc
//)

// TODO: Use enumns for type parsing declaration:
//   type parseType int
//   const (
//       String int = iota
//       Int
//       Float
//   )

// New is a constructor for DataFrames
func New(colConst ...C) (*DataFrame, error) {
	if len(colConst) == 0 {
		return nil, errors.New("Can't create empty DataFrame")
	}

	var colLength int
	colNames := []string{}
	colTypes := []string{}
	cols := columns{}
	for k, val := range colConst {
		col, err := newCol(val.Colname, val.Elements)
		if err != nil {
			return nil, err
		}

		// Check that the length of all columns are the same
		if k == 0 {
			colLength = len(col.cells)
		} else {
			if colLength != len(col.cells) {
				return nil, errors.New("columns don't have the same dimensions")
			}
		}
		cols = append(cols, *col)
		colNames = append(colNames, col.colName)
		colTypes = append(colTypes, col.colType)
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

// Names is the getter method for the column names
func (df DataFrame) Names() []string {
	return df.colNames
}

func (df DataFrame) copy() DataFrame {
	colnames := make([]string, len(df.colNames))
	for k, v := range df.colNames {
		colnames[k] = v
	}
	coltypes := make([]string, len(df.colTypes))
	for k, v := range df.colTypes {
		coltypes[k] = v
	}
	columns := make(columns, len(df.Columns))
	for k, v := range df.Columns {
		columns[k] = v.copy()
	}
	dfc := DataFrame{
		colNames: colnames,
		colTypes: coltypes,
		Columns:  columns,
		nRows:    df.nRows,
		nCols:    df.nCols,
	}
	return dfc
}

// SetNames let us specify the column names of a DataFrame
func (df *DataFrame) SetNames(colnames []string) error {
	if len(df.colNames) != len(colnames) {
		return errors.New("Different sizes for colnames array")
	}

	for k := range df.Columns {
		df.Columns[k].colName = colnames[k]
		df.Columns[k].recountNumChars()
		df.colNames[k] = colnames[k]
	}
	return nil
}

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

	cols := columns{}
	// Fill the columns on the DataFrame
	for j := 0; j < nCols; j++ {
		colstrarr := []string{}
		for i := 1; i < nRows+1; i++ {
			colstrarr = append(colstrarr, records[i][j])
		}

		col, err := newCol(colnames[j], Strings(colstrarr))
		if err != nil {
			return err
		}

		cols = append(cols, *col)
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
		for k := range df.Columns {
			col := df.Columns[k].copy()
			err := col.ParseColumn(types[k])
			if err != nil {
				return err
			}
			df.colTypes[k] = col.colType
		}
	case T:
		types := types.(T)
		for k, v := range types {
			i, err := df.colIndex(k)
			if err != nil {
				return err
			}
			col := df.Columns[*i].copy()
			err = col.ParseColumn(v)
			if err != nil {
				return err
			}
			colIndex, _ := df.colIndex(k)
			df.colTypes[*colIndex] = col.colType
			df.Columns[*colIndex] = col
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
		for _, v := range df.Columns {
			r = append(r, v.cells[i].String())
		}
		records = append(records, r)
	}

	return records
}

//// TODO: Save to other formats. JSON? XML?

// Dim will return the current dimensions of the DataFrame in a two element array
// where the first element is the number of rows and the second the number of
// columns.
func (df DataFrame) Dim() (dim [2]int) {
	dim[0] = df.nRows
	dim[1] = df.nCols
	return
}

// NRows is the getter method for the number of rows in a DataFrame
func (df DataFrame) NRows() int {
	return df.nRows
}

// NCols is the getter method for the number of rows in a DataFrame
func (df DataFrame) NCols() int {
	return df.nCols
}

// colIndex tries to find the column index for a given column name
func (df DataFrame) colIndex(colname string) (*int, error) {
	for k, v := range df.colNames {
		if v == colname {
			return &k, nil
		}
	}
	return nil, errors.New("Can't find the given column:")
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
	newDf := df.copy()

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
		newDf.colNames = newDf.colNames[s.From:s.To]
		newDf.colTypes = newDf.colTypes[s.From:s.To]
		newDf.Columns = newDf.Columns[s.From:s.To]
	case []int:
		colNums := subset.([]int)
		if len(colNums) == 0 {
			return nil, errors.New("Empty subset")
		}

		// Check for errors
		colNumsMap := make(map[int]bool)
		for _, v := range colNums {
			if v >= newDf.nCols || v < 0 {
				return nil, errors.New("Subset out of range")
			}
			if _, ok := colNumsMap[v]; !ok {
				colNumsMap[v] = true
			} else {
				return nil, errors.New("Duplicated column numbers")
			}
		}

		cols := columns{}
		colNames := []string{}
		colTypes := []string{}
		for _, v := range colNums {
			col := newDf.Columns[v]
			cols = append(cols, col)
			colNames = append(colNames, newDf.colNames[v])
			colTypes = append(colTypes, newDf.colTypes[v])
		}
		newDf.Columns = cols
		newDf.colNames = colNames
		newDf.colTypes = colTypes
	case []string:
		cols := subset.([]string)
		if len(cols) == 0 {
			return nil, errors.New("Empty subset")
		}

		// Check for duplicated cols
		colnamesMap := make(map[string]bool)
		dupedColnames := []string{}
		for _, v := range cols {
			if v != "" {
				if _, ok := colnamesMap[v]; !ok {
					colnamesMap[v] = true
				} else {
					dupedColnames = append(dupedColnames, v)
				}
			}
		}
		if len(dupedColnames) != 0 {
			return nil, errors.New(fmt.Sprint("Duplicated column names:", dupedColnames))
		}

		// Select the desired subset of columns
		columns := columns{}
		colNames := []string{}
		colTypes := []string{}
		for _, v := range cols {
			i, err := newDf.colIndex(v)
			if err != nil {
				return nil, err
			}

			col := newDf.Columns[*i]
			columns = append(columns, col)
			colNames = append(colNames, v)
			colTypes = append(colTypes, newDf.colTypes[*i])
		}
		newDf.Columns = columns
		newDf.colNames = colNames
		newDf.colTypes = colTypes
	default:
		return nil, errors.New("Unknown subsetting option")
	}

	newDf.nCols = len(newDf.colNames)

	return &newDf, nil
}

// SubsetRows will return a DataFrame that contains only the selected rows
func (df DataFrame) SubsetRows(subset interface{}) (*DataFrame, error) {
	// Generate a DataFrame to store the temporary values
	newDf := df.copy()

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
		columns := columns{}
		for _, v := range newDf.Columns {
			col, err := newCol(v.colName, v.cells[s.From:s.To])
			if err != nil {
				return nil, err
			}
			col.recountNumChars()
			columns = append(columns, *col)
		}
		newDf.Columns = columns
	case []int:
		rowNums := subset.([]int)

		if len(rowNums) == 0 {
			return nil, errors.New("Empty subset")
		}

		// Check for errors
		for _, v := range rowNums {
			if v >= df.nRows {
				return nil, errors.New("Subset out of range")
			}
		}

		newDf.nRows = len(rowNums)
		columns := columns{}
		for _, v := range newDf.Columns {
			cells := Cells{}

			for _, i := range rowNums {
				if i < 0 {
					cells = append(cells, v.empty)
				} else {
					cells = append(cells, v.cells[i])
				}
			}

			col, err := newCol(v.colName, cells)
			if err != nil {
				return nil, err
			}

			col.recountNumChars()
			columns = append(columns, *col)
		}
		newDf.Columns = columns
	default:
		return nil, errors.New("Unknown subsetting option")
	}

	return &newDf, nil
}

// Rbind combines the rows of two dataframes
func Rbind(a DataFrame, b DataFrame) (*DataFrame, error) {
	dfa := a.copy()
	dfb := b.copy()
	// Check that the given DataFrame contains the same number of columns that the
	// current dataframe.
	if dfa.nCols != dfb.nCols {
		return nil, errors.New("Different number of columns")
	}

	// Check that the names and the types of all columns are the same
	colNameTypeMap := make(map[string]string)
	for k, v := range dfa.colNames {
		colNameTypeMap[v] = dfa.colTypes[k]
	}

	for k, v := range dfb.colNames {
		if dfType, ok := colNameTypeMap[v]; ok {
			if dfType != dfb.colTypes[k] {
				return nil, errors.New("Mismatching column types")
			}
		} else {
			return nil, errors.New("Mismatching column names")
		}
	}

	cols := columns{}
	for _, v := range dfa.colNames {
		i, err := dfa.colIndex(v)
		if err != nil {
			return nil, err
		}
		j, err := dfb.colIndex(v)
		if err != nil {
			return nil, err
		}
		col := dfa.Columns[*i]
		col, err = col.append(dfb.Columns[*j].cells...)
		if err != nil {
			return nil, err
		}
		cols = append(cols, col)
	}
	dfa.Columns = cols
	dfa.nRows += dfb.nRows

	return &dfa, nil
}

// Cbind combines the columns of two DataFrames
func Cbind(a DataFrame, b DataFrame) (*DataFrame, error) {
	dfa := a.copy()
	dfb := b.copy()
	// Check that the two DataFrames contains the same number of rows
	if dfa.nRows != dfb.nRows {
		return nil, errors.New("Different number of rows")
	}

	// Check that the column names are unique when combined
	colNameMap := make(map[string]bool)
	for _, v := range dfa.colNames {
		colNameMap[v] = true
	}

	for _, v := range dfb.colNames {
		if _, ok := colNameMap[v]; ok {
			return nil, errors.New("Conflicting column names")
		}
	}

	dfa.colNames = append(dfa.colNames, dfb.colNames...)
	dfa.colTypes = append(dfa.colTypes, dfb.colTypes...)
	dfa.nCols = len(dfa.colNames)
	dfa.Columns = append(dfa.Columns, dfb.Columns...)

	return &dfa, nil
}

type b []byte

// uniqueRowsMap is a helper function that will get a map of unique or duplicated
// rows for a given DataFrame
func uniqueRowsMap(df DataFrame) map[string]u {
	uniqueRows := make(map[string]u)
	for i := 0; i < df.nRows; i++ {
		mdarr := []byte{}
		for _, v := range df.Columns {
			cs := v.cells[i].Checksum()
			mdarr = append(mdarr, cs[:]...)
		}
		str := string(mdarr)
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
	uniqueRows := uniqueRowsMap(df)
	appears := []int{}
	for _, v := range uniqueRows {
		if v.unique {
			appears = append(appears, v.appears[0])
		}
	}

	return df.SubsetRows(appears)
}

// RemoveUnique will return all duplicated rows inside a DataFrame
func (df DataFrame) RemoveUnique() (*DataFrame, error) {
	uniqueRows := uniqueRowsMap(df)
	appears := []int{}
	for _, v := range uniqueRows {
		if !v.unique {
			appears = append(appears, v.appears...)
		}
	}

	return df.SubsetRows(appears)
}

// RemoveDuplicated will return all unique rows in a DataFrame and the first
// appearance of all duplicated rows. The order of the rows will not be
// preserved.
func (df DataFrame) RemoveDuplicated() (*DataFrame, error) {
	uniqueRows := uniqueRowsMap(df)
	appears := []int{}
	for _, v := range uniqueRows {
		appears = append(appears, v.appears[0])
	}

	return df.SubsetRows(appears)
}

// Duplicated will return the first appearance of the duplicated rows in
// a DataFrame. The order of the rows will not be preserved.
func (df DataFrame) Duplicated() (*DataFrame, error) {
	uniqueRows := uniqueRowsMap(df)
	appears := []int{}
	for _, v := range uniqueRows {
		if !v.unique {
			appears = append(appears, v.appears[0])
		}
	}

	return df.SubsetRows(appears)
}

// Implementing the Stringer interface for DataFrame
func (df DataFrame) String() (str string) {
	// TODO: We should truncate the maximum length of shown columns and scape newline
	// characters'
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
		for k, v := range df.colNames {
			str += addRightPadding(v, df.Columns[k].numChars)
			str += "  "
		}
		str += "\n"
		str += "\n"
	}
	for i := 0; i < df.nRows; i++ {
		str += addLeftPadding(strconv.Itoa(i)+": ", nRowsPadding+2)
		for _, v := range df.Columns {
			elem := v.cells[i]
			str += addRightPadding(formatCell(elem), v.numChars)
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

// InnerJoin returns a DataFrame containing the inner join of two other DataFrames.
// This operation matches all rows that appear on both dataframes.
func InnerJoin(a DataFrame, b DataFrame, keys ...string) (*DataFrame, error) {
	dfa := a.copy()
	dfb := b.copy()
	// Check that we have all given keys in both DataFrames
	errorArr := []string{}
	for _, key := range keys {
		ia, erra := dfa.colIndex(key)
		ib, errb := dfb.colIndex(key)
		if erra != nil {
			errorArr = append(errorArr, fmt.Sprint("Can't find key \"", key, "\" on left DataFrame"))
		}
		if errb != nil {
			errorArr = append(errorArr, fmt.Sprint("Can't find key \"", key, "\" on right DataFrame"))
		}
		// Check that the column types are the same between DataFrames
		if ia != nil && ib != nil {
			ta := dfa.colTypes[*ia]
			tb := dfb.colTypes[*ib]
			if ta != tb {
				errorArr = append(errorArr, fmt.Sprint("Different types for key\"", key, "\". Left:", ta, " Right:", tb))
			}
		}
	}
	if len(errorArr) != 0 {
		return nil, errors.New(strings.Join(errorArr, "\n"))
	}

	// Rename non key coumns with the same name on both DataFrames
	colnamesa := make([]string, len(dfa.colNames))
	colnamesb := make([]string, len(dfb.colNames))
	for k, v := range dfa.colNames {
		colnamesa[k] = v
	}
	for k, v := range dfb.colNames {
		colnamesb[k] = v
	}
	for k, v := range colnamesa {
		if idx, err := dfb.colIndex(v); err == nil {
			if !inStringSlice(v, keys) {
				colnamesa[k] = v + ".x"
				colnamesb[*idx] = v + ".y"
			}
		}
	}
	dfa.SetNames(colnamesa)
	dfb.SetNames(colnamesb)

	// Get the column indexes of both columns for the given keys
	colIdxa := []int{}
	colIdxb := []int{}
	for _, key := range keys {
		ia, erra := dfa.colIndex(key)
		ib, errb := dfb.colIndex(key)
		if erra == nil && errb == nil {
			colIdxa = append(colIdxa, *ia)
			colIdxb = append(colIdxb, *ib)
		}
	}

	// Get the combined checksum for all keys in both DataFrames
	checksumsa := make([][]byte, dfa.nRows)
	checksumsb := make([][]byte, dfb.nRows)
	for _, i := range colIdxa {
		for k, v := range dfa.Columns[i].cells {
			b := []byte{}
			cs := v.Checksum()
			b = append(b, cs[:]...)
			checksumsa[k] = append(checksumsa[k], b...)
		}
	}
	for _, i := range colIdxb {
		for k, v := range dfb.Columns[i].cells {
			b := []byte{}
			cs := v.Checksum()
			b = append(b, cs[:]...)
			checksumsb[k] = append(checksumsb[k], b...)
		}
	}

	// Get the indexes of the rows we want to join
	dfaIndexes := []int{}
	dfbIndexes := []int{}
	for ka, ca := range checksumsa {
		for kb, cb := range checksumsb {
			if string(ca) == string(cb) {
				dfaIndexes = append(dfaIndexes, ka)
				dfbIndexes = append(dfbIndexes, kb)
			}
		}
	}

	// Get the names of the elements that are not keys on the right DataFrame
	nokeynamesb := []string{}
	for _, v := range dfb.colNames {
		if !inStringSlice(v, keys) {
			nokeynamesb = append(nokeynamesb, v)
		}
	}

	newdfa, _ := dfa.SubsetRows(dfaIndexes)
	newdfb, _ := dfb.Subset(nokeynamesb, dfbIndexes)
	return Cbind(*newdfa, *newdfb)
}

// CrossJoin returns a DataFrame containing the cartesian product of the rows on
// both DataFrames.
func CrossJoin(a DataFrame, b DataFrame) (*DataFrame, error) {
	dfa := a.copy()
	dfb := b.copy()
	colnamesa := make([]string, len(dfa.colNames))
	colnamesb := make([]string, len(dfb.colNames))
	for k, v := range dfb.colNames {
		colnamesb[k] = v
	}
	for k, v := range dfa.colNames {
		if idx, err := dfb.colIndex(v); err == nil {
			colnamesa[k] = v + ".x"
			colnamesb[*idx] = v + ".y"
		} else {
			colnamesa[k] = v
		}
	}
	dfa.SetNames(colnamesa)
	dfb.SetNames(colnamesb)

	// Get the indexes of the rows we want to join
	dfaIndexes := []int{}
	dfbIndexes := []int{}
	for i := 0; i < dfa.nRows; i++ {
		for j := 0; j < dfb.nRows; j++ {
			dfaIndexes = append(dfaIndexes, i)
			dfbIndexes = append(dfbIndexes, j)
		}
	}

	newdfa, _ := dfa.SubsetRows(dfaIndexes)
	newdfb, _ := dfb.SubsetRows(dfbIndexes)
	return Cbind(*newdfa, *newdfb)
}

// LeftJoin returns a DataFrame containing the left join of two other DataFrames.
// This operation matches all rows that appear on the left DataFrame and matches
// it with the existing ones on the right one, filling the missing rows on the
// right with an empty value.
func LeftJoin(a DataFrame, b DataFrame, keys ...string) (*DataFrame, error) {
	dfa := a.copy()
	dfb := b.copy()
	// Check that we have all given keys in both DataFrames
	errorArr := []string{}
	for _, key := range keys {
		ia, erra := dfa.colIndex(key)
		ib, errb := dfb.colIndex(key)
		if erra != nil {
			errorArr = append(errorArr, fmt.Sprint("Can't find key \"", key, "\" on left DataFrame"))
		}
		if errb != nil {
			errorArr = append(errorArr, fmt.Sprint("Can't find key \"", key, "\" on right DataFrame"))
		}
		// Check that the column types are the same between DataFrames
		if ia != nil && ib != nil {
			ta := dfa.colTypes[*ia]
			tb := dfb.colTypes[*ib]
			if ta != tb {
				errorArr = append(errorArr, fmt.Sprint("Different types for key\"", key, "\". Left:", ta, " Right:", tb))
			}
		}
	}
	if len(errorArr) != 0 {
		return nil, errors.New(strings.Join(errorArr, "\n"))
	}

	// Rename non key coumns with the same name on both DataFrames
	colnamesa := make([]string, len(dfa.colNames))
	colnamesb := make([]string, len(dfb.colNames))
	for k, v := range dfa.colNames {
		colnamesa[k] = v
	}
	for k, v := range dfb.colNames {
		colnamesb[k] = v
	}
	for k, v := range colnamesa {
		if !inStringSlice(v, keys) {
			if idx, err := dfb.colIndex(v); err == nil {
				colnamesa[k] = v + ".x"
				colnamesb[*idx] = v + ".y"
			}
		}
	}
	dfa.SetNames(colnamesa)
	dfb.SetNames(colnamesb)

	// Get the column indexes of both columns for the given keys
	colIdxa := []int{}
	colIdxb := []int{}
	for _, key := range keys {
		ia, erra := dfa.colIndex(key)
		ib, errb := dfb.colIndex(key)
		if erra == nil && errb == nil {
			colIdxa = append(colIdxa, *ia)
			colIdxb = append(colIdxb, *ib)
		}
	}

	// Get the combined checksum for all keys in both DataFrames
	checksumsa := make([][]byte, dfa.nRows)
	checksumsb := make([][]byte, dfb.nRows)
	for _, i := range colIdxa {
		for k, v := range dfa.Columns[i].cells {
			b := []byte{}
			cs := v.Checksum()
			b = append(b, cs[:]...)
			checksumsa[k] = append(checksumsa[k], b...)
		}
	}
	for _, i := range colIdxb {
		for k, v := range dfb.Columns[i].cells {
			b := []byte{}
			cs := v.Checksum()
			b = append(b, cs[:]...)
			checksumsb[k] = append(checksumsb[k], b...)
		}
	}

	// Get the indexes of the rows we want to join
	dfaIndexes := []int{}
	dfbIndexes := []int{}
	for ka, ca := range checksumsa {
		found := false
		for kb, cb := range checksumsb {
			if string(ca) == string(cb) {
				dfaIndexes = append(dfaIndexes, ka)
				dfbIndexes = append(dfbIndexes, kb)
				found = true
			}
		}
		if !found {
			dfaIndexes = append(dfaIndexes, ka)
			dfbIndexes = append(dfbIndexes, -1)

		}
	}

	// Get the names of the elements that are not keys on the right DataFrame
	nokeynamesb := []string{}
	for _, v := range dfb.colNames {
		if !inStringSlice(v, keys) {
			nokeynamesb = append(nokeynamesb, v)
		}
	}

	newdfa, _ := dfa.SubsetRows(dfaIndexes)
	newdfb, _ := dfb.Subset(nokeynamesb, dfbIndexes)
	return Cbind(*newdfa, *newdfb)
}

// RightJoin returns a DataFrame containing the right join of two other DataFrames.
// This operation matches all rows that appear on the right DataFrame and matches
// it with the existing ones on the left one, filling the missing rows on the
// left with an empty value.
func RightJoin(b DataFrame, a DataFrame, keys ...string) (*DataFrame, error) {
	dfa := a.copy()
	dfb := b.copy()
	// Check that we have all given keys in both DataFrames
	errorArr := []string{}
	for _, key := range keys {
		ia, erra := dfa.colIndex(key)
		ib, errb := dfb.colIndex(key)
		if erra != nil {
			errorArr = append(errorArr, fmt.Sprint("Can't find key \"", key, "\" on left DataFrame"))
		}
		if errb != nil {
			errorArr = append(errorArr, fmt.Sprint("Can't find key \"", key, "\" on right DataFrame"))
		}
		// Check that the column types are the same between DataFrames
		if ia != nil && ib != nil {
			ta := dfa.colTypes[*ia]
			tb := dfb.colTypes[*ib]
			if ta != tb {
				errorArr = append(errorArr, fmt.Sprint("Different types for key\"", key, "\". Left:", ta, " Right:", tb))
			}
		}
	}
	if len(errorArr) != 0 {
		return nil, errors.New(strings.Join(errorArr, "\n"))
	}

	// Rename non key coumns with the same name on both DataFrames
	colnamesa := make([]string, len(dfa.colNames))
	colnamesb := make([]string, len(dfb.colNames))
	for k, v := range dfa.colNames {
		colnamesa[k] = v
	}
	for k, v := range dfb.colNames {
		colnamesb[k] = v
	}
	for k, v := range colnamesa {
		if !inStringSlice(v, keys) {
			if idx, err := dfb.colIndex(v); err == nil {
				colnamesa[k] = v + ".y"
				colnamesb[*idx] = v + ".x"
			}
		}
	}
	dfa.SetNames(colnamesa)
	dfb.SetNames(colnamesb)

	// Get the column indexes of both columns for the given keys
	colIdxa := []int{}
	colIdxb := []int{}
	for _, key := range keys {
		ia, erra := dfa.colIndex(key)
		ib, errb := dfb.colIndex(key)
		if erra == nil && errb == nil {
			colIdxa = append(colIdxa, *ia)
			colIdxb = append(colIdxb, *ib)
		}
	}

	// Get the combined checksum for all keys in both DataFrames
	checksumsa := make([][]byte, dfa.nRows)
	checksumsb := make([][]byte, dfb.nRows)
	for _, i := range colIdxa {
		for k, v := range dfa.Columns[i].cells {
			b := []byte{}
			cs := v.Checksum()
			b = append(b, cs[:]...)
			checksumsa[k] = append(checksumsa[k], b...)
		}
	}
	for _, i := range colIdxb {
		for k, v := range dfb.Columns[i].cells {
			b := []byte{}
			cs := v.Checksum()
			b = append(b, cs[:]...)
			checksumsb[k] = append(checksumsb[k], b...)
		}
	}

	// Get the indexes of the rows we want to join
	dfaIndexes := []int{}
	dfbIndexes := []int{}
	for ka, ca := range checksumsa {
		found := false
		for kb, cb := range checksumsb {
			if string(ca) == string(cb) {
				dfaIndexes = append(dfaIndexes, ka)
				dfbIndexes = append(dfbIndexes, kb)
				found = true
			}
		}
		if !found {
			dfaIndexes = append(dfaIndexes, ka)
			dfbIndexes = append(dfbIndexes, -1)

		}
	}

	// Get the names of the elements that are not keys on the right DataFrame
	nokeynamesb := []string{}
	for _, v := range dfb.colNames {
		if !inStringSlice(v, keys) {
			nokeynamesb = append(nokeynamesb, v)
		}
	}

	newdfa, _ := dfa.SubsetRows(dfaIndexes)
	newdfb, _ := dfb.Subset(nokeynamesb, dfbIndexes)
	return Cbind(*newdfa, *newdfb)
}
