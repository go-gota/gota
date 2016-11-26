package dataframe

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/kniren/gota/series"
)

// DataFrame is the base data structure
type DataFrame struct {
	columns Columns
	ncols   int
	nrows   int
	err     error
}

type Columns []series.Series

// New is a constructor for DataFrames
func New(series ...series.Series) DataFrame {
	if series == nil || len(series) == 0 {
		return DataFrame{err: fmt.Errorf("empty DataFrame")}
	}

	nrows := 0
	var columns Columns
	var colnames []string
	for k, s := range series {
		if s.Err() != nil {
			err := fmt.Errorf("error on series %v: %v", k, s.Err())
			return DataFrame{err: err}
		}
		columns = append(columns, s.Copy())
		colnames = append(colnames, s.Name)
		l := s.Len()
		if k > 0 && l != nrows {
			return DataFrame{err: fmt.Errorf("arguments have different dimensions")}
		}
		nrows = l
	}

	// Fill DataFrame base structure
	df := DataFrame{
		columns: columns,
		ncols:   len(series),
		nrows:   nrows,
	}
	fixColnames(&df)
	return df
}

// Copy returns a copy of the DataFrame
func (df DataFrame) Copy() DataFrame {
	copy := New(df.columns...)
	if df.Err() != nil {
		copy.err = df.Err()
	}
	return copy
}

func (df DataFrame) Err() error {
	return df.err
}

// String implements the Stringer interface for DataFrame
func (df DataFrame) String() (str string) {
	if df.Err() != nil {
		str = "Empty DataFrame:" + df.Err().Error()
		return
	}
	if df.nrows == 0 {
		str = "Empty DataFrame..."
		return
	}
	records := df.Records()
	// Add the row numbers
	for i := 0; i < df.nrows+1; i++ {
		add := ""
		if i != 0 {
			add = strconv.Itoa(i) + ":"
		}
		records[i] = append([]string{add}, records[i]...)
	}

	// Get the maximum number of characters per column
	maxChars := make([]int, df.ncols+1)
	for i := 0; i < df.nrows+1; i++ {
		for j := 0; j < df.ncols+1; j++ {
			if len(records[i][j]) > maxChars[j] {
				maxChars[j] = utf8.RuneCountInString(records[i][j])
			}
		}
	}
	for i := 0; i < df.nrows+1; i++ {
		// Add right padding to all elements
		records[i][0] = addLeftPadding(records[i][0], maxChars[0]+1)
		for j := 1; j < df.ncols+1; j++ {
			records[i][j] = addRightPadding(records[i][j], maxChars[j])
		}
		// Create the final string
		str += strings.Join(records[i], " ")
		str += "\n"
	}
	return str
}

// Subsetting, mutating and transforming DataFrame methods
// =======================================================

// Subsets returns a subset of the rows of the original DataFrame based on the
// Series subsetting indexes
func (df DataFrame) Subset(indexes series.Indexes) DataFrame {
	if df.Err() != nil {
		return df
	}
	var columns []series.Series
	for _, column := range df.columns {
		sub := column.Subset(indexes)
		if sub.Err() != nil {
			return DataFrame{err: fmt.Errorf("can't subset: %v", sub.Err())}
		}
		columns = append(columns, sub)
	}
	return New(columns...)
}

func parseSelectIndexes(l int, indexes SelectIndexes, colnames []string) ([]int, error) {
	var idx []int
	switch indexes.(type) {
	case []int:
		idx = indexes.([]int)
	case int:
		idx = []int{indexes.(int)}
	case []bool:
		bools := indexes.([]bool)
		if len(bools) != l {
			return nil, fmt.Errorf("indexing error: index dimensions mismatch")
		}
		for i, b := range bools {
			if b {
				idx = append(idx, i)
			}
		}
	case string:
		s := indexes.(string)
		i := findInStringSlice(s, colnames)
		if i < 0 {
			return nil, fmt.Errorf("can't select columns: column name \"%v\" not found", s)
		}
		idx = append(idx, i)
	case []string:
		xs := indexes.([]string)
		for _, s := range xs {
			i := findInStringSlice(s, colnames)
			if i < 0 {
				return nil, fmt.Errorf("can't select columns: column name \"%v\" not found", s)
			}
			idx = append(idx, i)
		}
	case series.Series:
		s := indexes.(series.Series)
		if err := s.Err(); err != nil {
			return nil, fmt.Errorf("indexing error: new values has errors: %v", err)
		}
		if s.HasNaN() {
			return nil, fmt.Errorf("indexing error: indexes contain NaN")
		}
		switch s.Type() {
		case series.Int:
			return s.Int()
		case series.Bool:
			bools, err := s.Bool()
			if err != nil {
				return nil, fmt.Errorf("indexing error: %v", err)
			}
			return parseSelectIndexes(l, bools, colnames)
		case series.String:
			xs := indexes.(series.Series).Records()
			return parseSelectIndexes(l, xs, colnames)
		default:
			return nil, fmt.Errorf("indexing error: unknown indexing mode")
		}
	default:
		return nil, fmt.Errorf("indexing error: unknown indexing mode")
	}
	return idx, nil
}

// SelectIndexes are the supported indexes used for the DataFrame.Select method.
// Currently supported are: []int, int, []bool, string, []string, Series (Int,
// Bool, String)
type SelectIndexes interface{}

// Select the given DataFrame columns
func (df DataFrame) Select(indexes SelectIndexes) DataFrame {
	if df.Err() != nil {
		return df
	}
	var columns Columns
	idx, err := parseSelectIndexes(df.ncols, indexes, df.Names())
	if err != nil {
		return DataFrame{err: fmt.Errorf("can't select columns: %v", err)}
	}
	for _, i := range idx {
		if i < 0 || i >= df.ncols {
			return DataFrame{err: fmt.Errorf("can't select columns: index out of range")}
		}
		columns = append(columns, df.columns[i])
	}
	return New(columns...)
}

// Rename changes the name of one of the columns of a DataFrame
func (df DataFrame) Rename(newname, oldname string) DataFrame {
	if df.Err() != nil {
		return df
	}
	// Check that colname exist on dataframe
	var copy DataFrame
	colnames := df.Names()
	if idx := findInStringSlice(oldname, colnames); idx >= 0 {
		copy = df.Copy()
		copy.columns[idx].Name = newname
	} else {
		return DataFrame{
			err: fmt.Errorf("The given colname doesn't exist"),
		}
	}
	return copy
}

// CBind combines the columns of two DataFrames
func (df DataFrame) CBind(dfb DataFrame) DataFrame {
	if df.Err() != nil {
		return df
	}
	if dfb.Err() != nil {
		return dfb
	}
	cols := append(df.columns, dfb.columns...)
	return New(cols...)
}

// RBind matches the column names of two DataFrames and returns the combination of
// the rows of both of them
func (df DataFrame) RBind(dfb DataFrame) DataFrame {
	if df.Err() != nil {
		return df
	}
	if dfb.Err() != nil {
		return dfb
	}
	var expandedSeries Columns
	for k, v := range df.Names() {
		idx := findInStringSlice(v, dfb.Names())
		if idx < 0 {
			return DataFrame{err: fmt.Errorf("rbind error: column names are not compatible")}
		}
		originalSeries := df.columns[k]
		addedSeries := dfb.columns[idx]
		newSeries := originalSeries.Concat(addedSeries)
		if err := newSeries.Err(); err != nil {
			return DataFrame{err: fmt.Errorf("rbind error: %v", err)}
		}
		expandedSeries = append(expandedSeries, newSeries)
	}
	return New(expandedSeries...)
}

//// Mutate changes a column of the DataFrame with the given Series
//func (df DataFrame) Mutate(colname string, series Series) DataFrame {
//if df.Err() != nil {
//return df
//}
//strInsideSliceIdx := func(i string, s []string) (bool, int) {
//for k, v := range s {
//if v == i {
//return true, k
//}
//}
//return false, -1
//}
//if series.Len() != df.nrows {
//return DataFrame{
//err: fmt.Errorf("Can't set column. Different dimensions"),
//}
//}
//// Check that colname exist on dataframe
//newSeries := df.columns
//if exists, idx := strInsideSliceIdx(colname, df.Names()); exists {
//newSeries[idx] = series
//} else {
//series.Name = colname
//newSeries = append(newSeries, series)
//}
//return New(newSeries...)
//}

//// F is the filtering structure
//type F struct {
//Colname    string
//Comparator Comparator
//Comparando interface{}
//}

////// Filter will filter the rows of a DataFrame
////func (df DataFrame) Filter(filters ...F) DataFrame {
////if df.Err() != nil {
////return df
////}
////strInsideSliceIdx := func(i string, s []string) (bool, int) {
////for k, v := range s {
////if v == i {
////return true, k
////}
////}
////return false, -1
////}
////var compResults []Series
////for _, f := range filters {
////if exists, idx := strInsideSliceIdx(f.Colname, df.Names()); exists {
////res := df.columns[idx].Compare(f.Comparator, f.Comparando)
////if err := res.Err(); err != nil {
////return DataFrame{
////err: err,
////}
////}
////compResults = append(compResults, res)
////} else {
////return DataFrame{
////err: fmt.Errorf("The given colname doesn't exist"),
////}
////}
////}
////// Join compResults via "OR"
////if len(compResults) == 0 {
////return df.Copy()
////}
////res := compResults[0]
////for i := 1; i < len(compResults); i++ {
////nextRes := compResults[i]
////for j := 0; j < res.Len(); j++ {
////res[j] = res[j] || nextRes[j]
////}
////}
////return df.Subset(res)
////}

//// Read/Write Methods
//// =================

//// LoadOptions is the configuration that will be used for the loading operations
//type LoadOptions struct {
//detectTypes bool
//hasHeader   bool
//types       map[string]Type
//defaultType Type
//}

//func CfgDetectTypes(b bool) func(*LoadOptions) {
//return func(c *LoadOptions) {
//c.detectTypes = b
//}
//}

//func CfgHasHeader(b bool) func(*LoadOptions) {
//return func(c *LoadOptions) {
//c.hasHeader = b
//}
//}

//func CfgColumnTypes(coltypes map[string]Type) func(*LoadOptions) {
//return func(c *LoadOptions) {
//c.types = coltypes
//}
//}

//func CfgDefaultType(t Type) func(*LoadOptions) {
//return func(c *LoadOptions) {
//c.defaultType = t
//}
//}

//func LoadRecords(records [][]string, options ...func(*LoadOptions)) DataFrame {
//// Load the options
//cfg := LoadOptions{
//types:       make(map[string]Type),
//detectTypes: true,
//defaultType: String,
//hasHeader:   true,
//}
//for _, option := range options {
//option(&cfg)
//}

//if len(records) == 0 {
//return DataFrame{err: fmt.Errorf("Empty DataFrame")}
//}
//if cfg.hasHeader && len(records) <= 1 {
//return DataFrame{err: fmt.Errorf("Empty DataFrame")}
//}

//// Extract headers
//var headers []string
//if cfg.hasHeader {
//headers = records[0]
//records = records[1:]
//} else {
//for i := 0; i < len(records[0]); i++ {
//headers = append(headers, fmt.Sprint(i))
//}
//}
//types := make([]Type, len(headers))
//var rawcols [][]string
//for i, colname := range headers {
//var rawcol []string
//for j := 0; j < len(records); j++ {
//rawcol = append(rawcol, records[j][i])
//}
//rawcols = append(rawcols, rawcol)

//t, ok := cfg.types[colname]
//if !ok {
//t = cfg.defaultType
//if cfg.detectTypes {
//t = findType(rawcol)
//}
//}
//types[i] = t
//}

//var columns []Series
//for i, colname := range headers {
//col := NewSeries(rawcols[i], types[i])
//if col.Err() != nil {
//return DataFrame{
//err: col.Err(),
//}
//}
//col.Name = colname
//columns = append(columns, col)
//}
//return New(columns...)
//}

//func LoadMaps(maps []map[string]interface{}, options ...func(*LoadOptions)) DataFrame {
//if len(maps) == 0 {
//return DataFrame{
//err: fmt.Errorf("Can't parse empty map array"),
//}
//}
//inStrSlice := func(i string, s []string) bool {
//for _, v := range s {
//if v == i {
//return true
//}
//}
//return false
//}

//// Detect all colnames
//var colnames []string
//for _, v := range maps {
//for k, _ := range v {
//if exists := inStrSlice(k, colnames); !exists {
//colnames = append(colnames, k)
//}
//}
//}
//sort.Strings(colnames)
//records := [][]string{colnames}
//for _, m := range maps {
//var row []string
//for _, colname := range colnames {
//element := ""
//val, ok := m[colname]
//if ok {
//element = fmt.Sprint(val)
//}
//row = append(row, element)
//}
//records = append(records, row)
//}

//return LoadRecords(records, options...)
//}

//func ReadJSON(r io.Reader, options ...func(*LoadOptions)) DataFrame {
//var m []map[string]interface{}
//err := json.NewDecoder(r).Decode(&m)
//if err != nil {
//return DataFrame{err: err}
//}
//return LoadMaps(m, options...)
//}

//func ReadCSV(r io.Reader, options ...func(*LoadOptions)) DataFrame {
//csvReader := csv.NewReader(r)
//records, err := csvReader.ReadAll()
//if err != nil {
//return DataFrame{err: err}
//}
//return LoadRecords(records, options...)
//}

//func (df DataFrame) WriteJSON(w io.Writer) error {
//if df.Err() != nil {
//return df.Err()
//}
//m := df.Maps()
//return json.NewEncoder(w).Encode(m)
//}

//func (df DataFrame) WriteCSV(w io.Writer) error {
//if df.Err() != nil {
//return df.Err()
//}
//records := df.Records()
//return csv.NewWriter(w).WriteAll(records)
//}

//// Getters/Setters for DataFrame fields
//// ====================================

func (df DataFrame) Names() []string {
	var colnames []string
	for _, v := range df.columns {
		colnames = append(colnames, v.Name)
	}
	return colnames
}

func (df DataFrame) Types() []series.Type {
	var coltypes []series.Type
	for _, s := range df.columns {
		coltypes = append(coltypes, s.Type())
	}
	return coltypes
}

func (df DataFrame) SetNames(colnames []string) error {
	if df.Err() != nil {
		return df.Err()
	}
	if len(colnames) != df.ncols {
		err := fmt.Errorf("Couldn't set the column names. Wrong dimensions.")
		return err
	}
	for k, v := range colnames {
		df.columns[k].Name = v
	}
	return nil
}

// Dim retrieves the dimensiosn of a DataFrame
func (df DataFrame) Dim() (dim [2]int) {
	dim[0] = df.nrows
	dim[1] = df.ncols
	return
}

// NRows is the getter method for the number of rows in a DataFrame
func (df DataFrame) Nrow() int {
	return df.nrows
}

// NCols is the getter method for the number of rows in a DataFrame
func (df DataFrame) Ncol() int {
	return df.ncols
}

//// Col returns the Series with the given column name contained in the DataFrame
//func (df DataFrame) Col(colname string) Series {
//// TODO: Accept also an int with the position of the Series
//if df.Err() != nil {
//return Series{err: df.Err()}
//}
//strInsideSliceIdx := func(i string, s []string) (bool, int) {
//for k, v := range s {
//if v == i {
//return true, k
//}
//}
//return false, -1
//}
//// Check that colname exist on dataframe
//var ret Series
//if exists, idx := strInsideSliceIdx(colname, df.Names()); exists {
//ret = df.columns[idx].Copy()
//} else {
//return Series{
//err: fmt.Errorf("The given colname doesn't exist"),
//}
//}
//return ret
//}

//// InnerJoin returns a DataFrame containing the inner join of two DataFrames.
//// This operation matches all rows that appear on both dataframes.
//func (df DataFrame) InnerJoin(b DataFrame, keys ...string) DataFrame {
//if len(keys) == 0 {
//return DataFrame{err: fmt.Errorf("Unspecified Join keys")}
//}
//// Check that we have all given keys in both DataFrames
//errorArr := []string{}
//var iKeysA []int
//var iKeysB []int
//for _, key := range keys {
//i := df.ColIndex(key)
//if i < 0 {
//errorArr = append(errorArr, fmt.Sprint("Can't find key \"", key, "\" on left DataFrame"))
//}
//iKeysA = append(iKeysA, i)
//j := b.ColIndex(key)
//if j < 0 {
//errorArr = append(errorArr, fmt.Sprint("Can't find key '", key, "' on left DataFrame"))
//}
//iKeysB = append(iKeysB, j)
//}
//if len(errorArr) != 0 {
//return DataFrame{err: fmt.Errorf(strings.Join(errorArr, "\n"))}
//}

//aCols := df.columns
//bCols := b.columns
//// Initialize newCols
//var newCols []Series
//for _, i := range iKeysA {
//newCols = append(newCols, aCols[i].Empty())
//}
//var iNotKeysA []int
//for i := 0; i < df.ncols; i++ {
//if !inIntSlice(i, iKeysA) {
//iNotKeysA = append(iNotKeysA, i)
//newCols = append(newCols, aCols[i].Empty())
//}
//}
//var iNotKeysB []int
//for i := 0; i < b.ncols; i++ {
//if !inIntSlice(i, iKeysB) {
//iNotKeysB = append(iNotKeysB, i)
//newCols = append(newCols, bCols[i].Empty())
//}
//}

//// Fill newCols
//for i := 0; i < df.nrows; i++ {
//for j := 0; j < b.nrows; j++ {
//match := true
//for k := range keys {
//aElem := aCols[iKeysA[k]].elem(i)
//bElem := bCols[iKeysB[k]].elem(j)
//match = match && aElem.Eq(bElem)
//}
//if match {
//ii := 0
//for _, k := range iKeysA {
//elem := aCols[k].elem(i)
//newCols[ii].Append(elem)
//ii++
//}
//for _, k := range iNotKeysA {
//elem := aCols[k].elem(i)
//newCols[ii].Append(elem)
//ii++
//}
//for _, k := range iNotKeysB {
//elem := bCols[k].elem(j)
//newCols[ii].Append(elem)
//ii++
//}
//}
//}
//}
//return New(newCols...)
//}

//// LeftJoin returns a DataFrame containing the left join of two DataFrames.
//// This operation matches all rows that appear on both dataframes.
//func (df DataFrame) LeftJoin(b DataFrame, keys ...string) DataFrame {
//if len(keys) == 0 {
//return DataFrame{err: fmt.Errorf("Unspecified Join keys")}
//}
//// Check that we have all given keys in both DataFrames
//errorArr := []string{}
//var iKeysA []int
//var iKeysB []int
//for _, key := range keys {
//i := df.ColIndex(key)
//if i < 0 {
//errorArr = append(errorArr, fmt.Sprint("Can't find key \"", key, "\" on left DataFrame"))
//}
//iKeysA = append(iKeysA, i)
//j := b.ColIndex(key)
//if j < 0 {
//errorArr = append(errorArr, fmt.Sprint("Can't find key '", key, "' on left DataFrame"))
//}
//iKeysB = append(iKeysB, j)
//}
//if len(errorArr) != 0 {
//return DataFrame{err: fmt.Errorf(strings.Join(errorArr, "\n"))}
//}

//aCols := df.columns
//bCols := b.columns
//// Initialize newCols
//var newCols []Series
//for _, i := range iKeysA {
//newCols = append(newCols, aCols[i].Empty())
//}
//var iNotKeysA []int
//for i := 0; i < df.ncols; i++ {
//if !inIntSlice(i, iKeysA) {
//iNotKeysA = append(iNotKeysA, i)
//newCols = append(newCols, aCols[i].Empty())
//}
//}
//var iNotKeysB []int
//for i := 0; i < b.ncols; i++ {
//if !inIntSlice(i, iKeysB) {
//iNotKeysB = append(iNotKeysB, i)
//newCols = append(newCols, bCols[i].Empty())
//}
//}

//// Fill newCols
//for i := 0; i < df.nrows; i++ {
//matched := false
//for j := 0; j < b.nrows; j++ {
//match := true
//for k := range keys {
//aElem := aCols[iKeysA[k]].elem(i)
//bElem := bCols[iKeysB[k]].elem(j)
//match = match && aElem.Eq(bElem)
//}
//if match {
//matched = true
//ii := 0
//for _, k := range iKeysA {
//elem := aCols[k].elem(i)
//newCols[ii].Append(elem)
//ii++
//}
//for _, k := range iNotKeysA {
//elem := aCols[k].elem(i)
//newCols[ii].Append(elem)
//ii++
//}
//for _, k := range iNotKeysB {
//elem := bCols[k].elem(j)
//newCols[ii].Append(elem)
//ii++
//}
//}
//}
//if !matched {
//ii := 0
//for _, k := range iKeysA {
//elem := aCols[k].elem(i)
//newCols[ii].Append(elem)
//ii++
//}
//for _, k := range iNotKeysA {
//elem := aCols[k].elem(i)
//newCols[ii].Append(elem)
//ii++
//}
//for _, _ = range iNotKeysB {
//newCols[ii].Append(nil)
//ii++
//}
//}
//}
//return New(newCols...)
//}

//// RightJoin returns a DataFrame containing the right join of two DataFrames.
//// This operation matches all rows that appear on both dataframes.
//func (df DataFrame) RightJoin(b DataFrame, keys ...string) DataFrame {
//if len(keys) == 0 {
//return DataFrame{err: fmt.Errorf("Unspecified Join keys")}
//}
//// Check that we have all given keys in both DataFrames
//errorArr := []string{}
//var iKeysA []int
//var iKeysB []int
//for _, key := range keys {
//i := df.ColIndex(key)
//if i < 0 {
//errorArr = append(errorArr, fmt.Sprint("Can't find key \"", key, "\" on left DataFrame"))
//}
//iKeysA = append(iKeysA, i)
//j := b.ColIndex(key)
//if j < 0 {
//errorArr = append(errorArr, fmt.Sprint("Can't find key '", key, "' on left DataFrame"))
//}
//iKeysB = append(iKeysB, j)
//}
//if len(errorArr) != 0 {
//return DataFrame{err: fmt.Errorf(strings.Join(errorArr, "\n"))}
//}

//aCols := df.columns
//bCols := b.columns
//// Initialize newCols
//var newCols []Series
//for _, i := range iKeysA {
//newCols = append(newCols, aCols[i].Empty())
//}
//var iNotKeysA []int
//for i := 0; i < df.ncols; i++ {
//if !inIntSlice(i, iKeysA) {
//iNotKeysA = append(iNotKeysA, i)
//newCols = append(newCols, aCols[i].Empty())
//}
//}
//var iNotKeysB []int
//for i := 0; i < b.ncols; i++ {
//if !inIntSlice(i, iKeysB) {
//iNotKeysB = append(iNotKeysB, i)
//newCols = append(newCols, bCols[i].Empty())
//}
//}

//// Fill newCols
//var yesmatched []struct{ i, j int }
//var nonmatched []int
//for j := 0; j < b.nrows; j++ {
//matched := false
//for i := 0; i < df.nrows; i++ {
//match := true
//for k := range keys {
//aElem := aCols[iKeysA[k]].elem(i)
//bElem := bCols[iKeysB[k]].elem(j)
//match = match && aElem.Eq(bElem)
//}
//if match {
//matched = true
//yesmatched = append(yesmatched, struct{ i, j int }{i, j})
//}
//}
//if !matched {
//nonmatched = append(nonmatched, j)
//}
//}
//for _, v := range yesmatched {
//i := v.i
//j := v.j
//ii := 0
//for _, k := range iKeysA {
//elem := aCols[k].elem(i)
//newCols[ii].Append(elem)
//ii++
//}
//for _, k := range iNotKeysA {
//elem := aCols[k].elem(i)
//newCols[ii].Append(elem)
//ii++
//}
//for _, k := range iNotKeysB {
//elem := bCols[k].elem(j)
//newCols[ii].Append(elem)
//ii++
//}
//}
//for _, j := range nonmatched {
//ii := 0
//for _, k := range iKeysB {
//elem := bCols[k].elem(j)
//newCols[ii].Append(elem)
//ii++
//}
//for _, _ = range iNotKeysA {
//newCols[ii].Append(nil)
//ii++
//}
//for _, k := range iNotKeysB {
//elem := bCols[k].elem(j)
//newCols[ii].Append(elem)
//ii++
//}
//}
//return New(newCols...)
//}

//// OuterJoin returns a DataFrame containing the outer join of two DataFrames.
//// This operation matches all rows that appear on both dataframes.
//func (df DataFrame) OuterJoin(b DataFrame, keys ...string) DataFrame {
//if len(keys) == 0 {
//return DataFrame{err: fmt.Errorf("Unspecified Join keys")}
//}
//// Check that we have all given keys in both DataFrames
//errorArr := []string{}
//var iKeysA []int
//var iKeysB []int
//for _, key := range keys {
//i := df.ColIndex(key)
//if i < 0 {
//errorArr = append(errorArr, fmt.Sprint("Can't find key \"", key, "\" on left DataFrame"))
//}
//iKeysA = append(iKeysA, i)
//j := b.ColIndex(key)
//if j < 0 {
//errorArr = append(errorArr, fmt.Sprint("Can't find key '", key, "' on left DataFrame"))
//}
//iKeysB = append(iKeysB, j)
//}
//if len(errorArr) != 0 {
//return DataFrame{err: fmt.Errorf(strings.Join(errorArr, "\n"))}
//}

//aCols := df.columns
//bCols := b.columns
//// Initialize newCols
//var newCols []Series
//for _, i := range iKeysA {
//newCols = append(newCols, aCols[i].Empty())
//}
//var iNotKeysA []int
//for i := 0; i < df.ncols; i++ {
//if !inIntSlice(i, iKeysA) {
//iNotKeysA = append(iNotKeysA, i)
//newCols = append(newCols, aCols[i].Empty())
//}
//}
//var iNotKeysB []int
//for i := 0; i < b.ncols; i++ {
//if !inIntSlice(i, iKeysB) {
//iNotKeysB = append(iNotKeysB, i)
//newCols = append(newCols, bCols[i].Empty())
//}
//}

//// Fill newCols
//for i := 0; i < df.nrows; i++ {
//matched := false
//for j := 0; j < b.nrows; j++ {
//match := true
//for k := range keys {
//aElem := aCols[iKeysA[k]].elem(i)
//bElem := bCols[iKeysB[k]].elem(j)
//match = match && aElem.Eq(bElem)
//}
//if match {
//matched = true
//ii := 0
//for _, k := range iKeysA {
//elem := aCols[k].elem(i)
//newCols[ii].Append(elem)
//ii++
//}
//for _, k := range iNotKeysA {
//elem := aCols[k].elem(i)
//newCols[ii].Append(elem)
//ii++
//}
//for _, k := range iNotKeysB {
//elem := bCols[k].elem(j)
//newCols[ii].Append(elem)
//ii++
//}
//}
//}
//if !matched {
//ii := 0
//for _, k := range iKeysA {
//elem := aCols[k].elem(i)
//newCols[ii].Append(elem)
//ii++
//}
//for _, k := range iNotKeysA {
//elem := aCols[k].elem(i)
//newCols[ii].Append(elem)
//ii++
//}
//for _, _ = range iNotKeysB {
//newCols[ii].Append(nil)
//ii++
//}
//}
//}
//for j := 0; j < b.nrows; j++ {
//matched := false
//for i := 0; i < df.nrows; i++ {
//match := true
//for k := range keys {
//aElem := aCols[iKeysA[k]].elem(i)
//bElem := bCols[iKeysB[k]].elem(j)
//match = match && aElem.Eq(bElem)
//}
//if match {
//matched = true
//}
//}
//if !matched {
//ii := 0
//for _, k := range iKeysB {
//elem := bCols[k].elem(j)
//newCols[ii].Append(elem)
//ii++
//}
//for _ = range iNotKeysA {
//newCols[ii].Append(nil)
//ii++
//}
//for _, k := range iNotKeysB {
//elem := bCols[k].elem(j)
//newCols[ii].Append(elem)
//ii++
//}
//}
//}
//return New(newCols...)
//}

//// CrossJoin returns a DataFrame containing the cross join of two DataFrames.
//// This operation matches all rows that appear on both dataframes.
//func (df DataFrame) CrossJoin(b DataFrame) DataFrame {
//aCols := df.columns
//bCols := b.columns
//// Initialize newCols
//var newCols []Series
//for i := 0; i < df.ncols; i++ {
//newCols = append(newCols, aCols[i].Empty())
//}
//for i := 0; i < b.ncols; i++ {
//newCols = append(newCols, bCols[i].Empty())
//}
//// Fill newCols
//for i := 0; i < df.nrows; i++ {
//for j := 0; j < b.nrows; j++ {
//for ii := 0; ii < df.ncols; ii++ {
//elem := aCols[ii].elem(i)
//newCols[ii].Append(elem)
//}
//for ii := 0; ii < b.ncols; ii++ {
//jj := ii + df.ncols
//elem := bCols[ii].elem(j)
//newCols[jj].Append(elem)
//}
//}
//}
//return New(newCols...)
//}

//// ColIndex returns the index of the column with name `s`. If it fails to find the
//// column it returns -1 instead.
//func (df DataFrame) ColIndex(s string) int {
//for k, v := range df.Names() {
//if v == s {
//return k
//}
//}
//return -1
//}

func (df DataFrame) Records() [][]string {
	var records [][]string
	records = append(records, df.Names())
	if df.ncols == 0 || df.nrows == 0 {
		return records
	}
	var tRecords [][]string
	for _, col := range df.columns {
		tRecords = append(tRecords, col.Records())
	}
	records = append(records, transposeRecords(tRecords)...)
	return records
}

//func (df DataFrame) Maps() []map[string]interface{} {
//maps := make([]map[string]interface{}, df.nrows)
//colnames := df.Names()
//for i := 0; i < df.nrows; i++ {
//m := make(map[string]interface{})
//for k, v := range colnames {
//val, _ := df.columns[k].Val(i) // Ignoring the error as the index should not be out of bounds
//m[v] = val
//}
//maps[i] = m
//}
//return maps
//}

//func (df DataFrame) Dense() (*mat64.Dense, error) {
//if df.Err() != nil {
//return nil, df.Err()
//}
//var floats []float64
//for _, col := range df.columns {
//floats = append(floats, col.Float()...)
//}
//dense := mat64.NewDense(df.nrows, df.ncols, floats)
//return dense, nil
//}

// fixColnames assigns a name to the missing column names and makes it so that the
// column names are unique.
func fixColnames(df *DataFrame) {
	// Find duplicated colnames
	colnames := df.Names()
	dupnamesidx := make(map[string][]int)
	var missingnames []int
	for i := 0; i < len(colnames); i++ {
		a := colnames[i]
		if a == "" {
			missingnames = append(missingnames, i)
			continue
		}
		for j := 0; j < len(colnames); j++ {
			b := colnames[j]
			if i != j && a == b {
				temp := dupnamesidx[a]
				dupnamesidx[a] = append(temp, i)
			}
		}
	}

	// Autofill missing column names
	counter := 0
	for _, i := range missingnames {
		proposedName := fmt.Sprintf("X%v", counter)
		for findInStringSlice(proposedName, colnames) >= 0 {
			counter++
			proposedName = fmt.Sprintf("X%v", counter)
		}
		colnames[i] = proposedName
		df.columns[i].Name = proposedName
		counter++
	}

	// Sort map keys to make sure it always follows the same order
	var keys []string
	for k := range dupnamesidx {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Add a suffix to the duplicated colnames
	for _, name := range keys {
		idx := dupnamesidx[name]
		if name == "" {
			name = "X"
		}
		counter := 0
		for _, i := range idx {
			proposedName := fmt.Sprintf("%v_%v", name, counter)
			for findInStringSlice(proposedName, colnames) >= 0 {
				counter++
				proposedName = fmt.Sprintf("%v_%v", name, counter)
			}
			colnames[i] = proposedName
			df.columns[i].Name = proposedName
			counter++
		}
	}
}

func findInStringSlice(str string, s []string) int {
	for i, e := range s {
		if e == str {
			return i
		}
	}
	return -1
}
