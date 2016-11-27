package dataframe

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/kniren/gota/series"
)

// DataFrame is a data structure designed for operating on table like data (Such as Excel, CSV files, SQL table results...) where
// every column have to keep type integrity. As a general rule of thumb, variables
// are stored on columns where every row of a DataFrame represents an observation
// for each variable.
//
// On the real world, data is very messy and sometimes there are non measurements
// or missing data. For this reason, DataFrame has support for NaN elements and
// allows the most common data cleaning and mungling operations such as
// subsetting, filtering, type transformations, etc. In addition to this, this
// library provides the necessary functions to concatenate DataFrames (By rows or
// columns), different Join operations (Inner, Outer, Left, Right, Cross) and the
// ability to read and write from different formats (CSV/JSON).
type DataFrame struct {
	columns []series.Series
	ncols   int
	nrows   int
	Err     error
}

// New is the generic DataFrame constructor
func New(se ...series.Series) DataFrame {
	if se == nil || len(se) == 0 {
		return DataFrame{Err: fmt.Errorf("empty DataFrame")}
	}

	nrows := 0
	var columns []series.Series
	var colnames []string
	for k, s := range se {
		if s.Err != nil {
			err := fmt.Errorf("error on series %v: %v", k, s.Err)
			return DataFrame{Err: err}
		}
		columns = append(columns, s.Copy())
		colnames = append(colnames, s.Name)
		l := s.Len()
		if k > 0 && l != nrows {
			return DataFrame{Err: fmt.Errorf("arguments have different dimensions")}
		}
		nrows = l
	}

	// Fill DataFrame base structure
	df := DataFrame{
		columns: columns,
		ncols:   len(se),
		nrows:   nrows,
	}
	fixColnames(&df)
	return df
}

// Copy returns a copy of the DataFrame
func (df DataFrame) Copy() DataFrame {
	copy := New(df.columns...)
	if df.Err != nil {
		copy.Err = df.Err
	}
	return copy
}

// String implements the Stringer interface for DataFrame
func (df DataFrame) String() (str string) {
	if df.Err != nil {
		str = "DataFrame error: " + df.Err.Error()
		return
	}
	if df.nrows == 0 {
		str = "Empty DataFrame"
		return
	}
	records := df.Records()
	// Add the row numbers
	// TODO: The row numbers should start counting at 0 to be consistent with the indexing
	for i := 0; i < df.nrows+1; i++ {
		add := ""
		if i != 0 {
			add = strconv.Itoa(i-1) + ":"
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

// Set will updated the values of a DataFrame for the rows selected via indexes.
func (df DataFrame) Set(indexes series.Indexes, newvalues DataFrame) DataFrame {
	if df.Err != nil {
		return df
	}
	if newvalues.Err != nil {
		return DataFrame{Err: fmt.Errorf("argument has errors: %v", newvalues.Err)}
	}
	if df.ncols != newvalues.ncols {
		return DataFrame{Err: fmt.Errorf("different number of columns")}
	}
	var columns []series.Series
	for i, s := range df.columns {
		columns = append(columns, s.Set(indexes, newvalues.columns[i]))
	}
	return New(columns...)
}

// Subset returns a subset of the rows of the original DataFrame based on the
// Series subsetting indexes.
func (df DataFrame) Subset(indexes series.Indexes) DataFrame {
	if df.Err != nil {
		return df
	}
	var columns []series.Series
	for _, column := range df.columns {
		sub := column.Subset(indexes)
		if sub.Err != nil {
			return DataFrame{Err: fmt.Errorf("can't subset: %v", sub.Err)}
		}
		columns = append(columns, sub)
	}
	return New(columns...)
}

// SelectIndexes are the supported indexes used for the DataFrame.Select method. Currently supported are:
//
//     int              // Matches the given index number
//     []int            // Matches all given index numbers
//     []bool           // Matches all columns marked as true
//     string           // Matches the column with the matching column name
//     []string         // Matches all columns with the matching column names
//     Series [Int]     // Same as []int
//     Series [Bool]    // Same as []bool
//     Series [String]  // Same as []string
type SelectIndexes interface{}

// Select the given DataFrame columns
func (df DataFrame) Select(indexes SelectIndexes) DataFrame {
	if df.Err != nil {
		return df
	}
	var columns []series.Series
	idx, err := parseSelectIndexes(df.ncols, indexes, df.Names())
	if err != nil {
		return DataFrame{Err: fmt.Errorf("can't select columns: %v", err)}
	}
	for _, i := range idx {
		if i < 0 || i >= df.ncols {
			return DataFrame{Err: fmt.Errorf("can't select columns: index out of range")}
		}
		columns = append(columns, df.columns[i])
	}
	return New(columns...)
}

// Rename changes the name of one of the columns of a DataFrame
func (df DataFrame) Rename(newname, oldname string) DataFrame {
	if df.Err != nil {
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
			Err: fmt.Errorf("rename: can't find column name"),
		}
	}
	return copy
}

// CBind combines the columns of two DataFrames
func (df DataFrame) CBind(dfb DataFrame) DataFrame {
	if df.Err != nil {
		return df
	}
	if dfb.Err != nil {
		return dfb
	}
	cols := append(df.columns, dfb.columns...)
	return New(cols...)
}

// RBind matches the column names of two DataFrames and returns the combination of
// the rows of both of them.
func (df DataFrame) RBind(dfb DataFrame) DataFrame {
	if df.Err != nil {
		return df
	}
	if dfb.Err != nil {
		return dfb
	}
	var expandedSeries []series.Series
	for k, v := range df.Names() {
		idx := findInStringSlice(v, dfb.Names())
		if idx < 0 {
			return DataFrame{Err: fmt.Errorf("rbind: column names are not compatible")}
		}
		originalSeries := df.columns[k]
		addedSeries := dfb.columns[idx]
		newSeries := originalSeries.Concat(addedSeries)
		if err := newSeries.Err; err != nil {
			return DataFrame{Err: fmt.Errorf("rbind: %v", err)}
		}
		expandedSeries = append(expandedSeries, newSeries)
	}
	return New(expandedSeries...)
}

// Mutate changes a column of the DataFrame with the given Series or adds it as
// a new column if the column name does not exist.
func (df DataFrame) Mutate(s series.Series) DataFrame {
	if df.Err != nil {
		return df
	}
	colname := s.Name
	if s.Len() != df.nrows {
		return DataFrame{
			Err: fmt.Errorf("mutate: wrong dimensions"),
		}
	}
	df = df.Copy()
	// Check that colname exist on dataframe
	newSeries := df.columns
	if idx := findInStringSlice(colname, df.Names()); idx >= 0 {
		newSeries[idx] = s
	} else {
		s.Name = colname
		newSeries = append(newSeries, s)
	}
	return New(newSeries...)
}

// F is the filtering structure
type F struct {
	Colname    string
	Comparator series.Comparator
	Comparando interface{}
}

// Filter will filter the rows of a DataFrame based on the given filters. All
// filters on the argument of a Filter call are aggregated as an OR operation
// whereas if we chain Filter calls, every filter will act as an AND operation
// with regards to the rest.
func (df DataFrame) Filter(filters ...F) DataFrame {
	if df.Err != nil {
		return df
	}
	var compResults []series.Series
	for _, f := range filters {
		idx := findInStringSlice(f.Colname, df.Names())
		if idx < 0 {
			return DataFrame{Err: fmt.Errorf("filter: can't find column name")}
		}
		res := df.columns[idx].Compare(f.Comparator, f.Comparando)
		if err := res.Err; err != nil {
			return DataFrame{Err: fmt.Errorf("filter: %v", err)}
		}
		compResults = append(compResults, res)
	}
	// Join compResults via "OR"
	if len(compResults) == 0 {
		return df.Copy()
	}
	res, err := compResults[0].Bool()
	if err != nil {
		return DataFrame{Err: fmt.Errorf("filter: %v", err)}
	}
	for i := 1; i < len(compResults); i++ {
		nextRes, err := compResults[i].Bool()
		if err != nil {
			return DataFrame{Err: fmt.Errorf("filter: %v", err)}
		}
		for j := 0; j < len(res); j++ {
			res[j] = res[j] || nextRes[j]
		}
	}
	return df.Subset(res)
}

// Read/Write Methods
// =================

// LoadOptions is the configuration that will be used for the loading operations
type LoadOptions struct {
	// If set, the type of each column will be automatically detected unless
	// otherwise specified.
	detectTypes bool

	// If set, the first row of the tabular structure will be used as column
	// names.
	hasHeader bool

	// The types of specific columns can be specified via column name.
	types map[string]series.Type

	// Specifies which is the default type in case detectTypes is disabled.
	defaultType series.Type
}

// CfgDetectTypes set the detectTypes option for LoadOptions.
func CfgDetectTypes(b bool) func(*LoadOptions) {
	return func(c *LoadOptions) {
		c.detectTypes = b
	}
}

// CfgHasHeader set the hasHeader option for LoadOptions.
func CfgHasHeader(b bool) func(*LoadOptions) {
	return func(c *LoadOptions) {
		c.hasHeader = b
	}
}

// CfgColumnTypes set the types option for LoadOptions.
func CfgColumnTypes(coltypes map[string]series.Type) func(*LoadOptions) {
	return func(c *LoadOptions) {
		c.types = coltypes
	}
}

// CfgDefaultType set the defaultType option for LoadOptions.
func CfgDefaultType(t series.Type) func(*LoadOptions) {
	return func(c *LoadOptions) {
		c.defaultType = t
	}
}

// LoadRecords creates a new DataFrame based on the given records.
func LoadRecords(records [][]string, options ...func(*LoadOptions)) DataFrame {
	// Load the options
	cfg := LoadOptions{
		types:       make(map[string]series.Type),
		detectTypes: true,
		defaultType: series.String,
		hasHeader:   true,
	}
	for _, option := range options {
		option(&cfg)
	}

	if len(records) == 0 {
		return DataFrame{Err: fmt.Errorf("load records: empty DataFrame")}
	}
	if cfg.hasHeader && len(records) <= 1 {
		return DataFrame{Err: fmt.Errorf("load records: empty DataFrame")}
	}

	// Extract headers
	var headers []string
	if cfg.hasHeader {
		headers = records[0]
		records = records[1:]
	} else {
		for i := 0; i < len(records[0]); i++ {
			headers = append(headers, fmt.Sprint(i))
		}
	}
	types := make([]series.Type, len(headers))
	var rawcols [][]string
	for i, colname := range headers {
		var rawcol []string
		for j := 0; j < len(records); j++ {
			rawcol = append(rawcol, records[j][i])
		}
		rawcols = append(rawcols, rawcol)

		t, ok := cfg.types[colname]
		if !ok {
			t = cfg.defaultType
			if cfg.detectTypes {
				t = findType(rawcol)
			}
		}
		types[i] = t
	}

	var columns []series.Series
	for i, colname := range headers {
		col := series.New(rawcols[i], types[i], colname)
		if col.Err != nil {
			return DataFrame{Err: col.Err}
		}
		columns = append(columns, col)
	}
	return New(columns...)
}

// LoadMaps creates a new DataFrame based on the given maps. This function assumes
// that every map on the array represents a row of observations.
func LoadMaps(maps []map[string]interface{}, options ...func(*LoadOptions)) DataFrame {
	if len(maps) == 0 {
		return DataFrame{
			Err: fmt.Errorf("load maps: empty array"),
		}
	}
	inStrSlice := func(i string, s []string) bool {
		for _, v := range s {
			if v == i {
				return true
			}
		}
		return false
	}
	// Detect all colnames
	var colnames []string
	for _, v := range maps {
		for k := range v {
			if exists := inStrSlice(k, colnames); !exists {
				colnames = append(colnames, k)
			}
		}
	}
	sort.Strings(colnames)
	records := [][]string{colnames}
	for _, m := range maps {
		var row []string
		for _, colname := range colnames {
			element := ""
			val, ok := m[colname]
			if ok {
				element = fmt.Sprint(val)
			}
			row = append(row, element)
		}
		records = append(records, row)
	}
	return LoadRecords(records, options...)
}

// ReadCSV reads a CSV file from a io.Reader and builds a DataFrame with the
// resulting records.
func ReadCSV(r io.Reader, options ...func(*LoadOptions)) DataFrame {
	csvReader := csv.NewReader(r)
	records, err := csvReader.ReadAll()
	if err != nil {
		return DataFrame{Err: err}
	}
	return LoadRecords(records, options...)
}

// ReadJSON reads a JSON array from a io.Reader and builds a DataFrame with the
// resulting records.
func ReadJSON(r io.Reader, options ...func(*LoadOptions)) DataFrame {
	var m []map[string]interface{}
	err := json.NewDecoder(r).Decode(&m)
	if err != nil {
		return DataFrame{Err: err}
	}
	return LoadMaps(m, options...)
}

// WriteCSV writes the DataFrame to the given io.Writer as a CSV file.
func (df DataFrame) WriteCSV(w io.Writer) error {
	if df.Err != nil {
		return df.Err
	}
	records := df.Records()
	return csv.NewWriter(w).WriteAll(records)
}

// WriteJSON writes the DataFrame to the given io.Writer as a JSON array.
func (df DataFrame) WriteJSON(w io.Writer) error {
	if df.Err != nil {
		return df.Err
	}
	m := df.Maps()
	return json.NewEncoder(w).Encode(m)
}

// Getters/Setters for DataFrame fields
// ====================================

// Names returns the name of the columns on a DataFrame.
func (df DataFrame) Names() []string {
	var colnames []string
	for _, v := range df.columns {
		colnames = append(colnames, v.Name)
	}
	return colnames
}

// Types returns the types of the columns on a DataFrame.
func (df DataFrame) Types() []series.Type {
	var coltypes []series.Type
	for _, s := range df.columns {
		coltypes = append(coltypes, s.Type())
	}
	return coltypes
}

// SetNames changes the column names of a DataFrame to the ones passed as an
// argument.
func (df DataFrame) SetNames(colnames []string) error {
	if df.Err != nil {
		return df.Err
	}
	if len(colnames) != df.ncols {
		err := fmt.Errorf("setting names: wrong dimensions")
		return err
	}
	for k, v := range colnames {
		df.columns[k].Name = v
	}
	return nil
}

// Dim retrieves the dimensions of a DataFrame.
func (df DataFrame) Dim() (dim [2]int) {
	dim[0] = df.nrows
	dim[1] = df.ncols
	return
}

// Nrow returns the number of rows on a DataFrame.
func (df DataFrame) Nrow() int {
	return df.nrows
}

// Ncol returns the number of columns on a DataFrame.
func (df DataFrame) Ncol() int {
	return df.ncols
}

// Col returns the Series with the given column name contained in the DataFrame.
func (df DataFrame) Col(colname string) series.Series {
	if df.Err != nil {
		return series.Series{Err: df.Err}
	}
	// Check that colname exist on dataframe
	var ret series.Series
	idx := findInStringSlice(colname, df.Names())
	if idx < 0 {
		return series.Series{
			Err: fmt.Errorf("unknown column name"),
		}
	}
	ret = df.columns[idx].Copy()
	return ret
}

// InnerJoin returns a DataFrame containing the inner join of two DataFrames.
func (df DataFrame) InnerJoin(b DataFrame, keys ...string) DataFrame {
	if len(keys) == 0 {
		return DataFrame{Err: fmt.Errorf("join keys not specified")}
	}
	// Check that we have all given keys in both DataFrames
	errorArr := []string{}
	var iKeysA []int
	var iKeysB []int
	for _, key := range keys {
		i := df.colIndex(key)
		if i < 0 {
			errorArr = append(errorArr, fmt.Sprint("can't find key \"", key, "\" on left DataFrame"))
		}
		iKeysA = append(iKeysA, i)
		j := b.colIndex(key)
		if j < 0 {
			errorArr = append(errorArr, fmt.Sprint("can't find key \"", key, "\" on right DataFrame"))
		}
		iKeysB = append(iKeysB, j)
	}
	if len(errorArr) != 0 {
		return DataFrame{Err: fmt.Errorf("%v", strings.Join(errorArr, "\n"))}
	}

	aCols := df.columns
	bCols := b.columns
	// Initialize newCols
	var newCols []series.Series
	for _, i := range iKeysA {
		newCols = append(newCols, aCols[i].Empty())
	}
	var iNotKeysA []int
	for i := 0; i < df.ncols; i++ {
		if !inIntSlice(i, iKeysA) {
			iNotKeysA = append(iNotKeysA, i)
			newCols = append(newCols, aCols[i].Empty())
		}
	}
	var iNotKeysB []int
	for i := 0; i < b.ncols; i++ {
		if !inIntSlice(i, iKeysB) {
			iNotKeysB = append(iNotKeysB, i)
			newCols = append(newCols, bCols[i].Empty())
		}
	}

	// Fill newCols
	for i := 0; i < df.nrows; i++ {
		for j := 0; j < b.nrows; j++ {
			match := true
			for k := range keys {
				aElem := aCols[iKeysA[k]].Elem(i)
				bElem := bCols[iKeysB[k]].Elem(j)
				match = match && aElem.Eq(bElem)
			}
			if match {
				ii := 0
				for _, k := range iKeysA {
					elem := aCols[k].Elem(i)
					newCols[ii].Append(elem)
					ii++
				}
				for _, k := range iNotKeysA {
					elem := aCols[k].Elem(i)
					newCols[ii].Append(elem)
					ii++
				}
				for _, k := range iNotKeysB {
					elem := bCols[k].Elem(j)
					newCols[ii].Append(elem)
					ii++
				}
			}
		}
	}
	return New(newCols...)
}

// LeftJoin returns a DataFrame containing the left join of two DataFrames.
func (df DataFrame) LeftJoin(b DataFrame, keys ...string) DataFrame {
	if len(keys) == 0 {
		return DataFrame{Err: fmt.Errorf("join keys not specified")}
	}
	// Check that we have all given keys in both DataFrames
	errorArr := []string{}
	var iKeysA []int
	var iKeysB []int
	for _, key := range keys {
		i := df.colIndex(key)
		if i < 0 {
			errorArr = append(errorArr, fmt.Sprint("can't find key \"", key, "\" on left DataFrame"))
		}
		iKeysA = append(iKeysA, i)
		j := b.colIndex(key)
		if j < 0 {
			errorArr = append(errorArr, fmt.Sprint("can't find key \"", key, "\" on right DataFrame"))
		}
		iKeysB = append(iKeysB, j)
	}
	if len(errorArr) != 0 {
		return DataFrame{Err: fmt.Errorf(strings.Join(errorArr, "\n"))}
	}

	aCols := df.columns
	bCols := b.columns
	// Initialize newCols
	var newCols []series.Series
	for _, i := range iKeysA {
		newCols = append(newCols, aCols[i].Empty())
	}
	var iNotKeysA []int
	for i := 0; i < df.ncols; i++ {
		if !inIntSlice(i, iKeysA) {
			iNotKeysA = append(iNotKeysA, i)
			newCols = append(newCols, aCols[i].Empty())
		}
	}
	var iNotKeysB []int
	for i := 0; i < b.ncols; i++ {
		if !inIntSlice(i, iKeysB) {
			iNotKeysB = append(iNotKeysB, i)
			newCols = append(newCols, bCols[i].Empty())
		}
	}

	// Fill newCols
	for i := 0; i < df.nrows; i++ {
		matched := false
		for j := 0; j < b.nrows; j++ {
			match := true
			for k := range keys {
				aElem := aCols[iKeysA[k]].Elem(i)
				bElem := bCols[iKeysB[k]].Elem(j)
				match = match && aElem.Eq(bElem)
			}
			if match {
				matched = true
				ii := 0
				for _, k := range iKeysA {
					elem := aCols[k].Elem(i)
					newCols[ii].Append(elem)
					ii++
				}
				for _, k := range iNotKeysA {
					elem := aCols[k].Elem(i)
					newCols[ii].Append(elem)
					ii++
				}
				for _, k := range iNotKeysB {
					elem := bCols[k].Elem(j)
					newCols[ii].Append(elem)
					ii++
				}
			}
		}
		if !matched {
			ii := 0
			for _, k := range iKeysA {
				elem := aCols[k].Elem(i)
				newCols[ii].Append(elem)
				ii++
			}
			for _, k := range iNotKeysA {
				elem := aCols[k].Elem(i)
				newCols[ii].Append(elem)
				ii++
			}
			for _ = range iNotKeysB {
				newCols[ii].Append(nil)
				ii++
			}
		}
	}
	return New(newCols...)
}

// RightJoin returns a DataFrame containing the right join of two DataFrames.
func (df DataFrame) RightJoin(b DataFrame, keys ...string) DataFrame {
	if len(keys) == 0 {
		return DataFrame{Err: fmt.Errorf("join keys not specified")}
	}
	// Check that we have all given keys in both DataFrames
	errorArr := []string{}
	var iKeysA []int
	var iKeysB []int
	for _, key := range keys {
		i := df.colIndex(key)
		if i < 0 {
			errorArr = append(errorArr, fmt.Sprint("can't find key \"", key, "\" on left DataFrame"))
		}
		iKeysA = append(iKeysA, i)
		j := b.colIndex(key)
		if j < 0 {
			errorArr = append(errorArr, fmt.Sprint("can't find key \"", key, "\" on right DataFrame"))
		}
		iKeysB = append(iKeysB, j)
	}
	if len(errorArr) != 0 {
		return DataFrame{Err: fmt.Errorf(strings.Join(errorArr, "\n"))}
	}

	aCols := df.columns
	bCols := b.columns
	// Initialize newCols
	var newCols []series.Series
	for _, i := range iKeysA {
		newCols = append(newCols, aCols[i].Empty())
	}
	var iNotKeysA []int
	for i := 0; i < df.ncols; i++ {
		if !inIntSlice(i, iKeysA) {
			iNotKeysA = append(iNotKeysA, i)
			newCols = append(newCols, aCols[i].Empty())
		}
	}
	var iNotKeysB []int
	for i := 0; i < b.ncols; i++ {
		if !inIntSlice(i, iKeysB) {
			iNotKeysB = append(iNotKeysB, i)
			newCols = append(newCols, bCols[i].Empty())
		}
	}

	// Fill newCols
	var yesmatched []struct{ i, j int }
	var nonmatched []int
	for j := 0; j < b.nrows; j++ {
		matched := false
		for i := 0; i < df.nrows; i++ {
			match := true
			for k := range keys {
				aElem := aCols[iKeysA[k]].Elem(i)
				bElem := bCols[iKeysB[k]].Elem(j)
				match = match && aElem.Eq(bElem)
			}
			if match {
				matched = true
				yesmatched = append(yesmatched, struct{ i, j int }{i, j})
			}
		}
		if !matched {
			nonmatched = append(nonmatched, j)
		}
	}
	for _, v := range yesmatched {
		i := v.i
		j := v.j
		ii := 0
		for _, k := range iKeysA {
			elem := aCols[k].Elem(i)
			newCols[ii].Append(elem)
			ii++
		}
		for _, k := range iNotKeysA {
			elem := aCols[k].Elem(i)
			newCols[ii].Append(elem)
			ii++
		}
		for _, k := range iNotKeysB {
			elem := bCols[k].Elem(j)
			newCols[ii].Append(elem)
			ii++
		}
	}
	for _, j := range nonmatched {
		ii := 0
		for _, k := range iKeysB {
			elem := bCols[k].Elem(j)
			newCols[ii].Append(elem)
			ii++
		}
		for _ = range iNotKeysA {
			newCols[ii].Append(nil)
			ii++
		}
		for _, k := range iNotKeysB {
			elem := bCols[k].Elem(j)
			newCols[ii].Append(elem)
			ii++
		}
	}
	return New(newCols...)
}

// OuterJoin returns a DataFrame containing the outer join of two DataFrames.
func (df DataFrame) OuterJoin(b DataFrame, keys ...string) DataFrame {
	if len(keys) == 0 {
		return DataFrame{Err: fmt.Errorf("join keys not specified")}
	}
	// Check that we have all given keys in both DataFrames
	errorArr := []string{}
	var iKeysA []int
	var iKeysB []int
	for _, key := range keys {
		i := df.colIndex(key)
		if i < 0 {
			errorArr = append(errorArr, fmt.Sprint("can't find key \"", key, "\" on left DataFrame"))
		}
		iKeysA = append(iKeysA, i)
		j := b.colIndex(key)
		if j < 0 {
			errorArr = append(errorArr, fmt.Sprint("can't find key \"", key, "\" on right DataFrame"))
		}
		iKeysB = append(iKeysB, j)
	}
	if len(errorArr) != 0 {
		return DataFrame{Err: fmt.Errorf(strings.Join(errorArr, "\n"))}
	}

	aCols := df.columns
	bCols := b.columns
	// Initialize newCols
	var newCols []series.Series
	for _, i := range iKeysA {
		newCols = append(newCols, aCols[i].Empty())
	}
	var iNotKeysA []int
	for i := 0; i < df.ncols; i++ {
		if !inIntSlice(i, iKeysA) {
			iNotKeysA = append(iNotKeysA, i)
			newCols = append(newCols, aCols[i].Empty())
		}
	}
	var iNotKeysB []int
	for i := 0; i < b.ncols; i++ {
		if !inIntSlice(i, iKeysB) {
			iNotKeysB = append(iNotKeysB, i)
			newCols = append(newCols, bCols[i].Empty())
		}
	}

	// Fill newCols
	for i := 0; i < df.nrows; i++ {
		matched := false
		for j := 0; j < b.nrows; j++ {
			match := true
			for k := range keys {
				aElem := aCols[iKeysA[k]].Elem(i)
				bElem := bCols[iKeysB[k]].Elem(j)
				match = match && aElem.Eq(bElem)
			}
			if match {
				matched = true
				ii := 0
				for _, k := range iKeysA {
					elem := aCols[k].Elem(i)
					newCols[ii].Append(elem)
					ii++
				}
				for _, k := range iNotKeysA {
					elem := aCols[k].Elem(i)
					newCols[ii].Append(elem)
					ii++
				}
				for _, k := range iNotKeysB {
					elem := bCols[k].Elem(j)
					newCols[ii].Append(elem)
					ii++
				}
			}
		}
		if !matched {
			ii := 0
			for _, k := range iKeysA {
				elem := aCols[k].Elem(i)
				newCols[ii].Append(elem)
				ii++
			}
			for _, k := range iNotKeysA {
				elem := aCols[k].Elem(i)
				newCols[ii].Append(elem)
				ii++
			}
			for _ = range iNotKeysB {
				newCols[ii].Append(nil)
				ii++
			}
		}
	}
	for j := 0; j < b.nrows; j++ {
		matched := false
		for i := 0; i < df.nrows; i++ {
			match := true
			for k := range keys {
				aElem := aCols[iKeysA[k]].Elem(i)
				bElem := bCols[iKeysB[k]].Elem(j)
				match = match && aElem.Eq(bElem)
			}
			if match {
				matched = true
			}
		}
		if !matched {
			ii := 0
			for _, k := range iKeysB {
				elem := bCols[k].Elem(j)
				newCols[ii].Append(elem)
				ii++
			}
			for _ = range iNotKeysA {
				newCols[ii].Append(nil)
				ii++
			}
			for _, k := range iNotKeysB {
				elem := bCols[k].Elem(j)
				newCols[ii].Append(elem)
				ii++
			}
		}
	}
	return New(newCols...)
}

// CrossJoin returns a DataFrame containing the cross join of two DataFrames.
func (df DataFrame) CrossJoin(b DataFrame) DataFrame {
	aCols := df.columns
	bCols := b.columns
	// Initialize newCols
	var newCols []series.Series
	for i := 0; i < df.ncols; i++ {
		newCols = append(newCols, aCols[i].Empty())
	}
	for i := 0; i < b.ncols; i++ {
		newCols = append(newCols, bCols[i].Empty())
	}
	// Fill newCols
	for i := 0; i < df.nrows; i++ {
		for j := 0; j < b.nrows; j++ {
			for ii := 0; ii < df.ncols; ii++ {
				elem := aCols[ii].Elem(i)
				newCols[ii].Append(elem)
			}
			for ii := 0; ii < b.ncols; ii++ {
				jj := ii + df.ncols
				elem := bCols[ii].Elem(j)
				newCols[jj].Append(elem)
			}
		}
	}
	return New(newCols...)
}

// colIndex returns the index of the column with name `s`. If it fails to find the
// column it returns -1 instead.
func (df DataFrame) colIndex(s string) int {
	for k, v := range df.Names() {
		if v == s {
			return k
		}
	}
	return -1
}

// Records return the string record representation of a DataFrame.
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

// Maps return the array of maps representation of a DataFrame.
func (df DataFrame) Maps() []map[string]interface{} {
	maps := make([]map[string]interface{}, df.nrows)
	colnames := df.Names()
	for i := 0; i < df.nrows; i++ {
		m := make(map[string]interface{})
		for k, v := range colnames {
			val, _ := df.columns[k].Val(i) // Ignoring the error as the index should not be out of bounds
			m[v] = val
		}
		maps[i] = m
	}
	return maps
}

//func (df DataFrame) Dense() (*mat64.Dense, error) {
//if df.Err != nil {
//return nil, df.Err
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
		if err := s.Err; err != nil {
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
