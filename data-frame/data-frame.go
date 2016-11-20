package df

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/gonum/matrix/mat64"
)

// DataFrame is the base data structure
type DataFrame struct {
	columns []Series
	ncols   int
	nrows   int
	err     error
}

// New is a constructor for DataFrames
func New(series ...Series) DataFrame {
	if series == nil || len(series) == 0 {
		return DataFrame{
			err: errors.New("No arguments given, returning empty DataFrame"),
		}
	}
	allEqual := true
	lastLength := 0
	colnames := make([]string, len(series))
	var columns []Series
	for k, v := range series {
		columns = append(columns, v.Copy())
		colnames[k] = v.Name
		l := v.Len()
		// Check that all given Series have the same length
		if k > 0 {
			allEqual = l == lastLength
			if !allEqual {
				return DataFrame{
					err: errors.New("Series have different dimensions"),
				}
			}
		}
		lastLength = l
	}
	// Fill empty colnames
	strInsideSlice := func(i string, s []string) bool {
		for _, v := range s {
			if v == i {
				return true
			}
		}
		return false
	}
	strInsideSliceIdx := func(i string, s []string, j int) (bool, []int) {
		inside := false
		var idx []int
		for k, v := range s {
			if v == i && k != j {
				inside = true
				idx = append(idx, k)
			}
		}
		return inside, idx
	}
	i := 0
	// Autofill missing column names
	for k, v := range colnames {
		if v == "" {
			proposedName := "X" + fmt.Sprint(i)
			// Make sure that we don't have duplicate column names when autofilling
			inside, _ := strInsideSliceIdx(proposedName, colnames, k)
			for inside {
				i += 1
				proposedName = "X" + fmt.Sprint(i)
				inside, _ = strInsideSliceIdx(proposedName, colnames, k)
			}
			colnames[k] = proposedName
			columns[k].Name = proposedName
			i += 1
		}
	}

	// Make sure that colnames are unique renaming them if necessary
	for k, v := range colnames {
		inside, idx := strInsideSliceIdx(v, colnames, k)
		if inside {
			idx = append([]int{k}, idx...)
			i := 0
			for _, j := range idx {
				proposedName := v + "." + fmt.Sprint(i)
				for strInsideSlice(proposedName, colnames) {
					i += 1
					proposedName = v + "." + fmt.Sprint(i)
				}
				colnames[j] = proposedName
				columns[j].Name = proposedName
				i += 1
			}
		}
	}

	// Fill DataFrame base structure
	df := DataFrame{
		columns: columns,
		ncols:   len(series),
		nrows:   lastLength,
		err:     nil,
	}
	return df
}

// Copy wil copy the values of a given DataFrame
func (df DataFrame) Copy() DataFrame {
	if df.Err() != nil {
		return df
	}
	copy := New(df.columns...)
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

// Subsets the DataFrame based on the Series subsetting rules
func (df DataFrame) Subset(indexes interface{}) DataFrame {
	if df.Err() != nil {
		return df
	}
	var columnsSubset []Series
	for _, column := range df.columns {
		columnSubset := column.Subset(indexes)
		if columnSubset.Err() != nil {
			return DataFrame{err: columnSubset.Err()}
		}
		columnsSubset = append(columnsSubset, columnSubset)
	}
	return New(columnsSubset...)
}

// Select the given DataFrame columns
func (df DataFrame) Select(colnames ...string) DataFrame {
	if df.Err() != nil {
		return df
	}
	strInsideSlice := func(i string, s []string) bool {
		for _, v := range s {
			if v == i {
				return true
			}
		}
		return false
	}
	var columnsSelected []Series
	strInsideSliceIdx := func(i string, s []string) (bool, int) {
		for k, v := range s {
			if v == i {
				return true, k
			}
		}
		return false, -1
	}
	for k, v := range colnames {
		// Check duplicate colnames
		if strInsideSlice(v, colnames[k+1:]) {
			return DataFrame{
				err: errors.New("Duplicated colnames on Select"),
			}
		}
		// Check that colnames exist on dataframe
		dfColnames := df.Names()
		if exists, idx := strInsideSliceIdx(v, dfColnames); exists {
			columnsSelected = append(columnsSelected, df.columns[idx])
		} else {
			return DataFrame{
				err: errors.New("The given colname doesn't exist"),
			}
		}
	}
	return New(columnsSelected...)
}

func (df DataFrame) Rename(newname, oldname string) DataFrame {
	if df.Err() != nil {
		return df
	}
	strInsideSliceIdx := func(i string, s []string) (bool, int) {
		for k, v := range s {
			if v == i {
				return true, k
			}
		}
		return false, -1
	}
	// Check that colname exist on dataframe
	var copy DataFrame
	colnames := df.Names()
	if exists, idx := strInsideSliceIdx(oldname, colnames); exists {
		copy = df.Copy()
		copy.columns[idx].Name = newname
	} else {
		return DataFrame{
			err: errors.New("The given colname doesn't exist"),
		}
	}
	return copy
}

// CBind combines the columns of two DataFrames
func (df DataFrame) CBind(newdf DataFrame) DataFrame {
	// TODO: Expand to accept DataFrames, Series, and potentially other objects
	if df.Err() != nil {
		return df
	}
	if newdf.Err() != nil {
		return newdf
	}
	cols := append(df.columns, newdf.columns...)
	return New(cols...)
}

// RBind combines the rows of two DataFrames
func (df DataFrame) RBind(newdf DataFrame) DataFrame {
	// TODO: Expand to accept DataFrames, Series, and potentially other objects
	if df.Err() != nil {
		return df
	}
	if newdf.Err() != nil {
		return newdf
	}
	strInsideSliceIdx := func(i string, s []string) (bool, int) {
		for k, v := range s {
			if v == i {
				return true, k
			}
		}
		return false, -1
	}
	var expandedSeries []Series
	for k, v := range df.Names() {
		if exists, idx := strInsideSliceIdx(v, newdf.Names()); exists {
			originalSeries := df.columns[k]
			addedSeries := newdf.columns[idx]
			newSeries := originalSeries.Concat(addedSeries)
			if err := newSeries.Err(); err != nil {
				return DataFrame{err: err}
			}
			expandedSeries = append(expandedSeries, newSeries)
		} else {
			return DataFrame{err: errors.New("Not compatible column names")}
		}
	}
	return New(expandedSeries...)
}

// Mutate changes a column of the DataFrame with the given Series
func (df DataFrame) Mutate(colname string, series Series) DataFrame {
	if df.Err() != nil {
		return df
	}
	strInsideSliceIdx := func(i string, s []string) (bool, int) {
		for k, v := range s {
			if v == i {
				return true, k
			}
		}
		return false, -1
	}
	if series.Len() != df.nrows {
		return DataFrame{
			err: errors.New("Can't set column. Different dimensions"),
		}
	}
	// Check that colname exist on dataframe
	newSeries := df.columns
	if exists, idx := strInsideSliceIdx(colname, df.Names()); exists {
		newSeries[idx] = series
	} else {
		series.Name = colname
		newSeries = append(newSeries, series)
	}
	return New(newSeries...)
}

// F is the filtering structure
type F struct {
	Colname    string
	Comparator string
	Comparando interface{}
}

// Filter will filter the rows of a DataFrame
func (df DataFrame) Filter(filters ...F) DataFrame {
	if df.Err() != nil {
		return df
	}
	strInsideSliceIdx := func(i string, s []string) (bool, int) {
		for k, v := range s {
			if v == i {
				return true, k
			}
		}
		return false, -1
	}
	var compResults [][]bool
	for _, f := range filters {
		if exists, idx := strInsideSliceIdx(f.Colname, df.Names()); exists {
			res, err := df.columns[idx].Compare(f.Comparator, f.Comparando)
			if err != nil {
				return DataFrame{
					err: err,
				}
			}
			compResults = append(compResults, res)
		} else {
			return DataFrame{
				err: errors.New("The given colname doesn't exist"),
			}
		}
	}
	// Join compResults via "OR"
	if len(compResults) == 0 {
		return df.Copy()
	}
	res := compResults[0]
	for i := 1; i < len(compResults); i++ {
		nextRes := compResults[i]
		for j := 0; j < len(res); j++ {
			res[j] = res[j] || nextRes[j]
		}
	}
	return df.Subset(res)
}

// Read/Write Methods
// =================

func ReadJSON(r io.Reader, types ...string) DataFrame {
	var m []map[string]interface{}
	err := json.NewDecoder(r).Decode(&m)
	if err != nil {
		return DataFrame{err: err}
	}
	return LoadMaps(m, types...)
}

func ReadCSV(r io.Reader, types ...string) DataFrame {
	csvReader := csv.NewReader(r)
	records, err := csvReader.ReadAll()
	if err != nil {
		return DataFrame{err: err}
	}
	return LoadRecords(records, types...)
}

func LoadMaps(maps []map[string]interface{}, types ...string) DataFrame {
	if len(maps) == 0 {
		return DataFrame{
			err: errors.New("Can't parse empty map array"),
		}
	}
	strInsideSliceIdx := func(i string, s []string) (bool, int) {
		for k, v := range s {
			if v == i {
				return true, k
			}
		}
		return false, -1
	}
	fields := make(map[string][]string)
	var colnames []string
	// Initialize data structures
	for _, v := range maps {
		for k, _ := range v {
			if exists, _ := strInsideSliceIdx(k, colnames); !exists {
				colnames = append(colnames, k)
			}
			fields[k] = make([]string, len(maps))
		}
	}
	// Copy the values for all given elements
	for i, v := range maps {
		for k, w := range v {
			fields[k][i] = fmt.Sprint(w)
		}
	}

	// The order of the keys might be different that the types we expect, so we force
	// alphabetical ordering for the map keys.
	sort.Strings(colnames)

	var columns []Series
	if types != nil && len(types) != 0 {
		// Empty String only columns
		if len(types) == 1 {
			t := types[0]
			for _, colname := range colnames {
				col := fields[colname]
				switch t {
				// FIXME: Use SeriesType instead
				case "string":
					columns = append(columns, NamedStrings(colname, col))
				case "int":
					columns = append(columns, NamedInts(colname, col))
				case "float":
					columns = append(columns, NamedFloats(colname, col))
				case "bool":
					columns = append(columns, NamedBools(colname, col))
				default:
					return DataFrame{
						err: errors.New("Unknown type given"),
					}
				}
			}
			return New(columns...)
		}
		if len(types) != len(colnames) {
			return DataFrame{
				err: errors.New("Fields and types array have different dimensions"),
			}
		}
		for k, colname := range colnames {
			col := fields[colname]
			t := types[k]
			switch t {
			// FIXME: Use SeriesType instead
			case "string":
				columns = append(columns, NamedStrings(colname, col))
			case "int":
				columns = append(columns, NamedInts(colname, col))
			case "float":
				columns = append(columns, NamedFloats(colname, col))
			case "bool":
				columns = append(columns, NamedBools(colname, col))
			default:
				return DataFrame{
					err: errors.New("Unknown type given"),
				}
			}
		}
		return New(columns...)
	}

	for _, colname := range colnames {
		col := fields[colname]
		t := findType(col)
		switch t {
		// FIXME: Use SeriesType instead
		case "string":
			columns = append(columns, NamedStrings(colname, col))
		case "int":
			columns = append(columns, NamedInts(colname, col))
		case "float":
			columns = append(columns, NamedFloats(colname, col))
		case "bool":
			columns = append(columns, NamedBools(colname, col))
		default:
			return DataFrame{
				err: errors.New("Unknown type given"),
			}
		}
	}
	return New(columns...)
}

func LoadRecords(records [][]string, types ...string) DataFrame {
	if len(records) == 0 {
		return DataFrame{
			err: errors.New("Empty records"),
		}
	}
	var columns []Series
	if types != nil && len(types) != 0 {
		colnames := records[0]

		// Empty String only columns
		if len(records) == 1 {
			var columns []Series
			for _, v := range colnames {
				columns = append(columns, NamedStrings(v, nil))
				fmt.Println(columns)
			}
			return New(columns...)
		}

		records = transposeRecords(records[1:])
		if len(types) == 1 {
			t := types[0]
			for i, colname := range colnames {
				col := records[i]
				switch t {
				// FIXME: Use SeriesType instead
				case "string":
					columns = append(columns, NamedStrings(colname, col))
				case "int":
					columns = append(columns, NamedInts(colname, col))
				case "float":
					columns = append(columns, NamedFloats(colname, col))
				case "bool":
					columns = append(columns, NamedBools(colname, col))
				default:
					return DataFrame{
						err: errors.New("Unknown type given"),
					}
				}
			}
			return New(columns...)
		}
		if len(types) != len(colnames) {
			return DataFrame{
				err: errors.New("Records and types array have different dimensions"),
			}
		}
		for i, colname := range colnames {
			t := types[i]
			col := records[i]
			switch t {
			// FIXME: Use SeriesType instead
			case "string":
				columns = append(columns, NamedStrings(colname, col))
			case "int":
				columns = append(columns, NamedInts(colname, col))
			case "float":
				columns = append(columns, NamedFloats(colname, col))
			case "bool":
				columns = append(columns, NamedBools(colname, col))
			default:
				return DataFrame{
					err: errors.New("Unknown type given"),
				}
			}
		}
		return New(columns...)
	}

	colnames := records[0]
	// Empty String only columns
	if len(records) == 1 {
		var columns []Series
		for _, v := range colnames {
			columns = append(columns, NamedStrings(v, nil))
		}
		return New(columns...)
	}

	// If no type is given, try to auto-identify it
	records = transposeRecords(records[1:])
	for i, colname := range colnames {
		col := records[i]
		t := findType(col)
		switch t {
		// FIXME: Use SeriesType instead
		case "string":
			columns = append(columns, NamedStrings(colname, col))
		case "int":
			columns = append(columns, NamedInts(colname, col))
		case "float":
			columns = append(columns, NamedFloats(colname, col))
		case "bool":
			columns = append(columns, NamedBools(colname, col))
		default:
			return DataFrame{
				err: errors.New("Unknown type given"),
			}
		}
	}
	return New(columns...)
}

func (df DataFrame) WriteJSON(w io.Writer) error {
	if df.Err() != nil {
		return df.Err()
	}
	m := df.Maps()
	return json.NewEncoder(w).Encode(m)
}

func (df DataFrame) WriteCSV(w io.Writer) error {
	if df.Err() != nil {
		return df.Err()
	}
	records := df.Records()
	return csv.NewWriter(w).WriteAll(records)
}

// Getters/Setters for DataFrame fields
// ====================================

func (df DataFrame) Names() []string {
	var colnames []string
	for _, v := range df.columns {
		colnames = append(colnames, v.Name)
	}
	return colnames
}

func (df DataFrame) Types() []string {
	var coltypes []string
	for _, v := range df.columns {
		coltypes = append(coltypes, v.t)
	}
	return coltypes
}

func (df DataFrame) SetNames(colnames []string) error {
	if df.Err() != nil {
		return df.Err()
	}
	if len(colnames) != df.ncols {
		err := errors.New("Couldn't set the column names. Wrong dimensions.")
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

// Col returns the Series with the given column name contained in the DataFrame
func (df DataFrame) Col(colname string) Series {
	// TODO: Accept also an int with the position of the Series
	if df.Err() != nil {
		return Series{err: df.Err()}
	}
	strInsideSliceIdx := func(i string, s []string) (bool, int) {
		for k, v := range s {
			if v == i {
				return true, k
			}
		}
		return false, -1
	}
	// Check that colname exist on dataframe
	var ret Series
	if exists, idx := strInsideSliceIdx(colname, df.Names()); exists {
		ret = df.columns[idx].Copy()
	} else {
		return Series{
			err: errors.New("The given colname doesn't exist"),
		}
	}
	return ret
}

// InnerJoin returns a DataFrame containing the inner join of two DataFrames.
// This operation matches all rows that appear on both dataframes.
func (a DataFrame) InnerJoin(b DataFrame, keys ...string) DataFrame {
	if len(keys) == 0 {
		return DataFrame{err: errors.New("Unspecified Join keys")}
	}
	// Check that we have all given keys in both DataFrames
	errorArr := []string{}
	var iKeysA []int
	var iKeysB []int
	for _, key := range keys {
		i := a.ColIndex(key)
		if i < 0 {
			errorArr = append(errorArr, fmt.Sprint("Can't find key \"", key, "\" on left DataFrame"))
		}
		iKeysA = append(iKeysA, i)
		j := b.ColIndex(key)
		if j < 0 {
			errorArr = append(errorArr, fmt.Sprint("Can't find key '", key, "' on left DataFrame"))
		}
		iKeysB = append(iKeysB, j)
	}
	if len(errorArr) != 0 {
		return DataFrame{err: errors.New(strings.Join(errorArr, "\n"))}
	}

	aCols := a.columns
	bCols := b.columns
	// Initialize newCols
	var newCols []Series
	for _, i := range iKeysA {
		newCols = append(newCols, aCols[i].Empty())
	}
	var iNotKeysA []int
	for i := 0; i < a.ncols; i++ {
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
	for i := 0; i < a.nrows; i++ {
		for j := 0; j < b.nrows; j++ {
			match := true
			for k := range keys {
				aElem := aCols[iKeysA[k]].elem(i)
				bElem := bCols[iKeysB[k]].elem(j)
				match = match && aElem.Eq(bElem)
			}
			if match {
				ii := 0
				for _, k := range iKeysA {
					elem := aCols[k].elem(i)
					newCols[ii].Append(elem)
					ii++
				}
				for _, k := range iNotKeysA {
					elem := aCols[k].elem(i)
					newCols[ii].Append(elem)
					ii++
				}
				for _, k := range iNotKeysB {
					elem := bCols[k].elem(j)
					newCols[ii].Append(elem)
					ii++
				}
			}
		}
	}
	return New(newCols...)
}

// LeftJoin returns a DataFrame containing the left join of two DataFrames.
// This operation matches all rows that appear on both dataframes.
func (a DataFrame) LeftJoin(b DataFrame, keys ...string) DataFrame {
	if len(keys) == 0 {
		return DataFrame{err: errors.New("Unspecified Join keys")}
	}
	// Check that we have all given keys in both DataFrames
	errorArr := []string{}
	var iKeysA []int
	var iKeysB []int
	for _, key := range keys {
		i := a.ColIndex(key)
		if i < 0 {
			errorArr = append(errorArr, fmt.Sprint("Can't find key \"", key, "\" on left DataFrame"))
		}
		iKeysA = append(iKeysA, i)
		j := b.ColIndex(key)
		if j < 0 {
			errorArr = append(errorArr, fmt.Sprint("Can't find key '", key, "' on left DataFrame"))
		}
		iKeysB = append(iKeysB, j)
	}
	if len(errorArr) != 0 {
		return DataFrame{err: errors.New(strings.Join(errorArr, "\n"))}
	}

	aCols := a.columns
	bCols := b.columns
	// Initialize newCols
	var newCols []Series
	for _, i := range iKeysA {
		newCols = append(newCols, aCols[i].Empty())
	}
	var iNotKeysA []int
	for i := 0; i < a.ncols; i++ {
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
	for i := 0; i < a.nrows; i++ {
		matched := false
		for j := 0; j < b.nrows; j++ {
			match := true
			for k := range keys {
				aElem := aCols[iKeysA[k]].elem(i)
				bElem := bCols[iKeysB[k]].elem(j)
				match = match && aElem.Eq(bElem)
			}
			if match {
				matched = true
				ii := 0
				for _, k := range iKeysA {
					elem := aCols[k].elem(i)
					newCols[ii].Append(elem)
					ii++
				}
				for _, k := range iNotKeysA {
					elem := aCols[k].elem(i)
					newCols[ii].Append(elem)
					ii++
				}
				for _, k := range iNotKeysB {
					elem := bCols[k].elem(j)
					newCols[ii].Append(elem)
					ii++
				}
			}
		}
		if !matched {
			ii := 0
			for _, k := range iKeysA {
				elem := aCols[k].elem(i)
				newCols[ii].Append(elem)
				ii++
			}
			for _, k := range iNotKeysA {
				elem := aCols[k].elem(i)
				newCols[ii].Append(elem)
				ii++
			}
			for _, _ = range iNotKeysB {
				newCols[ii].Append(nil)
				ii++
			}
		}
	}
	return New(newCols...)
}

// RightJoin returns a DataFrame containing the right join of two DataFrames.
// This operation matches all rows that appear on both dataframes.
func (a DataFrame) RightJoin(b DataFrame, keys ...string) DataFrame {
	if len(keys) == 0 {
		return DataFrame{err: errors.New("Unspecified Join keys")}
	}
	// Check that we have all given keys in both DataFrames
	errorArr := []string{}
	var iKeysA []int
	var iKeysB []int
	for _, key := range keys {
		i := a.ColIndex(key)
		if i < 0 {
			errorArr = append(errorArr, fmt.Sprint("Can't find key \"", key, "\" on left DataFrame"))
		}
		iKeysA = append(iKeysA, i)
		j := b.ColIndex(key)
		if j < 0 {
			errorArr = append(errorArr, fmt.Sprint("Can't find key '", key, "' on left DataFrame"))
		}
		iKeysB = append(iKeysB, j)
	}
	if len(errorArr) != 0 {
		return DataFrame{err: errors.New(strings.Join(errorArr, "\n"))}
	}

	aCols := a.columns
	bCols := b.columns
	// Initialize newCols
	var newCols []Series
	for _, i := range iKeysA {
		newCols = append(newCols, aCols[i].Empty())
	}
	var iNotKeysA []int
	for i := 0; i < a.ncols; i++ {
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
		for i := 0; i < a.nrows; i++ {
			match := true
			for k := range keys {
				aElem := aCols[iKeysA[k]].elem(i)
				bElem := bCols[iKeysB[k]].elem(j)
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
			elem := aCols[k].elem(i)
			newCols[ii].Append(elem)
			ii++
		}
		for _, k := range iNotKeysA {
			elem := aCols[k].elem(i)
			newCols[ii].Append(elem)
			ii++
		}
		for _, k := range iNotKeysB {
			elem := bCols[k].elem(j)
			newCols[ii].Append(elem)
			ii++
		}
	}
	for _, j := range nonmatched {
		ii := 0
		for _, k := range iKeysB {
			elem := bCols[k].elem(j)
			newCols[ii].Append(elem)
			ii++
		}
		for _, _ = range iNotKeysA {
			newCols[ii].Append(nil)
			ii++
		}
		for _, k := range iNotKeysB {
			elem := bCols[k].elem(j)
			newCols[ii].Append(elem)
			ii++
		}
	}
	return New(newCols...)
}

// OuterJoin returns a DataFrame containing the outer join of two DataFrames.
// This operation matches all rows that appear on both dataframes.
func (a DataFrame) OuterJoin(b DataFrame, keys ...string) DataFrame {
	if len(keys) == 0 {
		return DataFrame{err: errors.New("Unspecified Join keys")}
	}
	// Check that we have all given keys in both DataFrames
	errorArr := []string{}
	var iKeysA []int
	var iKeysB []int
	for _, key := range keys {
		i := a.ColIndex(key)
		if i < 0 {
			errorArr = append(errorArr, fmt.Sprint("Can't find key \"", key, "\" on left DataFrame"))
		}
		iKeysA = append(iKeysA, i)
		j := b.ColIndex(key)
		if j < 0 {
			errorArr = append(errorArr, fmt.Sprint("Can't find key '", key, "' on left DataFrame"))
		}
		iKeysB = append(iKeysB, j)
	}
	if len(errorArr) != 0 {
		return DataFrame{err: errors.New(strings.Join(errorArr, "\n"))}
	}

	aCols := a.columns
	bCols := b.columns
	// Initialize newCols
	var newCols []Series
	for _, i := range iKeysA {
		newCols = append(newCols, aCols[i].Empty())
	}
	var iNotKeysA []int
	for i := 0; i < a.ncols; i++ {
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
	for i := 0; i < a.nrows; i++ {
		matched := false
		for j := 0; j < b.nrows; j++ {
			match := true
			for k := range keys {
				aElem := aCols[iKeysA[k]].elem(i)
				bElem := bCols[iKeysB[k]].elem(j)
				match = match && aElem.Eq(bElem)
			}
			if match {
				matched = true
				ii := 0
				for _, k := range iKeysA {
					elem := aCols[k].elem(i)
					newCols[ii].Append(elem)
					ii++
				}
				for _, k := range iNotKeysA {
					elem := aCols[k].elem(i)
					newCols[ii].Append(elem)
					ii++
				}
				for _, k := range iNotKeysB {
					elem := bCols[k].elem(j)
					newCols[ii].Append(elem)
					ii++
				}
			}
		}
		if !matched {
			ii := 0
			for _, k := range iKeysA {
				elem := aCols[k].elem(i)
				newCols[ii].Append(elem)
				ii++
			}
			for _, k := range iNotKeysA {
				elem := aCols[k].elem(i)
				newCols[ii].Append(elem)
				ii++
			}
			for _, _ = range iNotKeysB {
				newCols[ii].Append(nil)
				ii++
			}
		}
	}
	for j := 0; j < b.nrows; j++ {
		matched := false
		for i := 0; i < a.nrows; i++ {
			match := true
			for k := range keys {
				aElem := aCols[iKeysA[k]].elem(i)
				bElem := bCols[iKeysB[k]].elem(j)
				match = match && aElem.Eq(bElem)
			}
			if match {
				matched = true
			}
		}
		if !matched {
			ii := 0
			for _, k := range iKeysB {
				elem := bCols[k].elem(j)
				newCols[ii].Append(elem)
				ii++
			}
			for _ = range iNotKeysA {
				newCols[ii].Append(nil)
				ii++
			}
			for _, k := range iNotKeysB {
				elem := bCols[k].elem(j)
				newCols[ii].Append(elem)
				ii++
			}
		}
	}
	return New(newCols...)
}

// CrossJoin returns a DataFrame containing the cross join of two DataFrames.
// This operation matches all rows that appear on both dataframes.
func (a DataFrame) CrossJoin(b DataFrame) DataFrame {
	aCols := a.columns
	bCols := b.columns
	// Initialize newCols
	var newCols []Series
	for i := 0; i < a.ncols; i++ {
		newCols = append(newCols, aCols[i].Empty())
	}
	for i := 0; i < b.ncols; i++ {
		newCols = append(newCols, bCols[i].Empty())
	}
	// Fill newCols
	for i := 0; i < a.nrows; i++ {
		for j := 0; j < b.nrows; j++ {
			for ii := 0; ii < a.ncols; ii++ {
				elem := aCols[ii].elem(i)
				newCols[ii].Append(elem)
			}
			for ii := 0; ii < b.ncols; ii++ {
				jj := ii + a.ncols
				elem := bCols[ii].elem(j)
				newCols[jj].Append(elem)
			}
		}
	}
	return New(newCols...)
}

// ColIndex returns the index of the column with name `s`. If it fails to find the
// column it returns -1 instead.
func (d DataFrame) ColIndex(s string) int {
	for k, v := range d.Names() {
		if v == s {
			return k
		}
	}
	return -1
}

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

func (df DataFrame) Maps() []map[string]interface{} {
	maps := make([]map[string]interface{}, df.nrows)
	colnames := df.Names()
	for i := 0; i < df.nrows; i++ {
		m := make(map[string]interface{})
		for k, v := range colnames {
			val := df.columns[k].Val(i)
			m[v] = val
		}
		maps[i] = m
	}
	return maps
}

func (d DataFrame) Dense() (*mat64.Dense, error) {
	if d.Err() != nil {
		return nil, d.Err()
	}
	var floats []float64
	for _, col := range d.columns {
		f, err := col.Float()
		if err != nil {
			return nil, err
		}
		floats = append(floats, f...)
	}
	dense := mat64.NewDense(d.nrows, d.ncols, floats)
	return dense, nil
}
