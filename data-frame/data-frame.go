package df

// TODO: Remove append() in favour of manually allocating the slices

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
)

// DataFrame is the base data structure
type DataFrame struct {
	columns  []Series
	colnames []string
	coltypes []string
	ncols    int
	nrows    int
	err      error // TODO: Define custom error data type
}

func (df DataFrame) Err() error {
	return df.err
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
	var coltypes []string
	for k, v := range series {
		columns = append(columns, v.Copy())
		colnames[k] = v.Name
		coltypes = append(coltypes, v.t)
		l := Len(v)
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
		columns:  columns,
		colnames: colnames,
		coltypes: coltypes,
		ncols:    len(series),
		nrows:    lastLength,
		err:      nil,
	}
	return df
}

func (df DataFrame) Copy() DataFrame {
	if df.Err() != nil {
		return df
	}
	copy := New(df.columns...)
	return copy
}

// Subsets the DataFrame based on the Series subsetting rules
func (df DataFrame) Subset(indexes interface{}) DataFrame {
	if df.Err() != nil {
		return df
	}
	var columnsSubset []Series
	for _, column := range df.columns {
		columnSubset, err := column.Subset(indexes)
		if err != nil {
			return DataFrame{err: err}
		}
		columnsSubset = append(columnsSubset, columnSubset)
	}
	return New(columnsSubset...)
}

// Select the given DataFrame columns
func (df DataFrame) Select(colnames []string) DataFrame {
	// TODO: Expand to accept []int, []bool and Series
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
		if exists, idx := strInsideSliceIdx(v, df.colnames); exists {
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
	if exists, idx := strInsideSliceIdx(oldname, df.colnames); exists {
		copy = df.Copy()
		copy.colnames[idx] = newname
		copy.columns[idx].Name = newname
	} else {
		return DataFrame{
			err: errors.New("The given colname doesn't exist"),
		}
	}
	return copy
}

// TODO: Expand to accept DataFrames, Series, and potentially other objects
func (df DataFrame) CBind(newdf DataFrame) DataFrame {
	if df.Err() != nil {
		return df
	}
	if newdf.Err() != nil {
		return newdf
	}
	cols := append(df.columns, newdf.columns...)
	return New(cols...)
}

// TODO: Expand to accept DataFrames, Series, and potentially other objects
func (df DataFrame) RBind(newdf DataFrame) DataFrame {
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
	for k, v := range df.colnames {
		if exists, idx := strInsideSliceIdx(v, newdf.colnames); exists {
			originalSeries := df.columns[k]
			addedSeries := newdf.columns[idx]
			newSeries := originalSeries.Append(addedSeries)
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
	records := df.SaveRecords()
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

func (df DataFrame) SaveRecords() [][]string {
	var records [][]string
	records = append(records, df.colnames)
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

func ReadCSV(str string, types ...string) DataFrame {
	r := csv.NewReader(strings.NewReader(str))
	var records [][]string
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return DataFrame{err: err}
		}
		records = append(records, record)
	}
	return ReadRecords(records, types...)
}

func ReadRecords(records [][]string, types ...string) DataFrame {
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

func (df DataFrame) Names() []string {
	var colnames []string
	for _, v := range df.colnames {
		colnames = append(colnames, v)
	}
	return colnames
}

func (df DataFrame) Types() []string {
	var coltypes []string
	for _, v := range df.coltypes {
		coltypes = append(coltypes, v)
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
		df.colnames[k] = v
		df.columns[k].Name = v
	}
	return nil
}

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

func (df DataFrame) SaveMaps() []map[string]interface{} {
	maps := make([]map[string]interface{}, df.nrows)
	colnames := df.colnames
	for i := 0; i < df.nrows; i++ {
		m := make(map[string]interface{})
		for k, v := range colnames {
			val := df.columns[i].Val(k)
			if val != nil {
				m[v] = val
			}
		}
		maps[i] = m
	}
	return maps
}

func (df DataFrame) SaveCSV() ([]byte, error) {
	if df.Err() != nil {
		return nil, df.Err()
	}
	records := df.SaveRecords()
	b := &bytes.Buffer{}
	w := csv.NewWriter(b)
	for _, record := range records {
		if err := w.Write(record); err != nil {
			return nil, err
		}
	}
	w.Flush()
	return b.Bytes(), nil
}

func (df DataFrame) SaveJSON() ([]byte, error) {
	if df.Err() != nil {
		return nil, df.Err()
	}
	m := df.SaveMaps()
	return json.Marshal(m)
}

// TODO: Accept also an int with the position of the Series
func (df DataFrame) Col(colname string) Series {
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
	if exists, idx := strInsideSliceIdx(colname, df.colnames); exists {
		ret = df.columns[idx].Copy()
	} else {
		return Series{
			err: errors.New("The given colname doesn't exist"),
		}
	}
	return ret
}

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
	if Len(series) != df.nrows {
		return DataFrame{
			err: errors.New("Can't set column. Different dimensions"),
		}
	}
	// Check that colname exist on dataframe
	var newSeries []Series
	newSeries = append(newSeries, df.columns...)
	if exists, idx := strInsideSliceIdx(colname, df.colnames); exists {
		switch series.t {
		case "string":
			newSeries[idx] = NamedStrings(colname, series)
		case "int":
			newSeries[idx] = NamedInts(colname, series)
		case "float":
			newSeries[idx] = NamedFloats(colname, series)
		case "bool":
			newSeries[idx] = NamedBools(colname, series)
		default:
			return DataFrame{
				err: errors.New("Unknown Series type"),
			}
		}
	} else {
		switch series.t {
		case "string":
			newSeries = append(newSeries, NamedStrings(colname, series))
		case "int":
			newSeries = append(newSeries, NamedInts(colname, series))
		case "float":
			newSeries = append(newSeries, NamedFloats(colname, series))
		case "bool":
			newSeries = append(newSeries, NamedBools(colname, series))
		default:
			return DataFrame{
				err: errors.New("Unknown Series type"),
			}
		}
	}
	return New(newSeries...)
}

type F struct {
	Colname    string
	Comparator string
	Comparando interface{}
}

// TODO: Implement a better interface for filtering
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
		if exists, idx := strInsideSliceIdx(f.Colname, df.colnames); exists {
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

func ReadJSON(r io.Reader, types ...string) DataFrame {
	var m []map[string]interface{}
	err := json.NewDecoder(r).Decode(&m)
	if err != nil {
		return DataFrame{err: err}
	}
	return ReadMaps(m, types...)
}

func ReadJSONString(str string, types ...string) DataFrame {
	r := strings.NewReader(str)
	return ReadJSON(r, types...)
}

func ReadMaps(maps []map[string]interface{}, types ...string) DataFrame {
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

// TODO: (df DataFrame) Str() (string)
// TODO: (df DataFrame) Summary() (string)
// TODO: dplyr-ish: Arrange?
// TODO: dplyr-ish: GroupBy?
// TODO: Compare?
// TODO: UniqueRows?
// TODO: UniqueColumns?
// TODO: Joins: Inner/Outer/Right/Left all.x? all.y?
// TODO: ChangeType(DataFrame, types) (DataFrame, err) // Parse columns again
