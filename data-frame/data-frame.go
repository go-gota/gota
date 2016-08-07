package df

import (
	"errors"
	"fmt"
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
			var newSeries Series
			originalSeries := df.columns[k]
			addedSeries := newdf.columns[idx]
			// TODO: Refactor into Series.Append method
			switch originalSeries.t {
			case "string":
				newSeries = NamedStrings(originalSeries.Name, originalSeries, addedSeries)
			case "int":
				newSeries = NamedInts(originalSeries.Name, originalSeries, addedSeries)
			case "float":
				newSeries = NamedFloats(originalSeries.Name, originalSeries, addedSeries)
			case "bool":
				newSeries = NamedBools(originalSeries.Name, originalSeries, addedSeries)
			default:
				return DataFrame{err: errors.New("Unknown Series type")}
			}
			expandedSeries = append(expandedSeries, newSeries)
		} else {
			return DataFrame{err: errors.New("Not compatible column names")}
		}
	}
	return New(expandedSeries...)
}

// TODO: (df DataFrame) String() (string)
// TODO: (df DataFrame) Str() (string)
// TODO: (df DataFrame) Summary() (string)
// TODO: Dim(DataFrame) ([2]int)
// TODO: Nrows(DataFrame) (int)
// TODO: Ncols(DataFrame) (int)
// TODO: ReadRecords([][]string) (DataFrame, err)
// TODO: ReadMaps(map[string]interface) (DataFrame, err)
// TODO: ReadCSV(string) (DataFrame, err)
// TODO: ReadJSON(string) (DataFrame, err)
// TODO: ParseRecords([][]string, types) (DataFrame, err)
// TODO: ParseMaps(map[string]interface, types) (DataFrame, err)
// TODO: ParseCSV(string, types) (DataFrame, err)
// TODO: ParseJSON(string, types) (DataFrame, err)
// TODO: SaveRecords(DataFrame) ([][]string)
// TODO: SaveMaps(DataFrame) (map[string]interface)
// TODO: SaveCSV(DataFrame) (string) // Bytes?
// TODO: SaveJSON(DataFrame) (string) // Bytes?
// TODO: Rbind(DataFrame, DataFrame) (DataFrame, err)
// TODO: Cbind(DataFrame, DataFrame) (DataFrame, err)
// TODO: dplyr-ish: Filter(DataFrame, subset interface) (DataFrame, err)    // AKA: Filter
// TODO: dplyr-ish: Mutate ?
// TODO: dplyr-ish: Group_By ?
// TODO: Compare?
// TODO: UniqueRows?
// TODO: UniqueColumns?
// TODO: Joins: Inner/Outer/Right/Left all.x? all.y?
