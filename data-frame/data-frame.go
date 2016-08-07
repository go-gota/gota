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
// TODO: dplyr-ish: SubsetRows(DataFrame, subset interface) (DataFrame, err)    // AKA: Filter
// TODO: dplyr-ish: SubsetColumns(DataFrame, subset interface) (DataFrame, err) // AKA: Select
// TODO: dplyr-ish: Mutate ?
// TODO: dplyr-ish: Rename ?
// TODO: dplyr-ish: Group_By ?
// TODO: Compare?
// TODO: UniqueRows?
// TODO: UniqueColumns?
// TODO: Joins: Inner/Outer/Right/Left all.x? all.y?
