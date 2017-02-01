package dataframe

import (
	"fmt"

	"strings"

	"github.com/isuruceanu/gota/series"
)

type groupedStruct struct {
	colname string
	indexes []int
}

// GroupedDataFrame a DataFrame which is grouped by columns
type GroupedDataFrame struct {
	DataFrame
	groupedBy []string
}

// Group create a GroupedDataFrame with cols groups
func (d DataFrame) Group(cols ...string) GroupedDataFrame {
	r := GroupedDataFrame{DataFrame: d}

	for _, col := range cols {
		colIndex := d.ColIndex(col)
		if colIndex < 0 {
			r.Err = fmt.Errorf("colname %v doesn't exist", col)
			return r
		}

		r.groupedBy = append(r.groupedBy, col)
	}

	return r
}

func (g GroupedDataFrame) Summarize(f func(DataFrame) series.Series) DataFrame {

	keyIndexes := g.parseInternal()

	rowlen := -1
	elements := make([][]series.Element, len(keyIndexes))
	i := 0

	for k, indexes := range keyIndexes {

		row := f(g.Subset(indexes))

		keys := strings.Split(k, "$_$")

		if len(keys) != len(g.groupedBy) {
			return DataFrame{
				Err: fmt.Errorf("error keys lens differs from len of groups %v: %v", len(keys), len(g.groupedBy)),
			}
		}

		if row.Err != nil {
			return DataFrame{
				Err: fmt.Errorf("error applying function on row %v: %v", keys, row.Err),
			}
		}

		if rowlen != -1 && rowlen != row.Len() {
			return DataFrame{
				Err: fmt.Errorf("error applying function: rows have different lengths"),
			}
		}
		rowlen = row.Len()
		groupedLevels := series.Strings(keys)

		rowElems := make([]series.Element, rowlen+len(keys))
		//Add group levels
		for j := 0; j < len(keys); j++ {
			rowElems[j] = groupedLevels.Elem(j)
		}

		for j := 0; j < rowlen; j++ {
			rowElems[j+len(keys)] = row.Elem(j)
		}
		elements[i] = rowElems
		i++
	}

	ncol := rowlen + len(g.groupedBy)
	nrow := len(keyIndexes)
	// Cast columns if necessary
	columns := make([]series.Series, ncol)
	for j := 0; j < ncol; j++ {
		types := make([]series.Type, nrow)
		for i := 0; i < nrow; i++ {
			types[i] = elements[i][j].Type()
		}

		colType := detectType(types)
		s := series.New(nil, colType, "").Empty()
		for i := 0; i < nrow; i++ {
			s.Append(elements[i][j])
		}
		columns[j] = s
	}

	names := make([]string, len(g.groupedBy)+rowlen)
	orders := make([]Order, len(g.groupedBy))
	for i := 0; i < len(g.groupedBy)+rowlen; i++ {
		if i < len(g.groupedBy) {
			names[i] = g.groupedBy[i]
			orders[i] = Sort(g.groupedBy[i])
		} else {
			names[i] = fmt.Sprintf("X%v", i-len(g.groupedBy))
		}
	}

	dfr := New(columns...)

	dfr.SetNames(names)

	return dfr.Arrange(orders...)
}

func (g GroupedDataFrame) parseInternal() map[string][]int {

	groupedOnly := g.Select(g.groupedBy)

	groupSO := make(map[string][]int)
	key := make([]string, len(g.groupedBy))
	for i := 0; i < groupedOnly.nrows; i++ {
		row := groupedOnly.Subset(i)

		for idx, col := range g.groupedBy {
			key[idx] = row.Col(col).Elem(0).String()
		}
		dkey := strings.Join(key, "$_$")

		groupSO[dkey] = append(groupSO[dkey], i)

	}
	return groupSO
}
