package dataframe

import (
	"fmt"

	"github.com/mqy527/gota/series"
)

// All the operations on it will influence the DataFrame's content.
type Self struct {
	this *DataFrame
}

// All the operations on Self will influence the DataFrame's content.
func (df *DataFrame) Self() Self {
	self := Self{
		this: df,
	}
	return self
}

// AppendColumns Append columns on the DataFrame. The param's modification will influence the DataFrame's content after AppendColumns.
func (s Self) AppendColumns(cols ...series.Series) error {
	if s.this.Err != nil || len(cols) == 0 {
		return nil
	}
	slen := cols[0].Len()
	for i := 1; i < len(cols); i++ {
		if cols[i].Error() != nil {
			return fmt.Errorf("AppendColumns: col[%s] has error: %v", cols[i].Name(), cols[i].Error())
		}
		if slen != cols[i].Len() {
			return fmt.Errorf("AppendColumns: serieses length not equal")
		}
	}
	if slen != s.this.nrows {
		return fmt.Errorf("AppendColumns: wrong dimensions")
	}
	s.this.columns = append(s.this.columns, cols...)
	s.this.ncols = len(s.this.columns)

	colnames := s.this.Names()
	fixColnames(colnames)
	for i, colname := range colnames {
		s.this.columns[i].SetName(colname)
	}
	return nil
}

// Capply applies the given function to the columns of a DataFrame, will influence the DataFrame's content.
func (s Self) Capply(f func(series.Series)) {
	if s.this.Err != nil {
		return
	}
	for _, s := range s.this.columns {
		f(s)
	}
}

// CapplyByName applies the given function to the column, will influence the DataFrame's content.
func (s Self) CapplyByName(colname string, f func(series.Series)) {
	if s.this.Err != nil {
		return
	}
	idx := findInStringSlice(colname, s.this.Names())
	if idx < 0 {
		return
	}
	f(s.this.columns[idx])
}

// ImmutableCol returns an immutable Series of the DataFrame with the given column name contained in the DataFrame.
func (s Self) ImmutableCol(colname string) series.Series {
	if s.this.Err != nil {
		return series.Err(s.this.Err)
	}
	// Check that colname exist on dataframe
	idx := findInStringSlice(colname, s.this.Names())
	if idx < 0 {
		return series.Err(fmt.Errorf("unknown column name"))
	}
	return s.this.columns[idx].Immutable()
}

// Rename changes the name of one of the columns of a DataFrame
func (s Self) Rename(newname, oldname string) {
	if s.this.Err != nil {
		return
	}
	// Check that colname exist on dataframe
	colnames := s.this.Names()
	idx := findInStringSlice(oldname, colnames)
	if idx == -1 {
		return
	}
	s.this.columns[idx].SetName(newname)
}

func (s Self) RemoveCols(removedColnames ...string) {
	if s.this.Err != nil || len(removedColnames) == 0 {
		return
	}
	var cols []series.Series
	// Check that colname exist on dataframe
	colnames := s.this.Names()
	for i := 0; i < len(colnames); i++ {
		idx := findInStringSlice(colnames[i], removedColnames)
		if idx == -1 {
			cols = append(cols, s.this.columns[i])
		}
	}
	s.this.columns = cols
	s.this.ncols = len(cols)
}
