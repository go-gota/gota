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

// AppendColumns Append columns on the DataFrame.
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
