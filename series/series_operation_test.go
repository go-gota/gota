package series

import (
	"fmt"
	"reflect"
	"testing"
)

func TestSeries_Add(t *testing.T) {
	type args struct {
		right   Series
		indexes Indexes
	}
	tests := []struct {
		name        string
		inputSeries Series
		args        args
		want        Series
	}{
		{
			name:        "add series with the same dimension to all records",
			inputSeries: Ints([]int{1, 2, 3, 4, 5}),
			args: args{
				right:   Ints([]int{1, 1, 1, 1, 1}),
				indexes: Bools([]bool{true, true, true, true, true}),
			},
			want: Ints([]int{2, 3, 4, 5, 6}),
		},
		{
			name:        "add series with the same dimension to subset of records",
			inputSeries: Ints([]int{1, 2, 3, 4, 5}),
			args: args{
				right:   Ints([]int{1, 1, 1, 1, 1}),
				indexes: Bools([]bool{false, true, true, true, false}),
			},
			want: New([]interface{}{nil, 3, 4, 5, nil}, Int, ""),
		},
		{
			name:        "add list series with the same dimension to subset of records",
			inputSeries: IntsList([][]int{{1, 2, 3, 4}, {5, 6, 7, 8}}),
			args: args{
				right:   Ints([]int{1, 2, 3, 4, 5, 6, 7, 8}),
				indexes: Bools([]bool{false, true, true, true, false, false, false, true}),
			},
			want: New([]interface{}{nil, 4, 6, 8, nil, nil, nil, 16}, Int, ""),
		},
		{
			name:        "add series with the same dimension to no rows",
			inputSeries: Ints([]int{1, 2, 3, 4, 5}),
			args: args{
				right:   Ints([]int{1, 1, 1, 1, 1}),
				indexes: Bools([]bool{false, false, false, false, false}),
			},
			want: New([]interface{}{nil, nil, nil, nil, nil}, Int, ""),
		},
		{
			name:        "add series with the same dimension to subset of records",
			inputSeries: Ints([]int{1, 2, 3, 4, 5}),
			args: args{
				right:   Ints([]int{1, 1, 1, 1, 1}),
				indexes: Bools([]bool{false, true, true, true, false}),
			},
			want: New([]interface{}{nil, 3, 4, 5, nil}, Int, ""),
		},
		{
			name:        "add series with different dimension but the value can be broadcasted to subset of records",
			inputSeries: Strings([]string{"a", "b", "c", "d", "e"}),
			args: args{
				right:   Strings([]string{"_1"}),
				indexes: Bools([]bool{false, true, true, true, false}),
			},
			want: New([]interface{}{nil, "b_1", "c_1", "d_1", nil}, String, ""),
		},
		{
			name:        "add series with different dimension but the value can be broadcasted to subset of records",
			inputSeries: Strings([]string{"1_"}),
			args: args{
				right:   Strings([]string{"a", "b", "c", "d", "e"}),
				indexes: Bools([]bool{false, true, true, true, false}),
			},
			want: New([]interface{}{nil, "1_b", "1_c", "1_d", nil}, String, ""),
		},
		{
			name:        "add series with same dimension; different type; to subset of records",
			inputSeries: Ints([]int{1, 2, 3, 4, 5}),
			args: args{
				right:   Floats([]float64{2.1, 2.1, 3.1, 3.1, 3.1}),
				indexes: Bools([]bool{false, true, true, true, false}),
			},
			want: New([]interface{}{nil, 4.1, 6.1, 7.1, nil}, Float, ""),
		},
		{
			name:        "add series with different dimension; different type; to subset of records",
			inputSeries: Ints([]int{1, 2, 3, 4, 5}),
			args: args{
				right:   Floats([]float64{2.1, 2.1, 3.1, 3.1, 3.1, 3.2}),
				indexes: Bools([]bool{false, true, true, true, false, false}),
			},
			want: Series{Err: fmt.Errorf("can't do operation due to mismatch length")},
		},
		{
			name:        "add series with same dimension; different type; to subset of records",
			inputSeries: Strings([]string{"a_", "b_", "c_", "d_", "e_"}),
			args: args{
				right:   Bools([]bool{true, true, true, false, false}),
				indexes: Bools([]bool{false, true, true, true, true}),
			},
			want: New([]interface{}{nil, "b_true", "c_true", "d_false", "e_false"}, String, ""),
		},
		{
			name:        "could not add bool series",
			inputSeries: Bools([]bool{true, false, false, true, false}),
			args: args{
				right:   Bools([]bool{true, true, true, false, false}),
				indexes: Bools([]bool{false, true, true, true, true}),
			},
			want: Series{Err: fmt.Errorf("bool is not supported for + operation")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.inputSeries.Add(tt.args.right, tt.args.indexes); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.Add() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_Substract(t *testing.T) {
	type args struct {
		right   Series
		indexes Indexes
	}
	tests := []struct {
		name        string
		inputSeries Series
		args        args
		want        Series
	}{
		{
			name:        "substract series with the same dimension to all records",
			inputSeries: Ints([]int{1, 2, 3, 4, 5}),
			args: args{
				right:   Ints([]int{1, 1, 1, 1, 1}),
				indexes: Bools([]bool{true, true, true, true, true}),
			},
			want: Ints([]int{0, 1, 2, 3, 4}),
		},
		{
			name:        "substract series with the same dimension to subset of records",
			inputSeries: Ints([]int{1, 2, 3, 4, 5}),
			args: args{
				right:   Ints([]int{1, 1, 1, 1, 1}),
				indexes: Bools([]bool{false, true, true, true, false}),
			},
			want: New([]interface{}{nil, 1, 2, 3, nil}, Int, ""),
		},
		{
			name:        "substract list series with the same dimension to subset of records",
			inputSeries: IntsList([][]int{{1, 2, 3, 4}, {5, 6, 7, 8}}),
			args: args{
				right:   Ints([]int{1, 2, 3, 4, 5, 6, 7, 8}),
				indexes: Bools([]bool{false, true, true, true, false, false, false, true}),
			},
			want: New([]interface{}{nil, 0, 0, 0, nil, nil, nil, 0}, Int, ""),
		},
		{
			name:        "substract series with the same dimension to no rows",
			inputSeries: Ints([]int{1, 2, 3, 4, 5}),
			args: args{
				right:   Ints([]int{1, 1, 1, 1, 1}),
				indexes: Bools([]bool{false, false, false, false, false}),
			},
			want: New([]interface{}{nil, nil, nil, nil, nil}, Int, ""),
		},
		{
			name:        "substract series with the same dimension to subset of records",
			inputSeries: Ints([]int{1, 2, 3, 4, 5}),
			args: args{
				right:   Ints([]int{1, 1, 1, 1, 1}),
				indexes: Bools([]bool{false, true, true, true, false}),
			},
			want: New([]interface{}{nil, 1, 2, 3, nil}, Int, ""),
		},
		{
			name:        "substract string series is not supported",
			inputSeries: Strings([]string{"a", "b", "c", "d", "e"}),
			args: args{
				right:   Strings([]string{"_1"}),
				indexes: Bools([]bool{false, true, true, true, false}),
			},
			want: Series{Err: fmt.Errorf("operation - is not supported in string type")},
		},
		{
			name:        "substract series with different dimension but the value can be broadcasted to subset of records",
			inputSeries: Floats([]float64{1.5}),
			args: args{
				right:   Ints([]int{1, 1, 2, 3, 0}),
				indexes: Bools([]bool{false, true, true, true, false}),
			},
			want: New([]interface{}{nil, 0.5, -0.5, -1.5, nil}, Float, ""),
		},
		{
			name:        "substract series with different dimension; different type; to subset of records",
			inputSeries: Ints([]int{1, 2, 3, 4, 5}),
			args: args{
				right:   Floats([]float64{2.1, 2.1, 3.1, 3.1, 3.1, 3.2}),
				indexes: Bools([]bool{false, true, true, true, false, false}),
			},
			want: Series{Err: fmt.Errorf("can't do operation due to mismatch length")},
		},
		{
			name:        "could not substract bool series",
			inputSeries: Bools([]bool{true, false, false, true, false}),
			args: args{
				right:   Bools([]bool{true, true, true, false, false}),
				indexes: Bools([]bool{false, true, true, true, true}),
			},
			want: Series{Err: fmt.Errorf("bool is not supported for - operation")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.inputSeries.Substract(tt.args.right, tt.args.indexes); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.Substract() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_Multiply(t *testing.T) {
	type args struct {
		right   Series
		indexes Indexes
	}
	tests := []struct {
		name        string
		inputSeries Series
		args        args
		want        Series
	}{
		{
			name:        "multiply series with the same dimension to all records",
			inputSeries: Ints([]int{1, 2, 3, 4, 5}),
			args: args{
				right:   Ints([]int{1, 1, 1, 1, 1}),
				indexes: Bools([]bool{true, true, true, true, true}),
			},
			want: Ints([]int{1, 2, 3, 4, 5}),
		},
		{
			name:        "multiply series with the same dimension to subset of records",
			inputSeries: Ints([]int{1, 2, 3, 4, 5}),
			args: args{
				right:   Ints([]int{2, 2, 3, 4, 5}),
				indexes: Bools([]bool{false, true, true, true, false}),
			},
			want: New([]interface{}{nil, 4, 9, 16, nil}, Int, ""),
		},
		{
			name:        "multiply list series with the same dimension to subset of records",
			inputSeries: IntsList([][]int{{1, 2, 3, 4}, {5, 6, 7, 8}}),
			args: args{
				right:   Ints([]int{1, 2, 3, 4, 5, 6, 7, 8}),
				indexes: Bools([]bool{false, true, true, true, false, false, false, true}),
			},
			want: New([]interface{}{nil, 4, 9, 16, nil, nil, nil, 64}, Int, ""),
		},
		{
			name:        "multiply series with the same dimension to no rows",
			inputSeries: Ints([]int{1, 2, 3, 4, 5}),
			args: args{
				right:   Ints([]int{1, 1, 1, 1, 1}),
				indexes: Bools([]bool{false, false, false, false, false}),
			},
			want: New([]interface{}{nil, nil, nil, nil, nil}, Int, ""),
		},
		{
			name:        "multiply series with the same dimension to subset of records",
			inputSeries: Ints([]int{1, 2, 3, 4, 5}),
			args: args{
				right:   Ints([]int{1, 2, 3, 4, 5}),
				indexes: Bools([]bool{false, true, true, true, false}),
			},
			want: New([]interface{}{nil, 4, 9, 16, nil}, Int, ""),
		},
		{
			name:        "multiply string series is not supported",
			inputSeries: Strings([]string{"a", "b", "c", "d", "e"}),
			args: args{
				right:   Strings([]string{"_1"}),
				indexes: Bools([]bool{false, true, true, true, false}),
			},
			want: Series{Err: fmt.Errorf("operation * is not supported in string type")},
		},
		{
			name:        "multiply series with different dimension but the value can be broadcasted to subset of records",
			inputSeries: Floats([]float64{1.5}),
			args: args{
				right:   Ints([]int{1, 1, 2, 3, 0}),
				indexes: Bools([]bool{false, true, true, true, false}),
			},
			want: New([]interface{}{nil, 1.5, 3.0, 4.5, nil}, Float, ""),
		},
		{
			name:        "multiply series with different dimension; different type; to subset of records",
			inputSeries: Ints([]int{1, 2, 3, 4, 5}),
			args: args{
				right:   Floats([]float64{2.1, 2.1, 3.1, 3.1, 3.1, 3.2}),
				indexes: Bools([]bool{false, true, true, true, false, false}),
			},
			want: Series{Err: fmt.Errorf("can't do operation due to mismatch length")},
		},
		{
			name:        "could not multply bool series",
			inputSeries: Bools([]bool{true, false, false, true, false}),
			args: args{
				right:   Bools([]bool{true, true, true, false, false}),
				indexes: Bools([]bool{false, true, true, true, true}),
			},
			want: Series{Err: fmt.Errorf("bool is not supported for * operation")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.inputSeries.Multiply(tt.args.right, tt.args.indexes); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.Multiply() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_Divide(t *testing.T) {
	type args struct {
		right   Series
		indexes Indexes
	}
	tests := []struct {
		name        string
		inputSeries Series
		args        args
		want        Series
	}{
		{
			name:        "divide series with the same dimension to all records",
			inputSeries: Ints([]int{1, 2, 3, 4, 5}),
			args: args{
				right:   Ints([]int{1, 1, 1, 1, 1}),
				indexes: Bools([]bool{true, true, true, true, true}),
			},
			want: Ints([]int{1, 2, 3, 4, 5}),
		},
		{
			name:        "divide series with the same dimension to subset of records",
			inputSeries: Ints([]int{1, 2, 3, 4, 5}),
			args: args{
				right:   Ints([]int{2, 2, 3, 4, 2}),
				indexes: Bools([]bool{false, true, true, true, true}),
			},
			want: New([]interface{}{nil, 1, 1, 1, 2}, Int, ""),
		},
		{
			name:        "divide series with the same dimension to no rows",
			inputSeries: Ints([]int{1, 2, 3, 4, 5}),
			args: args{
				right:   Ints([]int{1, 1, 1, 1, 1}),
				indexes: Bools([]bool{false, false, false, false, false}),
			},
			want: New([]interface{}{nil, nil, nil, nil, nil}, Int, ""),
		},
		{
			name:        "divide list series with the same dimension to subset of records",
			inputSeries: IntsList([][]int{{1, 2, 3, 4}, {5, 6, 7, 8}}),
			args: args{
				right:   Ints([]int{1, 2, 3, 4, 5, 6, 7, 8}),
				indexes: Bools([]bool{false, true, true, true, false, false, false, true}),
			},
			want: New([]interface{}{nil, 1, 1, 1, nil, nil, nil, 1}, Int, ""),
		},
		{
			name:        "divide series with the same dimension to subset of records",
			inputSeries: Ints([]int{1, 2, 3, 4, 5}),
			args: args{
				right:   Ints([]int{1, 2, 3, 0, 5}),
				indexes: Bools([]bool{false, true, true, true, false}),
			},
			want: New([]interface{}{nil, 1, 1, nil, nil}, Int, ""),
		},
		{
			name:        "divide string series is not supported",
			inputSeries: Strings([]string{"a", "b", "c", "d", "e"}),
			args: args{
				right:   Strings([]string{"_1"}),
				indexes: Bools([]bool{false, true, true, true, false}),
			},
			want: Series{Err: fmt.Errorf("operation / is not supported in string type")},
		},
		{
			name:        "divide series with different dimension but the value can be broadcasted to subset of records",
			inputSeries: Floats([]float64{1.5}),
			args: args{
				right:   Ints([]int{1, 1, 2, 3, 0}),
				indexes: Bools([]bool{false, true, true, true, false}),
			},
			want: New([]interface{}{nil, 1.5, 0.75, 0.5, nil}, Float, ""),
		},
		{
			name:        "divide series with different dimension; different type; to subset of records",
			inputSeries: Ints([]int{1, 2, 3, 4, 5}),
			args: args{
				right:   Floats([]float64{2.1, 2.1, 3.1, 3.1, 3.1, 3.2}),
				indexes: Bools([]bool{false, true, true, true, false, false}),
			},
			want: Series{Err: fmt.Errorf("can't do operation due to mismatch length")},
		},
		{
			name:        "could not divide bool series",
			inputSeries: Bools([]bool{true, false, false, true, false}),
			args: args{
				right:   Bools([]bool{true, true, true, false, false}),
				indexes: Bools([]bool{false, true, true, true, true}),
			},
			want: Series{Err: fmt.Errorf("bool is not supported for / operation")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.inputSeries.Divide(tt.args.right, tt.args.indexes); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.Divide() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_Modulo(t *testing.T) {
	type args struct {
		right   Series
		indexes Indexes
	}
	tests := []struct {
		name        string
		inputSeries Series
		args        args
		want        Series
	}{
		{
			name:        "modulo series with the same dimension to all records",
			inputSeries: Ints([]int{1, 2, 3, 4, 5}),
			args: args{
				right:   Ints([]int{1, 1, 1, 1, 1}),
				indexes: Bools([]bool{true, true, true, true, true}),
			},
			want: Ints([]int{0, 0, 0, 0, 0}),
		},
		{
			name:        "modulo series with the same dimension to subset of records",
			inputSeries: Ints([]int{1, 2, 3, 4, 5}),
			args: args{
				right:   Ints([]int{2, 2, 2, 3, 3}),
				indexes: Bools([]bool{false, true, true, true, true}),
			},
			want: New([]interface{}{nil, 0, 1, 1, 2}, Int, ""),
		},
		{
			name:        "modulo series with the same dimension to no rows",
			inputSeries: Ints([]int{1, 2, 3, 4, 5}),
			args: args{
				right:   Ints([]int{1, 1, 1, 1, 1}),
				indexes: Bools([]bool{false, false, false, false, false}),
			},
			want: New([]interface{}{nil, nil, nil, nil, nil}, Int, ""),
		},
		{
			name:        "modulo list series with the same dimension to subset of records",
			inputSeries: IntsList([][]int{{1, 2, 3, 4}, {5, 6, 7, 8}}),
			args: args{
				right:   Ints([]int{1, 2, 3, 4, 5, 6, 7, 8}),
				indexes: Bools([]bool{false, true, true, true, false, false, false, true}),
			},
			want: New([]interface{}{nil, 0, 0, 0, nil, nil, nil, 0}, Int, ""),
		},
		{
			name:        "modulo series with the same dimension to subset of records, modulo with 0 value",
			inputSeries: Ints([]int{1, 2, 3, 4, 5}),
			args: args{
				right:   Ints([]int{1, 2, 3, 0, 5}),
				indexes: Bools([]bool{false, true, true, true, false}),
			},
			want: New([]interface{}{nil, 0, 0, nil, nil}, Int, ""),
		},
		{
			name:        "modulo string series is not supported",
			inputSeries: Strings([]string{"a", "b", "c", "d", "e"}),
			args: args{
				right:   Strings([]string{"_1"}),
				indexes: Bools([]bool{false, true, true, true, false}),
			},
			want: Series{Err: fmt.Errorf("operation %% is not supported in string type")},
		},
		{
			name:        "module float series is not supported",
			inputSeries: Floats([]float64{1.1, 2.2, 3.3, 4.4, 5.5}),
			args: args{
				right:   Ints([]int{1, 2, 3, 0, 5}),
				indexes: Bools([]bool{false, true, true, true, false}),
			},
			want: Series{Err: fmt.Errorf("operation %% is not supported in float type")},
		},
		{
			name:        "modulo bool series is not supported",
			inputSeries: Bools([]bool{true, false, false, true, false}),
			args: args{
				right:   Bools([]bool{true, true, true, false, false}),
				indexes: Bools([]bool{false, true, true, true, true}),
			},
			want: Series{Err: fmt.Errorf("bool is not supported for %% operation")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.inputSeries.Modulo(tt.args.right, tt.args.indexes); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.Modulo() = %v, want %v", got, tt.want)
			}
		})
	}
}
