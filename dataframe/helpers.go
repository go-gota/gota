package dataframe

import "errors"

func transposeRecords(x [][]string) [][]string {
	n := len(x)
	if n == 0 {
		return x
	}
	m := len(x[0])
	y := make([][]string, m)
	for i := 0; i < m; i++ {
		z := make([]string, n)
		for j := 0; j < n; j++ {
			z[j] = x[j][i]
		}
		y[i] = z
	}
	return y
}

func orBool(a []bool, b []bool) ([]bool, error) {
	if len(a) != len(b) {
		return nil, errors.New("Different lengths")
	}
	ret := make([]bool, len(a), len(a))
	for i := 0; i < len(a); i++ {
		ret[i] = a[i] || b[i]
	}
	return ret, nil
}

func inIntSlice(i int, is []int) bool {
	for _, v := range is {
		if v == i {
			return true
		}
	}
	return false
}
