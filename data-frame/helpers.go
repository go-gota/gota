package df

import "strings"

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

func addRightPadding(s string, nchar int) string {
	if len(s) < nchar {
		return s + strings.Repeat(" ", nchar-len(s))
	}
	return s
}

func addLeftPadding(s string, nchar int) string {
	if len(s) < nchar {
		return strings.Repeat(" ", nchar-len(s)) + s
	}
	return s
}
