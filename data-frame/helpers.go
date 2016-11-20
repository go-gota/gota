package df

import (
	"errors"
	"strconv"
	"strings"
	"unicode/utf8"
)

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
	if utf8.RuneCountInString(s) < nchar {
		return s + strings.Repeat(" ", nchar-utf8.RuneCountInString(s))
	}
	return s
}

func addLeftPadding(s string, nchar int) string {
	if utf8.RuneCountInString(s) < nchar {
		return strings.Repeat(" ", nchar-utf8.RuneCountInString(s)) + s
	}
	return s
}

func findType(arr []string) Type {
	hasFloats := false
	hasInts := false
	hasBools := false
	hasStrings := false
	for _, str := range arr {
		if _, err := strconv.Atoi(str); err == nil {
			hasInts = true
			continue
		}
		if _, err := strconv.ParseFloat(str, 64); err == nil {
			hasFloats = true
			continue
		}
		if str == "true" || str == "false" {
			hasBools = true
			continue
		}
		if str == "" || str == "NA" {
			continue
		}
		hasStrings = true
	}
	if hasFloats && !hasBools && !hasStrings {
		return Float
	}
	if hasInts && !hasFloats && !hasBools && !hasStrings {
		return Int
	}
	if !hasInts && !hasFloats && hasBools && !hasStrings {
		return Bool
	}
	return String
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
