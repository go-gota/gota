package df

import (
	"strconv"
	"strings"
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

func findType(arr []string) string {
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
		return "float"
	}
	if hasInts && !hasFloats && !hasBools && !hasStrings {
		return "int"
	}
	if !hasInts && !hasFloats && hasBools && !hasStrings {
		return "bool"
	}
	return "string"
}
