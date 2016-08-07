package df

import (
	"math"
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

func Range(start, end int) []int {
	if start > end {
		start, end = end, start
	}
	var arr []int
	for i := start; i <= end; i++ {
		arr = append(arr, i)
	}
	return arr
}

func Seq(start, end, step int) []int {
	if start > end {
		start, end = end, start
	}
	if step == 0 {
		return []int{}
	}
	var arr []int
	if step < 0 {
		step = int(math.Abs(float64(step)))
		for i := end; i >= start; i = i - step {
			arr = append(arr, i)
		}
		return arr
	} else {
		for i := start; i <= end; i = i + step {
			arr = append(arr, i)
		}
		return arr
	}
}
