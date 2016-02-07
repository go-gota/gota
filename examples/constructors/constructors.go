package main

import "github.com/kniren/gota/data-frame"

func main() {
	df.New(
		df.C{"test", []string{"a", "b", "c"}},
		df.C{"otherTest", []int{1, 2, 3}},
	)
}
