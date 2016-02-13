package main

import (
	"fmt"

	"github.com/kniren/gota/data-frame"
)

type test struct {
	this   int
	is     string
	sparta float64
}

func main() {
	d, _ := df.New(
		df.C{"A", df.Strings("a", "b", "c")},
		df.C{"B", df.Ints(1, nil, 2)},
		df.C{"C", df.Ints(1, 2, 3)},
	)

	fmt.Println(d)
}
