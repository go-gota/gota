package main

import (
	"time"

	"github.com/kniren/gota/data-frame"
)

type test struct {
	this   int
	is     string
	sparta float64
}

func main() {
	num := 1
	df.New(
		df.C{"A", []string{"a", "b", "c"}},
		df.C{"B", []*int{&num, nil, &num}},
		df.C{"C", []int{1, 2, 3}},
		df.C{"D", []time.Time{time.Now(), time.Now(), time.Now()}},
		df.C{"E", []test{test{1, "2", 3.4}}},
	)
}
