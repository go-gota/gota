package series

import "math"

// RollingWindow is used for rolling window calculations.
type RollingWindow struct {
	window int
	series Series
}

// Rolling creates new RollingWindow
func (s Series) Rolling(window int) RollingWindow {
	return RollingWindow{
		window: window,
		series: s,
	}
}

// Mean returns the rolling mean.
func (r RollingWindow) Mean() (s Series) {
	s = New([]float64{}, Float, "mean")

	for i := 1; i <= r.series.Len(); i++ {
		if i < r.window {
			s.Append(math.NaN())
			continue
		}

		index := []int{}
		for j := i - r.window; j < i; j++ {
			index = append(index, j)
		}

		mean := r.series.Subset(index).Mean()
		s.Append(mean)
	}

	return
}
