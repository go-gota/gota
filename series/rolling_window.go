package series

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
	s = New([]float64{}, Float, "Mean")
	for _, block := range r.getBlocks() {
		s.Append(block.Mean())
	}

	return
}

// StdDev returns the rolling mean.
func (r RollingWindow) StdDev() (s Series) {
	s = New([]float64{}, Float, "StdDev")
	for _, block := range r.getBlocks() {
		s.Append(block.StdDev())
	}

	return
}

func (r RollingWindow) getBlocks() (blocks []Series) {
	for i := 1; i <= r.series.Len(); i++ {
		if i < r.window {
			blocks = append(blocks, r.series.Empty())
			continue
		}

		index := []int{}
		for j := i - r.window; j < i; j++ {
			index = append(index, j)
		}
		blocks = append(blocks, r.series.Subset(index))
	}

	return
}
