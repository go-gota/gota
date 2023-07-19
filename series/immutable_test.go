package series

import (
	"fmt"
	"strings"
	"testing"
)

func TestImmutableSeries_ModifyPanic(t *testing.T) {
	tests := []struct {
		series       Series
		modifySeries func(Series)
	}{
		{
			Ints([]string{"2", "1", "3", "NaN", "4", "NaN"}),
			func(s Series) {
				s.Elem(0).SetString(NaN)
			},
		},
		{
			Floats([]string{"2", "1", "3", "NaN", "4", "NaN"}),
			func(s Series) {
				s.FillNaN("1")
			},
		},
		{
			Strings([]string{"c", "b", "a"}),
			func(s Series) {
				s.FillNaNForward()
			},
		},
		{
			Bools([]bool{true, false, false, false, true}),
			func(s Series) {
				s.FillNaNBackward()
			},
		},
		{
			Strings([]string{"c", "b", "a"}),
			func(s Series) {
				s.Set(0, NewDefault("a", String, "", 1))
			},
		},
		{
			Bools([]bool{true, false, false, false, true}),
			func(s Series) {
				s.Append([]bool{true, false})
			},
		},
	}
	for testnum, test := range tests {
		received := test.series.Immutable()
		modifySeries := test.modifySeries
		name := fmt.Sprintf("Test-%d", testnum)
		t.Run(name, func(t *testing.T) {
			defer func() {
				err := recover()
				if err == nil || !strings.Contains(err.(string), "is not supported by") {
					t.Errorf("Test:%v\nError, must panic: %v", testnum, err)
				}
			}()
			modifySeries(received)
		})
	}
}
