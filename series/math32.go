package series

import "math"

type math32T struct{}

// pseudo math32 package. use github.com/chewxy/math32 if you need more
var math32 math32T

func (math32T) NaN() float32         { return float32(math.NaN()) }
func (math32T) IsNaN(f float32) bool { return math.IsNaN(float64(f)) }
func (math32T) IsInf(f float32, sign int) bool {
	if sign < 0 {
		return f < -math.MaxFloat32
	}
	return f > math.MaxFloat32

}
