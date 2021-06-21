package series

import (
	"log"
)


func (s Series) And(in interface{}) Series {
	inSeries := New(in, s.t, "")
	result, err := Operation(func(index int, eles ...Element) interface{} {
		e0b,_ := eles[0].Bool()
		e1b,_ := eles[1].Bool()
		return e0b && e1b
	}, s, inSeries)
	if err != nil {
		log.Panic(err)
	}
	return result	
}

func (s Series) Or(in interface{}) Series {
	inSeries := New(in, s.t, "")
	result, err := Operation(func(index int, eles ...Element) interface{} {
		e0b,_ := eles[0].Bool()
		e1b,_ := eles[1].Bool()
		return e0b || e1b
	}, s, inSeries)
	if err != nil {
		log.Panic(err)
	}
	return result
}