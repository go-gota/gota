package series

import (
	"log"
)


func (s series) And(in interface{}) Series {
	inSeries := New(in, Bool, "")
	result, err := Operation(func(index int, eles ...Element) interface{} {
		e0b, err := eles[0].Bool()
		if err != nil {
			return nil
		}
		e1b, err := eles[1].Bool()
		if err != nil {
			return nil
		}
		return e0b && e1b
	}, &s, inSeries)
	if err != nil {
		log.Panic(err)
	}
	return result	
}

func (s series) Or(in interface{}) Series {
	inSeries := New(in, Bool, "")
	result, err := Operation(func(index int, eles ...Element) interface{} {
		e0b, err := eles[0].Bool()
		if err != nil {
			return nil
		}
		e1b, err := eles[1].Bool()
		if err != nil {
			return nil
		}
		return e0b || e1b
	}, &s, inSeries)
	if err != nil {
		log.Panic(err)
	}
	return result
}