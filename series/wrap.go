package series

//Wrapper define special operations for multiple Series
type Wrapper interface {
	FloatApply(f func(thisValue float64, wrapValues []float64) float64) Series
	BoolApply(f func(thisValue bool, wrapValues []bool) bool) Series
}

//wrapper implements Wrapper
type wrapper struct {
	thisSeries Series
	ss         []Series
}

func newWrapper(this Series, ss []Series) Wrapper {
	if len(ss) > 0 {
		expectedLen := this.Len()
		for i := 0; i < len(ss); i++ {
			if expectedLen != ss[i].Len() {
				panic("wrappered series must have the same length")
			}
		}
	}
	w := wrapper{thisSeries: this, ss: ss}
	return w
}

func (w wrapper) FloatApply(f func(thisValue float64, wrapValues []float64) float64) Series {
	length := w.thisSeries.Len()
	elements := make(floatElements, length)
	for i := 0; i < length; i++ {
		elements[i].SetFloat(f(w.thisSeries.Elem(i).Float(), rowFloats(i, w.ss)))
	}
	ret := &series{
		name:     "",
		elements: elements,
		t:        Float,
		err:      nil,
	}
	return ret
}

func (w wrapper) BoolApply(f func(thisValue bool, wrapValues []bool) bool) Series {
	length := w.thisSeries.Len()
	elements := make(boolElements, length)
	for i := 0; i < length; i++ {
		thisB, err := w.thisSeries.Elem(i).Bool()
		if err != nil {
			return Err(err)
		}
		wrapBs, err := rowBools(i, w.ss)
		if err != nil {
			return Err(err)
		}
		elements[i].SetBool(f(thisB, wrapBs))
	}
	ret := &series{
		name:     "",
		elements: elements,
		t:        Bool,
		err:      nil,
	}
	return ret
}

func rowBools(index int, ss []Series) ([]bool, error) {
	length := len(ss)
	if length == 0 {
		return nil, nil
	}
	ret := make([]bool, length)
	var err error
	for i := 0; i < length; i++ {
		ret[i], err = ss[i].Elem(index).Bool()
		if err != nil {
			return nil, err
		}
	}
	return ret, nil
}

func rowFloats(index int, ss []Series) []float64 {
	length := len(ss)
	if length == 0 {
		return nil
	}
	ret := make([]float64, length)
	for i := 0; i < length; i++ {
		ret[i] = ss[i].Elem(index).Float()
	}
	return ret
}
