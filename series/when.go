package series

type WhenFilterFunction func(ele Element, index int) bool
type WhenApplyFunction func(newEle Element, index int)

//When defines a conditional computation
type When interface {
	//We do the operation on the elements that satisfy the condition and do nothing on the elements that dose not satisfy the condition.
	Apply(f WhenApplyFunction) Series
}

func newWhen(whenF WhenFilterFunction, s Series) When {
	w := when{
		whenF: whenF,
		s:     s,
	}
	return w
}

type when struct {
	whenF WhenFilterFunction
	s     Series
}

func (e when) Apply(f WhenApplyFunction) Series {
	ret := e.s.Map(func(ele Element, index int) Element {
		newEle := ele.Copy()
		if e.whenF(ele, index) {
			f(newEle, index)
		}
		return newEle
	})
	return ret
}
