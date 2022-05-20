package series

// All the operations on it will influence the DataFrame's content.
type Self struct {
	this Series
}

// All the operations on Self will influence the Series's content.
func (s *series) Self() Self {
	self := Self{
		this: s,
	}
	return self
}

// Apply applies the given function to the element of a Series, will influence the Series's content.
func (s Self) Apply(f func(ele Element, index int)) {
	for i := 0; i < s.this.Len(); i++ {
		f(s.this.Elem(i), i)
	}
}
