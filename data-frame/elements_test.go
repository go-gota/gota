package df

import "testing"

func TestElements_Set(t *testing.T) {
	s := Strings("A", "B", nil, "C")
	e, err := s.elements.(stringElements).Set(2, "B")
	if err != nil {
		t.Error("Expected Success. Got error")
	}
	s.elements = e
	e, err = s.elements.(stringElements).Set(3, 3.2)
	if err != nil {
		t.Error("Expected Success. Got error")
	}
	s.elements = e
	e, err = s.elements.(stringElements).Set(0, 1)
	if err != nil {
		t.Error("Expected Success. Got error")
	}
	s.elements = e
	e, err = s.elements.(stringElements).Set(0, Ints(2))
	if err != nil {
		t.Error("Expected Success. Got error")
	}
	s.elements = e
	b := Ints(1, 2, 3, nil, 4)
	c, err := b.elements.(intElements).Set(0, Ints(2))
	if err != nil {
		t.Error("Expected Success. Got error")
	}
	b.elements = c
}
