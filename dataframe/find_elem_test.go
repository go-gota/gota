package dataframe

import (
	"testing"

	"github.com/go-gota/gota/series"
)

func TestFindElem(t *testing.T) {
	/*  Input is a 5x4 DataFrame

	   Strings  Floats   Primes Naturals
	0: e        2.718000 1      1
	1: Pi       3.142000 3      2
	2: Phi      1.618000 5      3
	3: Sqrt2    1.414000 7      4
	4: Ln2      0.693000 11     5
	   <string> <float>  <int>  <int>
	*/
	df := New(
		series.New([]string{"e", "Pi", "Phi", "Sqrt2", "Ln2"}, series.String, "Strings"),
		series.New([]int{1, 3, 5, 7, 11}, series.Int, "Ints"),
		series.New([]float64{2.718, 3.142, 1.618, 1.414, 0.693}, series.Float, "Floats"),
		series.New([]bool{false, true, false, false, false}, series.Bool, "Bools"),
	)

	t.Run("String lookup of float value", func(t *testing.T) {
		e, ok := df.FindElem("Strings", "Pi", "Floats")
		if !ok {
			t.Fatal("failed to find value")
		}
		observed := e.Float()
		expected := 3.142
		if observed != expected {
			t.Fatalf("values did not match - expected %f but got %f", expected, observed)
		}
	})

	t.Run("Float lookup of string value", func(t *testing.T) {
		e, ok := df.FindElem("Floats", 3.142, "Strings")
		if !ok {
			t.Fatal("failed to find value")
		}
		observed := e.String()
		expected := "Pi"
		if observed != expected {
			t.Fatalf("values did not match - expected %s but got %s", expected, observed)
		}
	})

	t.Run("Int lookup of bool value", func(t *testing.T) {
		e, ok := df.FindElem("Ints", 3, "Bools")
		if !ok {
			t.Fatal("failed to find value")
		}
		observed, _ := e.Bool()
		expected := true
		if observed != expected {
			t.Fatalf("values did not match - expected %t but got %t", expected, observed)
		}
	})

	t.Run("Bool lookup of int value", func(t *testing.T) {
		e, ok := df.FindElem("Bools", true, "Ints")
		if !ok {
			t.Fatal("failed to find value")
		}
		observed, _ := e.Int()
		expected := 3
		if observed != expected {
			t.Fatalf("values did not match - expected %d but got %d", expected, observed)
		}
	})

	t.Run("Multiple matches returns first", func(t *testing.T) {
		e, ok := df.FindElem("Bools", false, "Ints")
		if !ok {
			t.Fatal("failed to find value")
		}
		observed, _ := e.Int()
		expected := 1
		if observed != expected {
			t.Fatalf("values did not match - expected %d but got %d", expected, observed)
		}
	})

	t.Run("First column not found sets ok to false", func(t *testing.T) {
		_, ok := df.FindElem("Eentz", 11, "Strings")
		if ok {
			t.Fatal("expected ok false")
		}
	})

	t.Run("Key not found sets ok to false", func(t *testing.T) {
		_, ok := df.FindElem("Ints", 12, "Strings")
		if ok {
			t.Fatal("expected ok false")
		}
	})

	t.Run("Second column not found sets ok to false", func(t *testing.T) {
		_, ok := df.FindElem("Ints", 11, "Ropes")
		if ok {
			t.Fatal("expected ok false")
		}
	})
}
