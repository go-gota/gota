package df

import (
	"reflect"
	"testing"
)

func TestDataFrame_New(t *testing.T) {
	a := New(Strings("b"), Ints(1, 2))
	if a.Err() == nil {
		t.Error("Expected error, got success")
	}
	a = New(Strings("b", "a"), NamedInts("Y", 1, 2), Floats(3.0, 4.0))
	if a.Err() != nil {
		t.Error("Expected success, got error")
	}
	expectedNames := []string{"X0", "Y", "X1"}
	receivedNames := a.colnames
	if !reflect.DeepEqual(expectedNames, receivedNames) {
		t.Error(
			"Expected Names:",
			expectedNames,
			"Received Names:",
			receivedNames,
		)
	}
	expectedTypes := []string{"string", "int", "float"}
	receivedTypes := a.coltypes
	if !reflect.DeepEqual(expectedTypes, receivedTypes) {
		t.Error(
			"Expected Types:",
			expectedTypes,
			"Received Types:",
			receivedTypes,
		)
	}
	// TODO: Check that df.colnames == columns.colnames
	// TODO: Check that the address of the columns are different that of the original series
	// TODO: Check that dimensions match
	a = New()
	if a.Err() == nil {
		t.Error("Expected error, got success")
	}
}
