package df

import (
	"reflect"
	"testing"
)

func TestDataFrame_New(t *testing.T) {
	_, err := New(Strings("b"), Ints(1, 2))
	if err == nil {
		t.Error("Expected error, got success")
	}
	a, err := New(Strings("b", "a"), Ints(1, 2), Floats(3.0, 4.0))
	if err != nil {
		t.Error("Expected success, got error")
	}
	expectedNames := []string{"X0", "X1", "X2"}
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
	_, err = New()
	if err == nil {
		t.Error("Expected error, got success")
	}
}
