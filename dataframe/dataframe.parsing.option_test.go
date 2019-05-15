package dataframe

import (
	"fmt"
	"reflect"
	"testing"
	"strings"
)

func testCompareDataframes(df1, df2 *DataFrame) error {
	// Check that the types are the same between both DataFrames
	if !reflect.DeepEqual(df1.Types(), df2.Types()) {
		return fmt.Errorf("Different types:\nA:%v\nB:%v", df1.Types(), df2.Types())
	}

	// Check that the colnames are the same between both DataFrames
	if !reflect.DeepEqual(df1.Names(), df2.Names()) {
		return fmt.Errorf("Different colnames:\nA:%v\nB:%v", df1.Names(), df2.Names())
	}	

	// Check that the values are the same between both DataFrames
	if !reflect.DeepEqual(df1.Records(), df1.Records()) {
		return fmt.Errorf("Different values:\nA:%v\nB:%v", df1.Records(), df2.Records())
	}		

	return nil
}

func TestDataFrame_CustomTrimer_RemovedSuffixes(t *testing.T) {
	// arrange
	customParser := func(val string) string {
		suffixes := []string {
			"££", "£", // pounds
			"$$", "$", // dollars
		}

		result := val

		for _, suffix := range suffixes {
			result = strings.TrimSuffix(result, suffix)
		}

		return result
	}

	expected := LoadRecords(
		[][]string{
			[]string{"A", "B", "C", "D"},
			[]string{"1", "4", "5.1", "true"},
			[]string{"2", "5", "7.0", "true"},
			[]string{"35", "4", "6.02", "true"},
			[]string{"4", "2", "7.1", "false"},
			[]string{"5", "4", "5.1", "true"},
			[]string{"6", "5", "7.0", "true"},
			[]string{"7", "4", "8", "true"},
			[]string{"8", "2", "7.1", "false"},
			[]string{"9", "4", "5.1", "true"},
			[]string{"10", "5", "7.0", "true"},
			[]string{"11", "4", "8", "true"},
			[]string{"12", "2", "7.1", "false"},						
		})

	// act
	actual := LoadRecords(
		[][]string{
			[]string{"A", "B", "C", "D"},
			[]string{"1", "4", "5.1", "true"},
			[]string{"2", "5", "7.0", "true"},
			[]string{"35££", "4", "6.02$$", "true"},
			[]string{"4", "2", "7.1", "false"},
			[]string{"5", "4", "5.1", "true"},
			[]string{"6", "5", "7.0", "true"},
			[]string{"7", "4", "8", "true"},
			[]string{"8", "2", "7.1", "false"},
			[]string{"9", "4", "5.1", "true"},
			[]string{"10", "5", "7.0", "true"},
			[]string{"11", "4", "8", "true"},
			[]string{"12", "2", "7.1", "false"},						
		},
		OnCustomTrimer(customParser),
	)

	// assert	
	if err := testCompareDataframes(&expected, &actual); err != nil {
		t.Error(err)
	}
}