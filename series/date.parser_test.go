package series

import (
	"testing"
	"time"

	"github.com/araddon/dateparse"
)

func TestParseNumberDashText(t *testing.T) {

	testCases := []struct {
		text        string
		shouldParse bool
	}{
		{"2150Sometext", false},
		//	{"2150-Aug asdf", false},
	}

	for idx, test := range testCases {

		time1, err := dateparse.ParseStrict(test.text)
		if err == nil && !test.shouldParse {
			t.Errorf("%d: Fail Text: %v Error:%v Time:%v", idx, test.text, err, time1)
			format, _ := dateparse.ParseFormat(test.text)
			t.Errorf(" \t Format:%v ", format)

			time1, err = time.Parse(format, test.text)
			t.Errorf("--- %v --- %v", time1, err)
		}
	}

}
