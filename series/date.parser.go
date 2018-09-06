package series

import (
	"strings"
	"time"

	"github.com/araddon/dateparse"
	"github.com/metakeule/fmtdate"
)

// ParseDateTimeFormat parse date and returns time format
func ParseDateTimeFormat(date string) (string, error) {
	f, err := dateparse.ParseFormat(date)
	if err != nil && strings.Contains(err.Error(), "month out of range") {
		format := "DD.MM.YYYY"
		if strings.Contains(date, "/") {
			format = "DD/MM/YYYY"
		}
		return format, nil
	}
	return f, err
}

// ParseDateTime parse string to time.Time
func ParseDateTime(date string) (time.Time, error) {
	ts, err := dateparse.ParseAny(date)
	if err != nil && strings.Contains(err.Error(), "month out of range") {
		format := "DD.MM.YYYY"
		if strings.Contains(date, "/") {
			format = "DD/MM/YYYY"
		}

		ts, err = fmtdate.Parse(format, date)
		return ts, err

	}

	return ts, err
}
