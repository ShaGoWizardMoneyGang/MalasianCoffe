package parser

import "time"

func ToYearMonth(dateStr string) string {
	t, err := time.Parse("2006-01-02 15:04:05", dateStr)
	if err != nil {
		return ""
	}
	return t.Format("2006-01")
}
