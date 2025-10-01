package global_aggregator

/*
import (
	"strings"
	"testing"
)

func TestAggregateByMonth(t *testing.T) {
	in := strings.Join([]string{
		"5,63.5,2024-01-01 10:06:50",
		"5,10.5,2024-01-15 08:00:00",
		"5,1.0,2024-01-31 23:59:59",
	}, "\n")

	esperado := "2024-01,5,75.00\n"

	var ag Aggregator
	resultado := ag.AggregatorGlobal3ByMonthTPV(in)

	if esperado != resultado {
		panic("no coincide lo procesado con el resultado esperdo")
	}
}

func TestAggregateByMonthMultipleMonthsAndStores(t *testing.T) {
	in := strings.Join([]string{
		// store 5 en enero
		"5,63.5,2024-01-01 10:06:50",
		"5,10.5,2024-01-15 08:00:00",
		// store 7 en enero
		"7,2.0,2024-01-02 00:00:00",
		// store 5 en febrero
		"5,4.0,2024-02-01 00:00:00",
		"5,6.0,2024-02-28 12:34:56",
		// store 3 en febrero
		"3,1.5,2024-02-10 09:10:11",
	}, "\n")

	esperado := strings.Join([]string{
		"2024-01,5,74.00",
		"2024-01,7,2.00",
		"2024-02,3,1.50",
		"2024-02,5,10.00",
	}, "\n") + "\n"

	var ag Aggregator
	resultado := ag.Aggregator3ByMonthTPV(in)

	if esperado != resultado {
		panic("no coincide lo procesado con el resultado esperdo")
	}
}
*/
