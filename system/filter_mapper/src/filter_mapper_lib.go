package filter_mapper

import (
	"fmt"
	"malasian_coffe/packets/packet"
	"math"
	"strconv"
	"strings"
	"time"
)

type filterMapperOptions func(string) string

func filterByYearCommon(input string) string {
	input_lines := strings.Split(input, "\n")
	final := ""
	for _, line := range input_lines {
		data := strings.Split(line, ",")
		if len(data) < 9 {
			panic("Invalid data format")
		}
		if yearCondition(data) {
			final += line + "\n"
		}
	}
	return final
}

func yearCondition(data []string) bool {
	layout := "2006-01-02 15:04:05" // Go's reference layout
	t, _ := time.Parse(layout, data[8])
	return t.Year() >= 2024 && t.Year() <= 2025
}

func filterFunctionQuery1(input string) string {
	lines := strings.Split(input, "\n")

	final := ""
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		data := strings.Split(line, ",")
		if len(data) < 9 {
			panic("Invalid data format")
		}
		amount, _ := strconv.ParseFloat(data[7], 64)
		amount = math.Round(amount*10) / 10
		if yearCondition(data) && amount >= 15.0 {
			final += data[0] + "," + strconv.FormatFloat(amount, 'f', 1, 64) + "\n"
		}
	}
	return final
}

func filterFunctionQuery2a(input string) string {
	lines := strings.Split(input, "\n")
	final := ""
	for _, line := range lines {
		data := strings.Split(line, ",")
		if len(data) < 6 {
			panic("Invalid data format")
		}
		layout := "2006-01-02 15:04:05" // Go's reference layout
		t, _ := time.Parse(layout, data[5])
		if t.Year() >= 2024 && t.Year() <= 2025 {
			final += data[1] + "," + data[2] + "," + data[5] + "\n"
		}
	}
	return final
}

func filterFunctionQuery2b(input string) string {
	lines := strings.Split(input, "\n")
	final := ""
	for _, line := range lines {
		data := strings.Split(line, ",")
		if len(data) < 6 {
			panic("Invalid data format")
		}
		layout := "2006-01-02 15:04:05" // Go's reference layout
		t, _ := time.Parse(layout, data[5])
		if t.Year() >= 2024 && t.Year() <= 2025 {
			final += data[1] + "," + data[4] + "," + data[5] + "\n"
		}
	}
	return final
}

func mapStoreIdAndName(input string) string {
	lines := strings.Split(input, "\n")
	final := ""
	for _, line := range lines {
		data := strings.Split(line, ",")
		if len(data) < 8 {
			panic("Invalid data format")
		}
		final += data[0] + "," + data[1] + "\n"
	}
	return final
}

func filterFunctionQuery3Transactions(input string) string {
	lines := strings.Split(input, "\n")
	final := ""
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		data := strings.Split(line, ",")
		if len(data) < 9 {
			panic("Invalid data format")
		}
		layout := "2006-01-02 15:04:05" // Go's reference layout
		t, _ := time.Parse(layout, data[8])
		amount, _ := strconv.ParseFloat(data[7], 64)
		amount = math.Round(amount*10) / 10
		if yearCondition(data) && t.Hour() >= 6 && t.Hour() <= 23 {
			final += data[1] + "," + strconv.FormatFloat(amount, 'f', 1, 64) + "," + data[8] + "\n"
		}
	}
	return final
}

func filterFunctionQuery4Transactions(input string) string {
	lines := strings.Split(input, "\n")
	final := ""
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		data := strings.Split(line, ",")
		if len(data) < 9 {
			panic("Invalid data format")
		}
		if yearCondition(data) {
			final += data[0] + "," + data[1] + "," + data[4] + "\n"
		}
	}
	return final
}

func filterFunctionQuery4UsersBirthdates(input string) string {
	lines := strings.Split(input, "\n")
	final := ""
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		data := strings.Split(line, ",")
		if len(data) < 4 {
			panic("Invalid data format")
		}
		final += data[0] + "," + data[2] + "\n"
	}
	return final
}

func filterTransactions(input string) []string {
	lines := strings.Split(input, "\n")
	final_query1 := ""
	final_query3 := ""
	final_query4 := ""
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		data := strings.Split(line, ",")
		if len(data) < 9 {
			panic("Invalid data format")
		}
		amount, _ := strconv.ParseFloat(data[7], 64)
		amount = math.Round(amount*10) / 10
		if yearCondition(data) {
			final_query1 += data[0] + "," + data[1] + "," + data[4] + "\n"                                 //mapeo query 1
			final_query3 += data[1] + "," + strconv.FormatFloat(amount, 'f', 1, 64) + "," + data[8] + "\n" //mapeo query 3
			final_query4 += data[0] + "," + data[1] + "," + data[4] + "\n"                                 //mapeo query 4
		}
	}
	return []string{final_query1, final_query3, final_query4}
}

type FilterMapper struct {
}

func (c *FilterMapper) Process(pkt packet.Packet, function string) []packet.Packet {
	input := pkt.GetPayload()
	function_name := strings.ToLower(function)

	var output []string
	switch function_name {
	case "transactions":
		output = filterTransactions(input)
	case "yearfilter":
		output = []string{filterByYearCommon(input)}
	case "query1yearandamount":
		output = []string{filterFunctionQuery1(input)}
	case "query2ayearandquantity":
		output = []string{filterFunctionQuery2a(input)}
	case "query2byearandsubtotal":
		output = []string{filterFunctionQuery2b(input)}
	case "filterstores":
		output = []string{mapStoreIdAndName(input)}
	case "query3mapstoreidandname":
		output = []string{mapStoreIdAndName(input)}
	case "query3transactions":
		output = []string{filterFunctionQuery3Transactions(input)}
	case "query4transactions":
		output = []string{filterFunctionQuery4Transactions(input)}
	case "query4usersbirthdates":
		output = []string{filterFunctionQuery4UsersBirthdates(input)}
	default:
		panic(fmt.Sprintf("Unknown function %s", function))
	}

	var new_packets []packet.Packet
	for _, result := range output {
		new_packets = append(new_packets, packet.ChangePayload(pkt, []string{result})[0])
	}

	return new_packets
}
