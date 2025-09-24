package filter_mapper

import (
	"malasian_coffe/packet"
	"math"
	"strconv"
	"strings"
	"time"
)

type filterMapperOptions func(string) string

var (
	filterFunction1 filterMapperOptions = func(input string) string {
		lines := strings.Split(input, "\n")
		final := ""
		for _, line := range lines {
			data := strings.Split(line, ",")
			if len(data) < 9 {
				panic("Invalid data format")
			}
			layout := "2006-01-02 15:04:05" // Go's reference layout
			t, _ := time.Parse(layout, data[8])
			amount, _ := strconv.ParseFloat(data[7], 64)
			amount = math.Round(amount*10) / 10
			if t.Year() >= 2024 && t.Year() <= 2025 && amount >= 15.0 {
				final += data[0] + "," + strconv.FormatFloat(amount, 'f', 1, 64) + "\n"
			}
		}
		return final
	}
	filterFunction2a = func(input string) string {
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
	filterFunction2b = func(input string) string {
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
	filterFunction3Store = func(input string) string {
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
	Option4 = "Option4"
)

type FilterMapper struct {
	Function filterMapperOptions
}

func (fm *FilterMapper) Process(pkt packet.Packet) packet.Packet {
	returned := packet.Packet{Payload: []byte("")}
	if fm.Function != nil {
		result := fm.Function(string(pkt.Payload))
		returned.Payload = []byte(result)
	}
	return returned
}
