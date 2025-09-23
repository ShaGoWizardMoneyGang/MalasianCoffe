package filter_mapper

import (
	"malasian_coffe/packet"
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
			if t.Year() >= 2024 && t.Year() <= 2025 {
				final += data[0] + "," + data[7] + "\n"
			}
		}
		return final
	}
	Option2 = "Option2"
	Option3 = "Option3"
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
