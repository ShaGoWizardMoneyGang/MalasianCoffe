package main

import (
	"malasian_coffe/packet"
	"malasian_coffe/system/concat"
	"malasian_coffe/system/filter_mapper"
	"testing"
)

// Test con 2 paquetes para la Query 1, utilizando Filter Mapper y concat
func TestQuery1(t *testing.T) {
	transactionsRaw := []string{"2ae6d188-76c2-4095-b861-ab97d3cd9312,4,5,,,38.0,0.0,38.0,2024-07-01 07:00:00\n" +
		"7d0a474d-62f4-442a-96b6-a5df2bda8832,7,1,,,33.0,0.0,33.0,2025-07-01 07:00:02\n" +
		"928498fd-edbf-456c-bbd5-31aa56dc96c9,8,1,,,14.0,0.0,14.0,2023-07-01 07:02:21\n" +
		"48968d91-dd5a-47f2-8646-42f8b587932f,3,1,,,30.0,0.0,30.0,2023-07-01 07:01:54"}
	pkt_empty := packet.Packet{}
	pkt1 := packet.ChangePayload(pkt_empty, transactionsRaw)
	transactionsRaw2 := []string{"2ae6d188-76c2-4095-b861-ab97d3cd9312,4,5,,,38.0,0.0,38.0,2024-07-01 07:00:00\n" +
		"7d0a474d-62f4-442a-96b6-a5df2bda8832,7,1,,,33.0,0.0,33.0,2025-07-01 07:00:02\n" +
		"928498fd-edbf-456c-bbd5-31aa56dc96c9,8,1,,,14.0,0.0,14.0,2023-07-01 07:02:21\n" +
		"48968d91-dd5a-47f2-8646-42f8b587932f,3,1,,,30.0,0.0,30.0,2023-07-01 07:01:54"}
	pkt_empty2 := packet.Packet{}
	pkt2 := packet.ChangePayload(pkt_empty2, transactionsRaw2)
	workerFilterMapper := filter_mapper.FilterMapper{}

	result1 := workerFilterMapper.Process(pkt1[0], "query1YearAndAmount")
	result2 := workerFilterMapper.Process(pkt2[0], "query1YearAndAmount")

	expected1 := "2ae6d188-76c2-4095-b861-ab97d3cd9312,38.0\n7d0a474d-62f4-442a-96b6-a5df2bda8832,33.0\n"
	if result1[0].GetPayload() != expected1 {
		t.Fatalf("unexpected result: got %+v, expected %+v", result1[0].GetPayload(), expected1)
	}
	expected2 := "2ae6d188-76c2-4095-b861-ab97d3cd9312,38.0\n7d0a474d-62f4-442a-96b6-a5df2bda8832,33.0\n"
	if result1[0].GetPayload() != expected2 {
		t.Fatalf("unexpected result: got %+v, expected %+v", result2[0].GetPayload(), expected2)
	}
	workerConcat := concat.Concat{}
	resultConcat := workerConcat.Process(result1[0])
	resultConcat = workerConcat.Process(result2[0])
	expectedConcat := "2ae6d188-76c2-4095-b861-ab97d3cd9312,38.0\n7d0a474d-62f4-442a-96b6-a5df2bda8832,33.0\n2ae6d188-76c2-4095-b861-ab97d3cd9312,38.0\n7d0a474d-62f4-442a-96b6-a5df2bda8832,33.0\n"
	if resultConcat[0].GetPayload() != expectedConcat {
		t.Fatalf("unexpected result: got %+v, expected %+v", result2[0].GetPayload(), expectedConcat)
	}
}
