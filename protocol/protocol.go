package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
)

type Values uint

// Using the Value* prefix to avoid collissions
const (
	ValueString Values = 0
	ValueUInteger64 Values = 1
	ValueByte Values = 2
)

// ================================= String ====================================
func SerializeString(str string) ([]byte) {
	bytes := []byte(str)
	length := len(bytes)

	buffer_len := length + 1 + 8
	buffer := make([]byte, buffer_len)

	// Header
	buffer[0] = byte(ValueString)
	binary.BigEndian.PutUint64(buffer[1:9], uint64(length))

	for i := 0; i < length; i++ {
		current_byte := bytes[i]
		buffer[i + 9] = current_byte
	}

	return bytes
}
func DeserializeString(data *[]byte) (string, error) {
	reader := bytes.NewReader((*data))

	var string_indicator [1]byte;
	reader.Read(string_indicator[:])
	if string_indicator[0] != byte(ValueString) {
		return "", errors.New("Tried to deserialize uint64 when not uint64")
	}

	var string_length [8]byte;
	reader.Read(string_length[:])
	length := binary.BigEndian.Uint64(string_length[:])

	string_buffer := make([]byte, length)
	reader.Read(string_buffer[:])
	string_s := string(string_buffer)

	return string_s, nil
}

// ============================ Unsigned integer ===============================

func SerializeUInteger64(i uint64) ([]byte) {
	length := 8

	buffer_len := length + 2

	buffer := make([]byte, buffer_len)

	// 2 additional bytes
	buffer[0] = byte(ValueUInteger64)
	buffer[1] = byte(length)

	binary.BigEndian.PutUint64(buffer[2:buffer_len], i)

	return buffer
}

func DeserializeUInteger64(data *[]byte) (uint64, error) {
	reader := bytes.NewReader((*data))

	var integer_indicator [1]byte;
	reader.Read(integer_indicator[:])
	if integer_indicator[0] != byte(ValueUInteger64) {
		return 0, errors.New("Tried to deserialize uint64 when not uint64")
	}

	var integer_length [1]byte;
	reader.Read(integer_length[:])
	if uint8(integer_length[0]) != 8 {
		return 0, errors.New("Integer not length 8")
	}

	var integer_b [8]byte;
	reader.Read(integer_b[:])
	integer := binary.BigEndian.Uint64(integer_b[:])

	return integer, nil
}
