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
	ValueBool Values = 2
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

// ================================ Boolean ===================================

func SerializeBool(b bool) ([]byte) {
	length := 1

	buffer_len := length + 2

	buffer := make([]byte, buffer_len)

	// 2 additional bytes
	buffer[0] = byte(ValueBool)
	buffer[1] = byte(length)

	// Boolean in byte
	var b_b byte
	if b {
		b_b = byte(0)
	} else {
		b_b = byte(1)
	}
	buffer[2] = byte(b_b)

	return buffer
}

func DeserializeBool(data *[]byte) (bool, error) {
	reader := bytes.NewReader((*data))

	var boolean_indicator [1]byte;
	reader.Read(boolean_indicator[:])
	if boolean_indicator[0] != byte(ValueBool) {
		return false, errors.New("Tried to deserialize bool when not uint64")
	}

	var boolean_length [1]byte;
	reader.Read(boolean_length[:])
	if uint8(boolean_length[0]) != 1 {
		return false, errors.New("Boolean not length 1")
	}

	var boolean_b [1]byte;
	reader.Read(boolean_b[:])
	var boolean bool
	if boolean_b[0] == byte(0) {
		boolean = true
	} else {
		boolean = false
	}

	return boolean, nil
}
