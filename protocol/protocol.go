package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

type Values uint

// Using the Value* prefix to avoid collissions
const (
	ValueString Values = 0
	ValueUInteger64 Values = 1
	ValueBool Values = 2
)

func typeName(indicator Values) string {
	var type_of string
	switch indicator {
	case ValueString:
		type_of = "string"
	case ValueUInteger64:
		type_of = "uint64"
	case ValueBool:
		type_of = "bool"
	}
	return type_of
}

// ================================= String ====================================
func SerializeString(str string) ([]byte) {
	bytes := []byte(str)
	length := len(bytes)

	buffer_len := length + 1 + 8
	buffer := make([]byte, buffer_len)

	// Header
	buffer[0] = byte(ValueString)
	binary.BigEndian.PutUint64(buffer[1:9], uint64(length))

	for i := range length {
		current_byte := bytes[i]
		buffer[i + 9] = current_byte
	}

	return buffer
}
func DeserializeString(reader *bytes.Reader) (string, error) {
	var string_indicator [1]byte;
	reader.Read(string_indicator[:])
	if string_indicator[0] != byte(ValueString) {
		return "", fmt.Errorf("Tried to deserialize string it is %s", typeName(Values(string_indicator[0])))
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

func DeserializeUInteger64(reader *bytes.Reader) (uint64, error) {
	var integer_indicator [1]byte;
	reader.Read(integer_indicator[:])
	if integer_indicator[0] != byte(ValueUInteger64) {
		return 0, errors.New("Tried to deserialize uint64 when not uint64. It is a {}")
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

func DeserializeBool(reader *bytes.Reader) (bool, error) {
	var boolean_indicator [1]byte;
	reader.Read(boolean_indicator[:])
	if boolean_indicator[0] != byte(ValueBool) {
		return false, errors.New(fmt.Sprintf("Tried to deserialize bool when not uint64. Expected: %x, got %x", ValueBool, boolean_indicator[0]))
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
