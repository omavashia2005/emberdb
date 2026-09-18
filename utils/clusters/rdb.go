package clusters

import (
	"encoding/binary"
	"fmt"
)

const DUMP_TYPE_STRING byte = 0x00

// Format:
// [1 byte type][4 bytes value length][N bytes value]
func EncodeBinaryDump(value string) string {
	buf := make([]byte, 1+4+len(value))

	buf[0] = DUMP_TYPE_STRING

	binary.BigEndian.PutUint32(
		buf[1:5],
		uint32(len(value)),
	)

	copy(buf[5:], value)

	return string(buf)
}

func RestoreDataFromBinaryDump(dump string) (string, error) {
	data := []byte(dump)

	if len(data) < 5 {
		return "", fmt.Errorf("invalid dump")
	}

	if data[0] != DUMP_TYPE_STRING {
		return "", fmt.Errorf("unsupported dump type")
	}

	length := binary.BigEndian.Uint32(data[1:5])

	if len(data) != 5+int(length) {
		return "", fmt.Errorf("invalid dump length")
	}

	return string(data[5:]), nil
}
