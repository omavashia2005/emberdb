package resp

import (
	"github.com/Fusl/go-resp/types"
)

// ParseCommand parses the next RESP command (a multi-bulk array of bulk strings,
// the form redis clients send) from buf. On success it returns the command's
// arguments and the number of bytes consumed from buf. If buf does not yet hold a
// complete command it returns (nil, 0, ErrIncomplete). On a protocol violation it
// returns a non-nil error that is not ErrIncomplete.
// The returned [][]byte point into buf (no copy); the caller must consume them
// before mutating/discarding buf.
func ParseCommand(buf []byte) (args [][]byte, consumed int, err error) {
	// ponytail: inline commands are unsupported for now (RESP arrays only).
	if len(buf) == 0 {
		return nil, 0, ErrIncomplete
	}
	if buf[0] != types.RespArray {
		return nil, 0, ErrProtoExpectedString
	}

	// Parse the multi-bulk header: *<n>\r\n
	line, rest, ok := readCRLFLine(buf, 1)
	if !ok {
		return nil, 0, ErrIncomplete
	}
	n32, perr := ParseInt32(line)
	n := int(n32)
	if perr != nil || n > MaxMultiBulkLength {
		return nil, 0, ErrProtoInvalidMultiBulkLength
	}
	if n <= 0 {
		// Empty / negative array: consume the header only.
		return [][]byte{}, len(buf) - len(rest), nil
	}

	p := len(buf) - len(rest)
	refs := make([][]byte, n)
	for i := 0; i < n; i++ {
		// Each element must be a bulk string: $<len>\r\n<bytes>\r\n
		if p >= len(buf) {
			return nil, 0, ErrIncomplete
		}
		if buf[p] != types.RespString {
			return nil, 0, ErrProtoExpectedString
		}
		line, rest, ok := readCRLFLine(buf, p+1)
		if !ok {
			return nil, 0, ErrIncomplete
		}
		l32, perr := ParseUInt32(line)
		l := int(l32)
		if perr != nil || l > MaxBulkLength {
			return nil, 0, ErrProtoInvalidBulkLength
		}
		p = len(buf) - len(rest)
		// Need l bytes of payload plus the trailing CRLF.
		if p+l+2 > len(buf) {
			return nil, 0, ErrIncomplete
		}
		refs[i] = buf[p : p+l]
		p += l + 2
	}
	return refs, p, nil
}

// readCRLFLine scans buf starting at off for a \r\n terminator. On success it
// returns the bytes between off and the \r (exclusive), the remainder of buf
// after the \n, and true. If no complete \r\n is present it returns ok=false.
func readCRLFLine(buf []byte, off int) (line []byte, rest []byte, ok bool) {
	for i := off; i+1 < len(buf); i++ {
		if buf[i] == '\r' && buf[i+1] == '\n' {
			return buf[off:i], buf[i+2:], true
		}
	}
	return nil, nil, false
}
