package resp

import (
	"bytes"
	"errors"
	"testing"
)

func TestParseCommandByteByByte(t *testing.T) {
	full := []byte("*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")
	want := [][]byte{[]byte("SET"), []byte("foo"), []byte("bar")}

	for i := 1; i < len(full); i++ {
		args, consumed, err := ParseCommand(full[:i], nil)
		if !errors.Is(err, ErrIncomplete) {
			t.Fatalf("prefix len %d: got err=%v args=%v consumed=%d, want ErrIncomplete", i, err, args, consumed)
		}
	}

	args, consumed, err := ParseCommand(full, nil)
	if err != nil {
		t.Fatalf("full buffer: unexpected err: %v", err)
	}
	if consumed != len(full) {
		t.Fatalf("full buffer: consumed=%d, want %d", consumed, len(full))
	}
	if len(args) != len(want) {
		t.Fatalf("full buffer: got %d args, want %d", len(args), len(want))
	}
	for i := range want {
		if !bytes.Equal(args[i], want[i]) {
			t.Fatalf("arg %d: got %q, want %q", i, args[i], want[i])
		}
	}
}

func TestParseCommandTwoConcatenated(t *testing.T) {
	first := []byte("*1\r\n$4\r\nPING\r\n")
	second := []byte("*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n")
	buf := append(append([]byte{}, first...), second...)

	args, consumed, err := ParseCommand(buf, nil)
	if err != nil {
		t.Fatalf("first parse: unexpected err: %v", err)
	}
	if consumed != len(first) {
		t.Fatalf("first parse: consumed=%d, want %d (start of second command)", consumed, len(first))
	}
	if len(args) != 1 || !bytes.Equal(args[0], []byte("PING")) {
		t.Fatalf("first parse: got args=%v, want [PING]", args)
	}

	args2, consumed2, err := ParseCommand(buf[consumed:], nil)
	if err != nil {
		t.Fatalf("second parse: unexpected err: %v", err)
	}
	if consumed2 != len(second) {
		t.Fatalf("second parse: consumed=%d, want %d", consumed2, len(second))
	}
	if len(args2) != 2 || !bytes.Equal(args2[0], []byte("GET")) || !bytes.Equal(args2[1], []byte("foo")) {
		t.Fatalf("second parse: got args=%v, want [GET foo]", args2)
	}
}
