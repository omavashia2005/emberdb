package doublebuffer

import (
	"bytes"
	"io"
	"testing"
)

func TestDoubleBufferSmallWrites(t *testing.T) {
	buf := &bytes.Buffer{}
	db := NewWriterSize(buf, 1024)

	data := []byte("Hello, World!")
	n, err := db.Write(data)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if n != len(data) {
		t.Fatalf("Write returned wrong length: got %d, want %d", n, len(data))
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	if got := buf.String(); got != string(data) {
		t.Fatalf("Wrong data written: got %q, want %q", got, string(data))
	}
}

func TestDoubleBufferLargeWrites(t *testing.T) {
	buf := &bytes.Buffer{}
	db := NewWriterSize(buf, 16)

	data := bytes.Repeat([]byte("abcdefghijklmnop"), 4)
	n, err := db.Write(data)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if n != len(data) {
		t.Fatalf("Write returned wrong length: got %d, want %d", n, len(data))
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	if got := buf.String(); got != string(data) {
		t.Fatalf("Wrong data written: got %q, want %q", got, string(data))
	}
}

func TestDoubleBufferReset(t *testing.T) {
	buf1 := &bytes.Buffer{}
	db := NewWriterSize(buf1, 1024)

	data1 := []byte("First write")
	_, err := db.Write(data1)
	if err != nil {
		t.Fatalf("First write failed: %v", err)
	}

	buf2 := &bytes.Buffer{}
	db.Reset(buf2)

	data2 := []byte("Second write")
	_, err = db.Write(data2)
	if err != nil {
		t.Fatalf("Second write failed: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	if got := buf1.String(); got != string(data1) {
		t.Fatalf("Wrong data in first buffer: got %q, want %q", got, string(data1))
	}
	if got := buf2.String(); got != string(data2) {
		t.Fatalf("Wrong data in second buffer: got %q, want %q", got, string(data2))
	}
}

func TestDoubleBufferClose(t *testing.T) {
	db := NewWriterSize(io.Discard, 1024)

	data := []byte("Test data")
	_, err := db.Write(data)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Write after close should fail
	_, err = db.Write([]byte("More data"))
	if err == nil {
		t.Fatal("Write after close succeeded, want error")
	}
}

func TestWriteStats(t *testing.T) {
	t.Setenv("EMBERDB_RESP_WRITE_PROFILE", "1")
	before := Snapshot()
	db := NewWriterSize(io.Discard, 16)
	if n, err := db.Write(bytes.Repeat([]byte("x"), 64)); n != 64 || err != nil {
		t.Fatalf("Write = %d, %v", n, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	after := Snapshot()
	if after.LogicalWrites-before.LogicalWrites != 1 || after.LogicalBytes-before.LogicalBytes != 64 ||
		after.Flushes-before.Flushes != 4 || after.FlushBytes-before.FlushBytes != 64 ||
		after.SocketWrites-before.SocketWrites != 4 || after.SocketBytes-before.SocketBytes != 64 ||
		after.Wakeups-before.Wakeups < 4 {
		t.Fatalf("unexpected stats delta: before=%+v after=%+v", before, after)
	}
}

func BenchmarkDoubleBufferSmallWrites(b *testing.B) {
	db := NewWriterSize(io.Discard, 1024)
	defer db.Close()

	data := []byte("Hello, World!")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		db.Write(data)
	}
}

func BenchmarkDoubleBufferLargeWrites(b *testing.B) {
	db := NewWriterSize(io.Discard, 1024)
	defer db.Close()

	data := bytes.Repeat([]byte("abcdefghijklmnop"), 64)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		db.Write(data)
	}
}
