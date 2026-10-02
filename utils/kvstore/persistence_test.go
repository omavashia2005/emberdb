package kvstore

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	disk "github.com/omavashia2005/emberdb/utils/persistence"
)

func TestAOFUsesRESPCommands(t *testing.T) {
	requirePersistence(t)
	dir := t.TempDir()
	kv, err := OpenPersistent(dir)
	if err != nil {
		t.Fatal(err)
	}
	kv.Set("a\x00b", "x\r\ny")
	data, err := os.ReadFile(filepath.Join(dir, disk.AOFFilename))
	if err != nil {
		t.Fatal(err)
	}
	want := []byte("*3\r\n$3\r\nSET\r\n$3\r\na\x00b\r\n$4\r\nx\r\ny\r\n")
	if !bytes.Equal(data, want) {
		t.Fatalf("AOF command = %q, want %q", data, want)
	}
	if err := kv.Close(); err != nil {
		t.Fatal(err)
	}
	kv, err = OpenPersistent(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer kv.Close()
	if got := kv.Get("a\x00b"); got != "x\r\ny" {
		t.Fatalf("replayed value = %q", got)
	}
}

func TestAOFRejectsNonBulkArgument(t *testing.T) {
	requirePersistence(t)
	dir := t.TempDir()
	data := []byte("*3\r\n$3\r\nSET\r\n+key\r\n$5\r\nvalue\r\n")
	if err := os.WriteFile(filepath.Join(dir, disk.AOFFilename), data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenPersistent(dir); err == nil {
		t.Fatal("accepted a non-bulk AOF argument")
	}
}

func TestPersistenceRecoversAOFAndRDB(t *testing.T) {
	requirePersistence(t)
	dir := t.TempDir()
	kv, err := OpenPersistent(dir)
	if err != nil {
		t.Fatal(err)
	}
	kv.Set("string", "one\r\ntwo")
	kv.RPush("list", "a", "b")
	kv.HSet("hash", "field", "value")
	kv.SAdd("set", "b", "a")
	if _, err := kv.ZAdd("sorted", "2", "b", "1", "a"); err != nil {
		t.Fatal(err)
	}
	if err := kv.SaveRDB(); err != nil {
		t.Fatal(err)
	}
	if err := kv.Close(); err != nil {
		t.Fatal(err)
	}

	aofPath := filepath.Join(dir, disk.AOFFilename)
	info, err := os.Stat(aofPath)
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(aofPath, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString("*3\r\n$3\r\nSET\r\n$4\r\nhalf"); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	kv, err = OpenPersistent(dir)
	if err != nil {
		t.Fatal(err)
	}
	assertRecovered(t, kv)
	truncated, err := os.Stat(aofPath)
	if err != nil {
		t.Fatal(err)
	}
	if truncated.Size() != info.Size() {
		t.Fatalf("AOF size after truncated-tail recovery = %d, want %d", truncated.Size(), info.Size())
	}
	if err := kv.Close(); err != nil {
		t.Fatal(err)
	}

	if err := os.Remove(aofPath); err != nil {
		t.Fatal(err)
	}
	kv, err = OpenPersistent(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer kv.Close()
	assertRecovered(t, kv)
}

func TestAOFFailureSkipsMutation(t *testing.T) {
	requirePersistence(t)
	kv, err := OpenPersistent(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := kv.persistence.Close(); err != nil {
		t.Fatal(err)
	}

	kv.Set("unlogged", "value")
	if got := kv.Get("unlogged"); got != "(nil)" {
		t.Fatalf("SET changed memory after AOF failure: %q", got)
	}
	_ = kv.Close()
}

func TestPersistenceDisabled(t *testing.T) {
	t.Setenv("EMBERDB_PERSISTENCE", "0")
	for _, cluster := range []bool{false, true} {
		path := filepath.Join(t.TempDir(), "data")
		kv, err := OpenPersistent(path, cluster)
		if err != nil {
			t.Fatal(err)
		}
		if kv.PersistenceEnabled() {
			t.Fatal("persistence enabled with EMBERDB_PERSISTENCE=0")
		}
		kv.Set("key", "value")
		if got := kv.Get("key"); got != "value" {
			t.Fatalf("GET = %q", got)
		}
		if err := kv.SaveRDB(); err == nil {
			t.Fatal("SAVE succeeded with persistence disabled")
		}
		if err := kv.RewriteAOF(); err == nil {
			t.Fatal("BGREWRITEAOF succeeded with persistence disabled")
		}
		if err := kv.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("persistence directory exists: %v", err)
		}
	}
	dir := t.TempDir()
	aofPath := filepath.Join(dir, disk.AOFFilename)
	if err := os.WriteFile(aofPath, []byte("invalid AOF"), 0o600); err != nil {
		t.Fatal(err)
	}
	kv, err := OpenPersistent(dir)
	if err != nil {
		t.Fatalf("disabled persistence read existing AOF: %v", err)
	}
	kv.Set("key", "value")
	if err := kv.Close(); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(aofPath); err != nil || string(data) != "invalid AOF" {
		t.Fatalf("existing AOF changed: %q, %v", data, err)
	}
}

func requirePersistence(t *testing.T) {
	t.Helper()
	if os.Getenv("EMBERDB_PERSISTENCE") == "0" {
		t.Skip("persistence disabled")
	}
}

func assertRecovered(t *testing.T, kv *KVStore) {
	t.Helper()
	if got := kv.Get("string"); got != "one\r\ntwo" {
		t.Fatalf("GET string = %q", got)
	}
	if got := kv.LRange("list", 0, -1); !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Fatalf("LRANGE list = %#v", got)
	}
	if got, ok := kv.HGet("hash", "field"); !ok || got != "value" {
		t.Fatalf("HGET hash field = %q, %v", got, ok)
	}
	if got := kv.SMembers("set"); !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Fatalf("SMEMBERS set = %#v", got)
	}
	if got := kv.ZRange("sorted", 0, -1); !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Fatalf("ZRANGE sorted = %#v", got)
	}
}
