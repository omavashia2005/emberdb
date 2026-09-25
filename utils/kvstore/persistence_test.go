package kvstore

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestPersistenceRecoversAOFAndRDB(t *testing.T) {
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

	aofPath := filepath.Join(dir, aofFilename)
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
	kv, err := OpenPersistent(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	kv.persistence.mu.Lock()
	if err := kv.persistence.aof.Close(); err != nil {
		t.Fatal(err)
	}
	kv.persistence.mu.Unlock()

	kv.Set("unlogged", "value")
	if got := kv.Get("unlogged"); got != "(nil)" {
		t.Fatalf("SET changed memory after AOF failure: %q", got)
	}
	_ = kv.Close()
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
