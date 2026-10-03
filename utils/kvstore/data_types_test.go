package kvstore

import (
	"reflect"
	"testing"
)

func TestReferenceDataTypeCommands(t *testing.T) {
	kv := NewKVStore()
	if got := kv.LPush("list", "c", "b", "a"); got != 3 {
		t.Fatalf("LPush length = %d", got)
	}
	kv.RPush("list", "d", "e", "f")
	if got := kv.LRange("list", 0, -1); !reflect.DeepEqual(got, []string{"c", "b", "a", "d", "e", "f"}) {
		t.Fatalf("LRange = %v", got)
	}

	kv.HSet("hash", "one", "1")
	kv.HMSet("hash", map[string]string{"two": "2", "three": "3"})
	if got := kv.HMGet("hash", "one", "missing", "three"); !reflect.DeepEqual(got, []any{"1", nil, "3"}) {
		t.Fatalf("HMGet = %v", got)
	}

	if got := kv.SAdd("set", "a", "b", "a"); got != 2 || !kv.SIsMember("set", "b") {
		t.Fatalf("SAdd = %d, SIsMember = %v", got, kv.SIsMember("set", "b"))
	}

	if got, err := kv.ZAdd("sorted", "3", "c", "1", "a", "2", "b"); err != nil || got != 3 {
		t.Fatalf("ZAdd = %d, %v", got, err)
	}
	if got := kv.ZRange("sorted", 0, -1); !reflect.DeepEqual(got, []string{"a", "b", "c"}) {
		t.Fatalf("ZRange = %v", got)
	}
}
