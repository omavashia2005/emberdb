package kvstore_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/omavashia2005/emberdb/utils/kvstore"
)

func TestReferenceDataTypeCommands(t *testing.T) {
	for _, clustered := range []bool{false, true} {
		t.Run(fmt.Sprintf("clustered=%t", clustered), func(t *testing.T) {
			kv := kvstore.NewKVStore(clustered)
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

			if got := kv.HDel("hash", "one", "one", "missing"); got != 1 {
				t.Fatalf("HDel duplicate count = %d", got)
			}
			if got := kv.HDel("hash"); got != 0 {
				t.Fatalf("empty HDel count = %d", got)
			}
			if got := kv.SRem("set", "a", "a", "missing"); got != 1 {
				t.Fatalf("SRem duplicate count = %d", got)
			}
			if got := kv.SRem("set"); got != 0 {
				t.Fatalf("empty SRem count = %d", got)
			}
			if got := kv.SAdd("set", "b"); got != 0 {
				t.Fatalf("duplicate SAdd count = %d", got)
			}
			if got := kv.SAdd("set"); got != 0 {
				t.Fatalf("empty SAdd count = %d", got)
			}
		})
	}
}
