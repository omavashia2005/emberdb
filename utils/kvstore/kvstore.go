package kvstore

import (
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/lobaro/crc16"
)

type DataType uint8

const (
	StringType DataType = iota
	ListType
	HashType
	SetType
	SortedSetType
)

type SortedSetMember struct {
	Member string
	Score  float64
}

// Value is the valid-data-type representation used by clustered slots.
type Value struct {
	Type      DataType
	String    string
	List      []string
	Hash      map[string]string
	Set       map[string]struct{}
	SortedSet []SortedSetMember
}

type KVStore struct {
	Strings           map[string]string
	Lists             map[string][]string
	Hashes            map[string]map[string]string
	Sets              map[string]map[string]struct{}
	SortedSets        map[string][]SortedSetMember
	Expirations       map[string]time.Time
	mu                sync.RWMutex
	CommandsProcessed int
	SlotKeys          [16384]map[string]Value
	clusterEnabled    bool
}

func NewKVStore(clusterEnabled ...bool) *KVStore {
	kv := &KVStore{
		Strings:     make(map[string]string),
		Lists:       make(map[string][]string),
		Hashes:      make(map[string]map[string]string),
		Sets:        make(map[string]map[string]struct{}),
		SortedSets:  make(map[string][]SortedSetMember),
		Expirations: make(map[string]time.Time),
	}
	if len(clusterEnabled) > 0 {
		kv.clusterEnabled = clusterEnabled[0]
	}
	return kv
}

func SlotForKey(key string) uint16 {
	start := strings.IndexByte(key, '{')
	if start >= 0 {
		if end := strings.IndexByte(key[start+1:], '}'); end > 0 {
			key = key[start+1 : start+1+end]
		}
	}
	return crc16.ChecksumXModem([]byte(key)) % 16384
}

func (kv *KVStore) value(key string, dataType DataType) (Value, bool) {
	value, ok := kv.SlotKeys[SlotForKey(key)][key]
	return value, ok && value.Type == dataType
}

func (kv *KVStore) setValue(key string, value Value) {
	slot := SlotForKey(key)
	if kv.SlotKeys[slot] == nil {
		kv.SlotKeys[slot] = make(map[string]Value)
	}
	kv.SlotKeys[slot][key] = value
}

func (kv *KVStore) Delete(key string) int {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.clusterEnabled {
		slot := SlotForKey(key)
		if _, ok := kv.SlotKeys[slot][key]; !ok {
			return 0
		}
		delete(kv.SlotKeys[slot], key)
		return 1
	}
	_, stringFound := kv.Strings[key]
	_, listFound := kv.Lists[key]
	_, hashFound := kv.Hashes[key]
	_, setFound := kv.Sets[key]
	_, sortedSetFound := kv.SortedSets[key]
	if !stringFound && !listFound && !hashFound && !setFound && !sortedSetFound {
		return 0
	}
	if stringFound {
		delete(kv.Strings, key)
	}
	if listFound {
		delete(kv.Lists, key)
	}
	if hashFound {
		delete(kv.Hashes, key)
	}
	if setFound {
		delete(kv.Sets, key)
	}
	if sortedSetFound {
		delete(kv.SortedSets, key)
	}
	return 1
}

func (kv *KVStore) FlushAll() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.clusterEnabled {
		kv.SlotKeys = [16384]map[string]Value{}
		return
	}
	kv.Strings = make(map[string]string)
	kv.Lists = make(map[string][]string)
	kv.Hashes = make(map[string]map[string]string)
	kv.Sets = make(map[string]map[string]struct{})
	kv.SortedSets = make(map[string][]SortedSetMember)
	kv.Expirations = make(map[string]time.Time)
}

func (kv *KVStore) GetKeysInSlot(slot uint64, count int) []string {
	if slot >= uint64(len(kv.SlotKeys)) || count <= 0 {
		return []string{}
	}
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	keys := make([]string, 0, count)
	for key := range kv.SlotKeys[slot] {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if len(keys) > count {
		keys = keys[:count]
	}
	return keys
}
