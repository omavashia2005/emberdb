package kvstore

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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

var ErrWrongType = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")

type SortedSetMember struct {
	Member string
	Score  float64
}

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
	CommandsProcessed atomic.Int64
	slots             [16384]shard
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

func (kv *KVStore) Delete(key string) int {
	if kv.clusterEnabled {
		s := kv.getShard(key)
		s.mu.Lock()
		_, ok := s.m[key]
		if ok {
			delete(s.m, key)
		}
		s.mu.Unlock()
		if ok {
			return 1
		}
		return 0
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, stringFound := kv.Strings[key]
	_, listFound := kv.Lists[key]
	_, hashFound := kv.Hashes[key]
	_, setFound := kv.Sets[key]
	_, sortedSetFound := kv.SortedSets[key]
	if !stringFound && !listFound && !hashFound && !setFound && !sortedSetFound {
		return 0
	}
	delete(kv.Strings, key)
	delete(kv.Lists, key)
	delete(kv.Hashes, key)
	delete(kv.Sets, key)
	delete(kv.SortedSets, key)
	return 1
}

// applyDelta parses current (empty when the key is absent) and adds delta.
func applyDelta(current string, exists bool, delta int) (string, error) {
	n := 0
	if exists {
		var err error
		if n, err = strconv.Atoi(current); err != nil {
			return "", fmt.Errorf("ERR value is not an integer")
		}
	}
	return strconv.Itoa(n + delta), nil
}


func (kv *KVStore) FlushAll() {
	if kv.clusterEnabled {
		for i := range kv.slots {
			s := &kv.slots[i]
			s.mu.Lock()
			s.m = nil
			s.mu.Unlock()
		}
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.Strings = make(map[string]string)
	kv.Lists = make(map[string][]string)
	kv.Hashes = make(map[string]map[string]string)
	kv.Sets = make(map[string]map[string]struct{})
	kv.SortedSets = make(map[string][]SortedSetMember)
	kv.Expirations = make(map[string]time.Time)
}

func (kv *KVStore) GetKeysInSlot(slot uint64, count int) []string {
	if slot >= uint64(len(kv.slots)) || count <= 0 {
		return []string{}
	}
	s := &kv.slots[slot]
	s.mu.RLock()
	defer s.mu.RUnlock()

	keys := make([]string, 0, min(count, len(s.m)))
	for k := range s.m {
		if len(keys) == count {
			break
		}
		keys = append(keys, k)
	}
	return keys
}
