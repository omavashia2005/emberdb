package kvstore

import (
	"sync"
)

type shard struct {
	mu sync.RWMutex
	m  map[string]Value
}

func (s *shard) ensure() {
	if s.m == nil {
		s.m = make(map[string]Value)
	}
}

func (kv *KVStore) getShard(key string) *shard {
	return &kv.slots[SlotForKey(key)]
}
