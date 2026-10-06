package kvstore

import (
	"sort"
)

func (kv *KVStore) HSet(key, field, value string) {
	kv.HMSet(key, map[string]string{field: value})
}

func (kv *KVStore) HMSet(key string, fields map[string]string) {
	if len(fields) == 0 {
		return
	}
	if kv.clusterEnabled {
		s := kv.getShard(key)
		s.mu.Lock()
		defer s.mu.Unlock()
		v, ok := s.m[key]
		if ok && v.Type != HashType {
			return
		}
		if !ok {
			v = Value{Type: HashType, Hash: make(map[string]string, len(fields))}
		}
		for f, val := range fields {
			v.Hash[f] = val
		}
		s.ensure()
		s.m[key] = v
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	hash := kv.Hashes[key]
	if hash == nil {
		hash = make(map[string]string, len(fields))
		kv.Hashes[key] = hash
	}

	for f, val := range fields {
		hash[f] = val
	}
}

func (kv *KVStore) HGet(key, field string) (string, bool) {
	if kv.clusterEnabled {
		s := kv.getShard(key)
		s.mu.RLock()
		defer s.mu.RUnlock()
		v, ok := s.m[key]
		if !ok || v.Type != HashType {
			return "", false
		}
		result, ok := v.Hash[field]
		return result, ok
	}
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	result, ok := kv.Hashes[key][field]
	return result, ok
}

func (kv *KVStore) HMGet(key string, fields ...string) []any {
	var hash map[string]string
	if kv.clusterEnabled {
		s := kv.getShard(key)
		s.mu.RLock()
		defer s.mu.RUnlock()
		if v, ok := s.m[key]; ok && v.Type == HashType {
			hash = v.Hash
		}
	} else {
		kv.mu.RLock()
		defer kv.mu.RUnlock()
		hash = kv.Hashes[key]
	}
	result := make([]any, len(fields))
	for i, field := range fields {
		if value, ok := hash[field]; ok {
			result[i] = value
		}
	}
	return result
}

func (kv *KVStore) HGetAll(key string) []string {
	var hash map[string]string
	if kv.clusterEnabled {
		s := kv.getShard(key)
		s.mu.RLock()
		defer s.mu.RUnlock()
		if v, ok := s.m[key]; ok && v.Type == HashType {
			hash = v.Hash
		}
	} else {
		kv.mu.RLock()
		defer kv.mu.RUnlock()
		hash = kv.Hashes[key]
	}
	fields := make([]string, 0, len(hash))
	for field := range hash {
		fields = append(fields, field)
	}
	sort.Strings(fields)
	result := make([]string, 0, len(hash)*2)
	for _, field := range fields {
		result = append(result, field, hash[field])
	}
	return result
}

func (kv *KVStore) HDel(key string, fields ...string) int {
	removeFields := func(hash map[string]string) int {
		removed := 0
		for _, field := range fields {
			if _, ok := hash[field]; ok {
				delete(hash, field)
				removed++
			}
		}
		return removed
	}
	if kv.clusterEnabled {
		s := kv.getShard(key)
		s.mu.Lock()
		defer s.mu.Unlock()
		v, ok := s.m[key]
		if !ok || v.Type != HashType {
			return 0
		}
		removed := removeFields(v.Hash)
		if len(v.Hash) == 0 {
			delete(s.m, key)
		}
		return removed
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	hash := kv.Hashes[key]
	removed := removeFields(hash)
	if hash != nil && len(hash) == 0 {
		delete(kv.Hashes, key)
	}
	return removed
}

