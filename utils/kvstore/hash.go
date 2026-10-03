package kvstore

import "sort"

func (kv *KVStore) HSet(key, field, value string) {
	kv.HMSet(key, map[string]string{field: value})
}

func (kv *KVStore) HMSet(key string, fields map[string]string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	var hash map[string]string
	if kv.clusterEnabled {
		value, _ := kv.value(key, HashType)
		hash = value.Hash
	} else {
		hash = kv.Hashes[key]
	}
	if hash == nil {
		hash = make(map[string]string)
	}
	if len(fields) == 0 {
		return
	}
	for field, value := range fields {
		hash[field] = value
	}
	if kv.clusterEnabled {
		kv.setValue(key, Value{Type: HashType, Hash: hash})
	} else {
		kv.Hashes[key] = hash
	}
}

func (kv *KVStore) HGet(key, field string) (string, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	if kv.clusterEnabled {
		value, ok := kv.value(key, HashType)
		if !ok {
			return "", false
		}
		result, ok := value.Hash[field]
		return result, ok
	}
	result, ok := kv.Hashes[key][field]
	return result, ok
}

func (kv *KVStore) HMGet(key string, fields ...string) []any {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	var hash map[string]string
	if kv.clusterEnabled {
		value, _ := kv.value(key, HashType)
		hash = value.Hash
	} else {
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
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	var hash map[string]string
	if kv.clusterEnabled {
		value, _ := kv.value(key, HashType)
		hash = value.Hash
	} else {
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
	kv.mu.Lock()
	defer kv.mu.Unlock()
	var hash map[string]string
	if kv.clusterEnabled {
		value, _ := kv.value(key, HashType)
		hash = value.Hash
	} else {
		hash = kv.Hashes[key]
	}
	removed := 0
	for _, field := range fields {
		if _, ok := hash[field]; ok {
			delete(hash, field)
			removed++
		}
	}
	return removed
}
