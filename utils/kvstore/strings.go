package kvstore

import (
	"fmt"
	"strconv"
)

func (kv *KVStore) Set(key, value string) {
	if kv.clusterEnabled {
		s := kv.getShard(key)
		s.mu.Lock()
		s.ensure()
		s.m[key] = Value{Type: StringType, String: value}
		s.mu.Unlock()
		return
	}
	kv.mu.Lock()
	kv.Strings[key] = value
	kv.mu.Unlock()
}

func (kv *KVStore) Mset(keys, values []string) {
	if len(keys) == 0 {
		return
	}
	if kv.clusterEnabled {
		s := &kv.slots[SlotForKey(keys[0])]
		s.mu.Lock()
		s.ensure()
		for i, k := range keys {
			s.m[k] = Value{Type: StringType, String: values[i]}
		}
		s.mu.Unlock()
		return
	}

	kv.mu.Lock()
	for i, k := range keys {
		kv.Strings[k] = values[i]
	}
	kv.mu.Unlock()
}

func (kv *KVStore) Get(key string) string {

	if kv.clusterEnabled {
		s := kv.getShard(key)
		s.mu.RLock()
		v, ok := s.m[key]
		s.mu.RUnlock()
		if !ok || v.Type != StringType {
			return "(nil)"
		}
		return v.String
	}

	kv.mu.RLock()
	value, ok := kv.Strings[key]
	kv.mu.RUnlock()

	if !ok {
		return "(nil)"
	}
	return value
}

func (kv *KVStore) Mget(keys []string) []string {
	if len(keys) == 0 {
		return nil
	}
	result := make([]string, len(keys))
	if kv.clusterEnabled {
		get := func(s *shard, k string) string {
			if v, ok := s.m[k]; ok && v.Type == StringType {
				return v.String
			}
			return "(nil)"
		}

		s := &kv.slots[SlotForKey(keys[0])]
		s.mu.RLock()
		for i, k := range keys {
			result[i] = get(s, k)
		}
		s.mu.RUnlock()
		return result
	}

	kv.mu.RLock()
	for i, k := range keys {
		if v, ok := kv.Strings[k]; ok {
			result[i] = v
		} else {
			result[i] = "(nil)"
		}
	}
	kv.mu.RUnlock()
	return result

}


func (kv *KVStore) Append(key, suffix string) error {
	if kv.clusterEnabled {
		s := kv.getShard(key)
		s.mu.Lock()
		defer s.mu.Unlock()
		v, ok := s.m[key]
		if ok && v.Type != StringType {
			return ErrWrongType
		}
		s.ensure()
		s.m[key] = Value{Type: StringType, String: v.String + suffix}
		return nil
	}
	kv.mu.Lock()
	kv.Strings[key] += suffix
	kv.mu.Unlock()
	return nil
}

func (kv *KVStore) changeInteger(key string, delta int) error {
	if kv.clusterEnabled {
		s := kv.getShard(key)
		s.mu.Lock()
		defer s.mu.Unlock()
		v, exists := s.m[key]
		if exists && v.Type != StringType {
			return ErrWrongType
		}
		next, err := applyDelta(v.String, exists, delta)
		if err != nil {
			return err
		}
		s.ensure()
		s.m[key] = Value{Type: StringType, String: next}
		return nil
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	current, exists := kv.Strings[key]
	next, err := applyDelta(current, exists, delta)
	if err != nil {
		return err
	}
	kv.Strings[key] = next
	return nil
}

func (kv *KVStore) Incr(key string) error {
	return kv.changeInteger(key, 1)
}

func (kv *KVStore) IncrBy(key, increment string) error {
	delta, err := strconv.Atoi(increment)
	if err != nil {
		return fmt.Errorf("ERR INCRBY val is not an integer")
	}
	return kv.changeInteger(key, delta)
}

func (kv *KVStore) Decr(key string) error {
	return kv.changeInteger(key, -1)
}

func (kv *KVStore) DecrBy(key, decrement string) error {
	delta, err := strconv.Atoi(decrement)
	if err != nil {
		return fmt.Errorf("ERR DECRBY val is not an integer")
	}
	return kv.changeInteger(key, -delta)
}
