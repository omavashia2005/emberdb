package kvstore

import (
	"fmt"
	"strconv"
)

func (kv *KVStore) Set(key, value string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.clusterEnabled {
		kv.setValue(key, Value{Type: StringType, String: value})
		return
	}
	kv.Strings[key] = value
}

func (kv *KVStore) Get(key string) string {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	if kv.clusterEnabled {
		if value, ok := kv.value(key, StringType); ok {
			return value.String
		}
		return "(nil)"
	}
	if value, ok := kv.Strings[key]; ok {
		return value
	}
	return "(nil)"
}

func (kv *KVStore) Append(key, suffix string) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.clusterEnabled {
		value, _ := kv.value(key, StringType)
		kv.setValue(key, Value{Type: StringType, String: value.String + suffix})
		return nil
	}
	kv.Strings[key] += suffix
	return nil
}

func (kv *KVStore) changeInteger(key string, delta int) error {
	var current string
	var exists bool
	if kv.clusterEnabled {
		value, ok := kv.value(key, StringType)
		exists = ok
		if exists {
			current = value.String
		}
	} else {
		current, exists = kv.Strings[key]
	}
	n := 0
	var err error
	if exists {
		n, err = strconv.Atoi(current)
		if err != nil {
			return fmt.Errorf("ERR value is not an integer")
		}
	}
	current = strconv.Itoa(n + delta)
	if kv.clusterEnabled {
		kv.setValue(key, Value{Type: StringType, String: current})
	} else {
		kv.Strings[key] = current
	}
	return nil
}

func (kv *KVStore) Incr(key string) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.changeInteger(key, 1)
}

func (kv *KVStore) IncrBy(key, increment string) error {
	delta, err := strconv.Atoi(increment)
	if err != nil {
		return fmt.Errorf("ERR INCRBY val is not an integer")
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.changeInteger(key, delta)
}

func (kv *KVStore) Decr(key string) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.changeInteger(key, -1)
}

func (kv *KVStore) DecrBy(key, decrement string) error {
	delta, err := strconv.Atoi(decrement)
	if err != nil {
		return fmt.Errorf("ERR DECRBY val is not an integer")
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.changeInteger(key, -delta)
}
