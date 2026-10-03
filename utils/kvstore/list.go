package kvstore

func (kv *KVStore) LPush(key string, values ...string) int {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	var list []string
	if kv.clusterEnabled {
		value, _ := kv.value(key, ListType)
		list = value.List
	} else {
		list = kv.Lists[key]
	}
	if len(values) == 0 {
		return len(list)
	}
	for i := len(values) - 1; i >= 0; i-- {
		list = append([]string{values[i]}, list...)
	}
	if kv.clusterEnabled {
		kv.setValue(key, Value{Type: ListType, List: list})
	} else {
		kv.Lists[key] = list
	}
	return len(list)
}

func (kv *KVStore) LPop(key string) (string, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	var list []string
	if kv.clusterEnabled {
		value, _ := kv.value(key, ListType)
		list = value.List
	} else {
		list = kv.Lists[key]
	}
	if len(list) == 0 {
		return "", false
	}
	result := list[0]
	list = list[1:]
	if kv.clusterEnabled {
		kv.setValue(key, Value{Type: ListType, List: list})
	} else {
		kv.Lists[key] = list
	}
	return result, true
}

func (kv *KVStore) RPush(key string, values ...string) int {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	var list []string
	if kv.clusterEnabled {
		value, _ := kv.value(key, ListType)
		list = value.List
	} else {
		list = kv.Lists[key]
	}
	if len(values) == 0 {
		return len(list)
	}
	list = append(list, values...)
	if kv.clusterEnabled {
		kv.setValue(key, Value{Type: ListType, List: list})
	} else {
		kv.Lists[key] = list
	}
	return len(list)
}

func (kv *KVStore) RPop(key string) (string, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	var list []string
	if kv.clusterEnabled {
		value, _ := kv.value(key, ListType)
		list = value.List
	} else {
		list = kv.Lists[key]
	}
	if len(list) == 0 {
		return "", false
	}
	last := len(list) - 1
	result := list[last]
	list = list[:last]
	if kv.clusterEnabled {
		kv.setValue(key, Value{Type: ListType, List: list})
	} else {
		kv.Lists[key] = list
	}
	return result, true
}

func (kv *KVStore) LRange(key string, start, end int) []string {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	var list []string
	if kv.clusterEnabled {
		value, _ := kv.value(key, ListType)
		list = value.List
	} else {
		list = kv.Lists[key]
	}
	if start < 0 {
		start += len(list)
	}
	if end < 0 {
		end += len(list)
	}
	if start < 0 {
		start = 0
	}
	if end >= len(list) {
		end = len(list) - 1
	}
	if start > end || start >= len(list) || end < 0 {
		return []string{}
	}
	return append([]string(nil), list[start:end+1]...)
}

func (kv *KVStore) LLen(key string) int {
	return len(kv.LRange(key, 0, -1))
}
