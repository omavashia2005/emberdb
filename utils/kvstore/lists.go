package kvstore

func prepend(list, values []string) []string {
	out := make([]string, 0, len(values)+len(list))
	out = append(out, values...)
	return append(out, list...)
}

func (kv *KVStore) LPush(key string, values ...string) int {
	if kv.clusterEnabled {
		s := kv.getShard(key)
		s.mu.Lock()
		defer s.mu.Unlock()
		v, ok := s.m[key]
		if ok && v.Type != ListType {
			return 0
		}
		if len(values) == 0 {
			return len(v.List)
		}
		s.ensure()
		s.m[key] = Value{Type: ListType, List: prepend(v.List, values)}
		return len(v.List) + len(values)
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	list := kv.Lists[key]
	if len(values) == 0 {
		return len(list)
	}
	kv.Lists[key] = prepend(list, values)
	return len(list) + len(values)
}

func (kv *KVStore) RPush(key string, values ...string) int {
	if kv.clusterEnabled {
		s := kv.getShard(key)
		s.mu.Lock()
		defer s.mu.Unlock()
		v, ok := s.m[key]
		if ok && v.Type != ListType {
			return 0
		}
		if len(values) == 0 {
			return len(v.List)
		}
		s.ensure()
		s.m[key] = Value{Type: ListType, List: append(v.List, values...)}
		return len(v.List) + len(values)
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	list := kv.Lists[key]
	if len(values) == 0 {
		return len(list)
	}
	list = append(list, values...)
	kv.Lists[key] = list
	return len(list)
}

func (kv *KVStore) LPop(key string) (string, bool) {
	if kv.clusterEnabled {
		s := kv.getShard(key)
		s.mu.Lock()
		defer s.mu.Unlock()
		v, ok := s.m[key]
		if !ok || v.Type != ListType || len(v.List) == 0 {
			return "", false
		}
		result := v.List[0]
		if len(v.List) == 1 {
			delete(s.m, key)
		} else {
			v.List = v.List[1:]
			s.m[key] = v
		}
		return result, true
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	list := kv.Lists[key]
	if len(list) == 0 {
		return "", false
	}
	result := list[0]
	if len(list) == 1 {
		delete(kv.Lists, key)
	} else {
		kv.Lists[key] = list[1:]
	}
	return result, true
}

func (kv *KVStore) RPop(key string) (string, bool) {
	if kv.clusterEnabled {
		s := kv.getShard(key)
		s.mu.Lock()
		defer s.mu.Unlock()
		v, ok := s.m[key]
		if !ok || v.Type != ListType || len(v.List) == 0 {
			return "", false
		}
		last := len(v.List) - 1
		result := v.List[last]
		if last == 0 {
			delete(s.m, key)
		} else {
			v.List = v.List[:last]
			s.m[key] = v
		}
		return result, true
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	list := kv.Lists[key]
	if len(list) == 0 {
		return "", false
	}
	last := len(list) - 1
	result := list[last]
	if last == 0 {
		delete(kv.Lists, key)
	} else {
		kv.Lists[key] = list[:last]
	}
	return result, true
}

func rangeBounds(n, start, end int) (int, int, bool) {
	if start < 0 {
		start += n
	}
	if end < 0 {
		end += n
	}
	if start < 0 {
		start = 0
	}
	if end >= n {
		end = n - 1
	}
	if start > end || start >= n || end < 0 {
		return 0, 0, false
	}
	return start, end, true
}
func (kv *KVStore) LRange(key string, start, end int) []string {
	var list []string
	if kv.clusterEnabled {
		s := kv.getShard(key)
		s.mu.RLock()
		defer s.mu.RUnlock()
		if v, ok := s.m[key]; ok && v.Type == ListType {
			list = v.List
		}
	} else {
		kv.mu.RLock()
		defer kv.mu.RUnlock()
		list = kv.Lists[key]
	}
	start, end, ok := rangeBounds(len(list), start, end)
	if !ok {
		return []string{}
	}
	return append([]string(nil), list[start:end+1]...)
}


func (kv *KVStore) LLen(key string) int {
	if kv.clusterEnabled {
		s := kv.getShard(key)
		s.mu.RLock()
		defer s.mu.RUnlock()
		if v, ok := s.m[key]; ok && v.Type == ListType {
			return len(v.List)
		}
		return 0
	}
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	return len(kv.Lists[key])
}


