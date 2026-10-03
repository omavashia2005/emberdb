package kvstore

import "sort"

func (kv *KVStore) SAdd(key string, members ...string) int {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	var set map[string]struct{}
	if kv.clusterEnabled {
		value, _ := kv.value(key, SetType)
		set = value.Set
	} else {
		set = kv.Sets[key]
	}
	if set == nil {
		set = make(map[string]struct{})
	}
	added := 0
	for _, member := range members {
		if _, ok := set[member]; !ok {
			set[member] = struct{}{}
			added++
		}
	}
	if kv.clusterEnabled {
		kv.setValue(key, Value{Type: SetType, Set: set})
	} else {
		kv.Sets[key] = set
	}
	return added
}

func (kv *KVStore) SMembers(key string) []string {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	var set map[string]struct{}
	if kv.clusterEnabled {
		value, _ := kv.value(key, SetType)
		set = value.Set
	} else {
		set = kv.Sets[key]
	}
	members := make([]string, 0, len(set))
	for member := range set {
		members = append(members, member)
	}
	sort.Strings(members)
	return members
}

func (kv *KVStore) SIsMember(key, member string) bool {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	if kv.clusterEnabled {
		value, ok := kv.value(key, SetType)
		if !ok {
			return false
		}
		_, ok = value.Set[member]
		return ok
	}
	_, ok := kv.Sets[key][member]
	return ok
}

func (kv *KVStore) SRem(key string, members ...string) int {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	var set map[string]struct{}
	if kv.clusterEnabled {
		value, _ := kv.value(key, SetType)
		set = value.Set
	} else {
		set = kv.Sets[key]
	}
	removed := 0
	for _, member := range members {
		if _, ok := set[member]; ok {
			delete(set, member)
			removed++
		}
	}
	return removed
}
