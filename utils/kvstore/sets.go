package kvstore

import "sort"



func (kv *KVStore) SAdd(key string, members ...string) int {
	addMembers := func(set map[string]struct{}) int {
		added := 0
		for _, member := range members {
			if _, ok := set[member]; !ok {
				set[member] = struct{}{}
				added++
			}
		}
		return added
	}
	if kv.clusterEnabled {
		s := kv.getShard(key)
		s.mu.Lock()
		defer s.mu.Unlock()
		v, ok := s.m[key]
		if ok && v.Type != SetType {
			return 0
		}
		if len(members) == 0 {
			return 0
		}
		if !ok {
			v = Value{Type: SetType, Set: make(map[string]struct{}, len(members))}
		}
		added := addMembers(v.Set)
		s.ensure()
		s.m[key] = v
		return added
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if len(members) == 0 {
		return 0
	}
	set := kv.Sets[key]
	if set == nil {
		set = make(map[string]struct{}, len(members))
		kv.Sets[key] = set
	}
	return addMembers(set)
}

func (kv *KVStore) SMembers(key string) []string {
	var set map[string]struct{}
	if kv.clusterEnabled {
		s := kv.getShard(key)
		s.mu.RLock()
		defer s.mu.RUnlock()
		if v, ok := s.m[key]; ok && v.Type == SetType {
			set = v.Set
		}
	} else {
		kv.mu.RLock()
		defer kv.mu.RUnlock()
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
	if kv.clusterEnabled {
		s := kv.getShard(key)
		s.mu.RLock()
		defer s.mu.RUnlock()
		v, ok := s.m[key]
		if !ok || v.Type != SetType {
			return false
		}
		_, ok = v.Set[member]
		return ok
	}
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	_, ok := kv.Sets[key][member]
	return ok
}

func (kv *KVStore) SRem(key string, members ...string) int {
	removeMembers := func(set map[string]struct{}) int {
		removed := 0
		for _, member := range members {
			if _, ok := set[member]; ok {
				delete(set, member)
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
		if !ok || v.Type != SetType {
			return 0
		}
		removed := removeMembers(v.Set)
		if len(v.Set) == 0 {
			delete(s.m, key)
		}
		return removed
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	set := kv.Sets[key]
	removed := removeMembers(set)
	if set != nil && len(set) == 0 {
		delete(kv.Sets, key)
	}
	return removed
}
