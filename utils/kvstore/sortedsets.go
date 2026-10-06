package kvstore

import (
	"slices"
	"sort"
	"fmt"
	"strconv"
)

// zadd returns a new slice so readers holding the old one are unaffected.
func zadd(current []SortedSetMember, members []string, scores []float64) ([]SortedSetMember, int) {
	next := slices.Clone(current)
	added := 0
	for i, score := range scores {
		member := members[i]
		found := false
		for j := range next {
			if next[j].Member == member {
				next[j].Score = score
				found = true
				break
			}
		}
		if !found {
			next = append(next, SortedSetMember{Member: member, Score: score})
			added++
		}
	}
	sort.SliceStable(next, func(i, j int) bool { return next[i].Score < next[j].Score })
	return next, added
}

func (kv *KVStore) ZAdd(key string, pairs ...string) (int, error) {
	if len(pairs)%2 != 0 {
		return 0, fmt.Errorf("ERR ZADD requires an even number of arguments")
	}
	scores := make([]float64, len(pairs)/2)
	members := make([]string, len(pairs)/2)
	for i := 0; i < len(pairs); i += 2 {
		score, err := strconv.ParseFloat(pairs[i], 64)
		if err != nil {
			return 0, fmt.Errorf("ERR ZADD invalid score")
		}
		scores[i/2] = score
		members[i/2] = pairs[i+1]
	}
	if kv.clusterEnabled {
		s := kv.getShard(key)
		s.mu.Lock()
		defer s.mu.Unlock()
		v, ok := s.m[key]
		if ok && v.Type != SortedSetType {
			return 0, ErrWrongType
		}
		next, added := zadd(v.SortedSet, members, scores)
		s.ensure()
		s.m[key] = Value{Type: SortedSetType, SortedSet: next}
		return added, nil
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	next, added := zadd(kv.SortedSets[key], members, scores)
	kv.SortedSets[key] = next
	return added, nil
}

func (kv *KVStore) ZRange(key string, start, end int) []string {
	var sortedSet []SortedSetMember
	if kv.clusterEnabled {
		s := kv.getShard(key)
		s.mu.RLock()
		defer s.mu.RUnlock()
		if v, ok := s.m[key]; ok && v.Type == SortedSetType {
			sortedSet = v.SortedSet
		}
	} else {
		kv.mu.RLock()
		defer kv.mu.RUnlock()
		sortedSet = kv.SortedSets[key]
	}
	start, end, ok := rangeBounds(len(sortedSet), start, end)
	if !ok {
		return []string{}
	}
	result := make([]string, 0, end-start+1)
	for _, member := range sortedSet[start : end+1] {
		result = append(result, member.Member)
	}
	return result
}

// zrem returns the kept members and how many were removed.
func zrem(current []SortedSetMember, members []string) ([]SortedSetMember, int) {
	remove := make(map[string]struct{}, len(members))
	for _, member := range members {
		remove[member] = struct{}{}
	}
	kept := make([]SortedSetMember, 0, len(current))
	for _, member := range current {
		if _, ok := remove[member.Member]; !ok {
			kept = append(kept, member)
		}
	}
	return kept, len(current) - len(kept)
}

func (kv *KVStore) ZRem(key string, members ...string) int {
	if kv.clusterEnabled {
		s := kv.getShard(key)
		s.mu.Lock()
		defer s.mu.Unlock()
		v, ok := s.m[key]
		if !ok || v.Type != SortedSetType {
			return 0
		}
		kept, removed := zrem(v.SortedSet, members)
		switch {
		case removed == 0:
		case len(kept) == 0:
			delete(s.m, key)
		default:
			s.m[key] = Value{Type: SortedSetType, SortedSet: kept}
		}
		return removed
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kept, removed := zrem(kv.SortedSets[key], members)
	switch {
	case removed == 0:
	case len(kept) == 0:
		delete(kv.SortedSets, key)
	default:
		kv.SortedSets[key] = kept
	}
	return removed
}
