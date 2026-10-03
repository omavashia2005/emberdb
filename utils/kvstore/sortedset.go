package kvstore

import (
	"fmt"
	"sort"
	"strconv"
)

func (kv *KVStore) ZAdd(key string, pairs ...string) (int, error) {
	if len(pairs)%2 != 0 {
		return 0, fmt.Errorf("ERR ZADD requires an even number of arguments")
	}
	scores := make([]float64, len(pairs)/2)
	for i := 0; i < len(pairs); i += 2 {
		score, err := strconv.ParseFloat(pairs[i], 64)
		if err != nil {
			return 0, fmt.Errorf("ERR ZADD invalid score")
		}
		scores[i/2] = score
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	var sortedSet []SortedSetMember
	if kv.clusterEnabled {
		value, _ := kv.value(key, SortedSetType)
		sortedSet = value.SortedSet
	} else {
		sortedSet = kv.SortedSets[key]
	}
	added := 0
	for i, score := range scores {
		member := pairs[i*2+1]
		found := false
		for j := range sortedSet {
			if sortedSet[j].Member == member {
				sortedSet[j].Score = score
				found = true
				break
			}
		}
		if !found {
			sortedSet = append(sortedSet, SortedSetMember{Member: member, Score: score})
			added++
		}
	}
	sort.SliceStable(sortedSet, func(i, j int) bool { return sortedSet[i].Score < sortedSet[j].Score })
	if kv.clusterEnabled {
		kv.setValue(key, Value{Type: SortedSetType, SortedSet: sortedSet})
	} else {
		kv.SortedSets[key] = sortedSet
	}
	return added, nil
}

func (kv *KVStore) ZRange(key string, start, end int) []string {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	var sortedSet []SortedSetMember
	if kv.clusterEnabled {
		value, _ := kv.value(key, SortedSetType)
		sortedSet = value.SortedSet
	} else {
		sortedSet = kv.SortedSets[key]
	}
	if start < 0 {
		start += len(sortedSet)
	}
	if end < 0 {
		end += len(sortedSet)
	}
	if start < 0 {
		start = 0
	}
	if end >= len(sortedSet) {
		end = len(sortedSet) - 1
	}
	if start > end || start >= len(sortedSet) || end < 0 {
		return []string{}
	}
	result := make([]string, 0, end-start+1)
	for _, member := range sortedSet[start : end+1] {
		result = append(result, member.Member)
	}
	return result
}

func (kv *KVStore) ZRem(key string, members ...string) int {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	var sortedSet []SortedSetMember
	if kv.clusterEnabled {
		value, _ := kv.value(key, SortedSetType)
		sortedSet = value.SortedSet
	} else {
		sortedSet = kv.SortedSets[key]
	}
	remove := make(map[string]struct{}, len(members))
	for _, member := range members {
		remove[member] = struct{}{}
	}
	removed := 0
	for _, member := range sortedSet {
		if _, ok := remove[member.Member]; ok {
			removed++
		}
	}
	if removed == 0 {
		return 0
	}
	kept := make([]SortedSetMember, 0, len(sortedSet)-removed)
	for _, member := range sortedSet {
		if _, ok := remove[member.Member]; !ok {
			kept = append(kept, member)
		}
	}
	if kv.clusterEnabled {
		kv.setValue(key, Value{Type: SortedSetType, SortedSet: kept})
	} else {
		kv.SortedSets[key] = kept
	}
	return removed
}
