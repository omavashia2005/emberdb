package kvstore

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lobaro/crc16"
)

type DataType uint8

const (
	StringType DataType = iota
	ListType
	HashType
	SetType
	SortedSetType
)

type SortedSetMember struct {
	Member string
	Score  float64
}

// Value is the valid-data-type representation used by clustered slots.
type Value struct {
	Type      DataType
	String    string
	List      []string
	Hash      map[string]string
	Set       map[string]struct{}
	SortedSet []SortedSetMember
}

type KVStore struct {
	Strings           map[string]string
	Lists             map[string][]string
	Hashes            map[string]map[string]string
	Sets              map[string]map[string]struct{}
	SortedSets        map[string][]SortedSetMember
	Expirations       map[string]time.Time
	mu                sync.RWMutex
	CommandsProcessed int
	SlotKeys          [16384]map[string]Value
	clusterEnabled    bool
	persistence       *persistence
}

func NewKVStore(clusterEnabled ...bool) *KVStore {
	kv := &KVStore{
		Strings:     make(map[string]string),
		Lists:       make(map[string][]string),
		Hashes:      make(map[string]map[string]string),
		Sets:        make(map[string]map[string]struct{}),
		SortedSets:  make(map[string][]SortedSetMember),
		Expirations: make(map[string]time.Time),
	}
	if len(clusterEnabled) > 0 {
		kv.clusterEnabled = clusterEnabled[0]
	}
	return kv
}

func SlotForKey(key string) uint16 {
	start := strings.IndexByte(key, '{')
	if start >= 0 {
		if end := strings.IndexByte(key[start+1:], '}'); end > 0 {
			key = key[start+1 : start+1+end]
		}
	}
	return crc16.ChecksumXModem([]byte(key)) % 16384
}

func (kv *KVStore) value(key string, dataType DataType) (Value, bool) {
	value, ok := kv.SlotKeys[SlotForKey(key)][key]
	return value, ok && value.Type == dataType
}

func (kv *KVStore) setValue(key string, value Value) {
	slot := SlotForKey(key)
	if kv.SlotKeys[slot] == nil {
		kv.SlotKeys[slot] = make(map[string]Value)
	}
	kv.SlotKeys[slot][key] = value
}

func (kv *KVStore) Set(key, value string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.persist("SET", key, value) != nil {
		return
	}
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

func (kv *KVStore) Delete(key string) int {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.clusterEnabled {
		slot := SlotForKey(key)
		if _, ok := kv.SlotKeys[slot][key]; !ok {
			return 0
		}
		if kv.persist("DEL", key) != nil {
			return 0
		}
		delete(kv.SlotKeys[slot], key)
		return 1
	}
	_, stringFound := kv.Strings[key]
	_, listFound := kv.Lists[key]
	_, hashFound := kv.Hashes[key]
	_, setFound := kv.Sets[key]
	_, sortedSetFound := kv.SortedSets[key]
	if !stringFound && !listFound && !hashFound && !setFound && !sortedSetFound {
		return 0
	}
	if kv.persist("DEL", key) != nil {
		return 0
	}
	if stringFound {
		delete(kv.Strings, key)
	}
	if listFound {
		delete(kv.Lists, key)
	}
	if hashFound {
		delete(kv.Hashes, key)
	}
	if setFound {
		delete(kv.Sets, key)
	}
	if sortedSetFound {
		delete(kv.SortedSets, key)
	}
	return 1
}

func (kv *KVStore) Append(key, suffix string) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.clusterEnabled {
		value, _ := kv.value(key, StringType)
		if err := kv.persist("SET", key, value.String+suffix); err != nil {
			return err
		}
		kv.setValue(key, Value{Type: StringType, String: value.String + suffix})
		return nil
	}
	if err := kv.persist("SET", key, kv.Strings[key]+suffix); err != nil {
		return err
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
	if err := kv.persist("SET", key, current); err != nil {
		return err
	}
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
	if len(values) == 0 || kv.persist(append([]string{"LPUSH", key}, values...)...) != nil {
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
	if kv.persist("LPOP", key) != nil {
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
	if len(values) == 0 || kv.persist(append([]string{"RPUSH", key}, values...)...) != nil {
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
	if kv.persist("RPOP", key) != nil {
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
	command := []string{"HMSET", key}
	for _, field := range sortedKeys(fields) {
		command = append(command, field, fields[field])
	}
	if kv.persist(command...) != nil {
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
			removed++
		}
	}
	if removed == 0 || kv.persist(append([]string{"HDEL", key}, fields...)...) != nil {
		return 0
	}
	removed = 0
	for _, field := range fields {
		if _, ok := hash[field]; ok {
			delete(hash, field)
			removed++
		}
	}
	return removed
}

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
	newMembers := make(map[string]struct{}, len(members))
	for _, member := range members {
		if _, ok := set[member]; !ok {
			newMembers[member] = struct{}{}
		}
	}
	added = len(newMembers)
	if added > 0 {
		if kv.persist(append([]string{"SADD", key}, members...)...) != nil {
			return 0
		}
		for member := range newMembers {
			set[member] = struct{}{}
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
			removed++
		}
	}
	if removed == 0 || kv.persist(append([]string{"SREM", key}, members...)...) != nil {
		return 0
	}
	removed = 0
	for _, member := range members {
		if _, ok := set[member]; ok {
			delete(set, member)
			removed++
		}
	}
	return removed
}

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
	if err := kv.persist(append([]string{"ZADD", key}, pairs...)...); err != nil {
		return 0, err
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
	if kv.persist(append([]string{"ZREM", key}, members...)...) != nil {
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

func (kv *KVStore) FlushAll() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.persist("FLUSHALL") != nil {
		return
	}
	if kv.clusterEnabled {
		kv.SlotKeys = [16384]map[string]Value{}
		return
	}
	kv.Strings = make(map[string]string)
	kv.Lists = make(map[string][]string)
	kv.Hashes = make(map[string]map[string]string)
	kv.Sets = make(map[string]map[string]struct{})
	kv.SortedSets = make(map[string][]SortedSetMember)
	kv.Expirations = make(map[string]time.Time)
}

func (kv *KVStore) GetKeysInSlot(slot uint64, count int) []string {
	if slot >= uint64(len(kv.SlotKeys)) || count <= 0 {
		return []string{}
	}
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	keys := make([]string, 0, count)
	for key := range kv.SlotKeys[slot] {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if len(keys) > count {
		keys = keys[:count]
	}
	return keys
}
