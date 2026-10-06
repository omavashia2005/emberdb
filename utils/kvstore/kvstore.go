package kvstore

import (
	"errors"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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

// ErrWrongType matches Redis's WRONGTYPE error. Returned by methods that
// already return an error; methods without an error return leave a key of the
// wrong type untouched instead of overwriting it.
var ErrWrongType = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")

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

// clusterSlotShard holds one cluster slot's keys. Every read and write of m,
// including reads and writes of the maps and slices inside its Values, must
// happen while holding mu.
type clusterSlotShard struct {
	mu sync.RWMutex
	m  map[string]Value
}

func (s *clusterSlotShard) ensure() {
	if s.m == nil {
		s.m = make(map[string]Value)
	}
}

// KVStore has two independent layouts:
//   - cluster mode: keys live in slots, each guarded by its own lock. kv.mu is
//     never taken in this mode.
//   - non-cluster mode: keys live in the per-type maps, all guarded by kv.mu.
type KVStore struct {
	Strings           map[string]string
	Lists             map[string][]string
	Hashes            map[string]map[string]string
	Sets              map[string]map[string]struct{}
	SortedSets        map[string][]SortedSetMember
	Expirations       map[string]time.Time
	mu                sync.RWMutex
	CommandsProcessed atomic.Int64
	slots             [16384]clusterSlotShard
	clusterEnabled    bool
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

func (kv *KVStore) shard(key string) *clusterSlotShard {
	return &kv.slots[SlotForKey(key)]
}

// --- strings ---

func (kv *KVStore) Set(key, value string) {
	if kv.clusterEnabled {
		s := kv.shard(key)
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

// Mset writes all pairs atomically. In cluster mode the server rejects
// cross-slot MSETs, so this is normally one slot and one lock; if keys do span
// slots, their locks are taken in ascending slot order to avoid deadlock.
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

// Mget reads all keys under one lock (or one lock per slot, in slot order),
// so it can't observe a write to one key without the other, unlike looping
// over GetString/Get. Missing keys or non-string values map to "(nil)".
func (kv *KVStore) Mget(keys []string) []string {
	if len(keys) == 0 {
		return nil
	}
	result := make([]string, len(keys))
	if !kv.clusterEnabled {
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

	get := func(s *clusterSlotShard, k string) string {
		if v, ok := s.m[k]; ok && v.Type == StringType {
			return v.String
		}
		return "(nil)"
	}

	slotIDs := make([]uint16, len(keys))
	single := true
	for i, k := range keys {
		slotIDs[i] = SlotForKey(k)
		if slotIDs[i] != slotIDs[0] {
			single = false
		}
	}

	if single {
		s := &kv.slots[slotIDs[0]]
		s.mu.RLock()
		for i, k := range keys {
			result[i] = get(s, k)
		}
		s.mu.RUnlock()
		return result
	}

	order := slices.Clone(slotIDs)
	slices.Sort(order)
	order = slices.Compact(order)
	for _, id := range order {
		kv.slots[id].mu.RLock()
	}
	for i, k := range keys {
		result[i] = get(&kv.slots[slotIDs[i]], k)
	}
	for _, id := range order {
		kv.slots[id].mu.RUnlock()
	}
	return result
}

func (kv *KVStore) Get(key string) string {
	if value, ok := kv.GetString(key); ok {
		return value
	}
	return "(nil)"
}

// GetString returns a string value and whether the key exists. Callers that
// must distinguish a missing key from a stored value (GET, MGET, MIGRATE) use
// this instead of Get, which folds "missing" into the "(nil)" sentinel.
func (kv *KVStore) GetString(key string) (string, bool) {
	if kv.clusterEnabled {
		s := kv.shard(key)
		s.mu.RLock()
		v, ok := s.m[key]
		s.mu.RUnlock()
		if !ok || v.Type != StringType {
			return "", false
		}
		return v.String, true
	}
	kv.mu.RLock()
	value, ok := kv.Strings[key]
	kv.mu.RUnlock()
	return value, ok
}

func (kv *KVStore) Delete(key string) int {
	if kv.clusterEnabled {
		s := kv.shard(key)
		s.mu.Lock()
		_, ok := s.m[key]
		if ok {
			delete(s.m, key)
		}
		s.mu.Unlock()
		if ok {
			return 1
		}
		return 0
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, stringFound := kv.Strings[key]
	_, listFound := kv.Lists[key]
	_, hashFound := kv.Hashes[key]
	_, setFound := kv.Sets[key]
	_, sortedSetFound := kv.SortedSets[key]
	if !stringFound && !listFound && !hashFound && !setFound && !sortedSetFound {
		return 0
	}
	delete(kv.Strings, key)
	delete(kv.Lists, key)
	delete(kv.Hashes, key)
	delete(kv.Sets, key)
	delete(kv.SortedSets, key)
	return 1
}

func (kv *KVStore) Append(key, suffix string) error {
	if kv.clusterEnabled {
		s := kv.shard(key)
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

// applyDelta parses current (empty when the key is absent) and adds delta.
func applyDelta(current string, exists bool, delta int) (string, error) {
	n := 0
	if exists {
		var err error
		if n, err = strconv.Atoi(current); err != nil {
			return "", fmt.Errorf("ERR value is not an integer")
		}
	}
	return strconv.Itoa(n + delta), nil
}

func (kv *KVStore) changeInteger(key string, delta int) error {
	if kv.clusterEnabled {
		s := kv.shard(key)
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

// --- lists ---

// prepend keeps EmberDB's existing LPUSH order: LPUSH k a b c yields [a b c ...].
func prepend(list, values []string) []string {
	out := make([]string, 0, len(values)+len(list))
	out = append(out, values...)
	return append(out, list...)
}

func (kv *KVStore) LPush(key string, values ...string) int {
	if kv.clusterEnabled {
		s := kv.shard(key)
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
		s := kv.shard(key)
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
		s := kv.shard(key)
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
		s := kv.shard(key)
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
		s := kv.shard(key)
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
		s := kv.shard(key)
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

// --- hashes ---

func (kv *KVStore) HSet(key, field, value string) {
	kv.HMSet(key, map[string]string{field: value})
}

func (kv *KVStore) HMSet(key string, fields map[string]string) {
	if len(fields) == 0 {
		return
	}
	if kv.clusterEnabled {
		s := kv.shard(key)
		s.mu.Lock()
		defer s.mu.Unlock()
		v, ok := s.m[key]
		if ok && v.Type != HashType {
			return
		}
		if !ok {
			v = Value{Type: HashType, Hash: make(map[string]string, len(fields))}
		}
		for f, val := range fields {
			v.Hash[f] = val
		}
		s.ensure()
		s.m[key] = v
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	hash := kv.Hashes[key]
	if hash == nil {
		hash = make(map[string]string, len(fields))
		kv.Hashes[key] = hash
	}
	for f, val := range fields {
		hash[f] = val
	}
}

func (kv *KVStore) HGet(key, field string) (string, bool) {
	if kv.clusterEnabled {
		s := kv.shard(key)
		s.mu.RLock()
		defer s.mu.RUnlock()
		v, ok := s.m[key]
		if !ok || v.Type != HashType {
			return "", false
		}
		result, ok := v.Hash[field]
		return result, ok
	}
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	result, ok := kv.Hashes[key][field]
	return result, ok
}

func (kv *KVStore) HMGet(key string, fields ...string) []any {
	var hash map[string]string
	if kv.clusterEnabled {
		s := kv.shard(key)
		s.mu.RLock()
		defer s.mu.RUnlock()
		if v, ok := s.m[key]; ok && v.Type == HashType {
			hash = v.Hash
		}
	} else {
		kv.mu.RLock()
		defer kv.mu.RUnlock()
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
	var hash map[string]string
	if kv.clusterEnabled {
		s := kv.shard(key)
		s.mu.RLock()
		defer s.mu.RUnlock()
		if v, ok := s.m[key]; ok && v.Type == HashType {
			hash = v.Hash
		}
	} else {
		kv.mu.RLock()
		defer kv.mu.RUnlock()
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
	removeFields := func(hash map[string]string) int {
		removed := 0
		for _, field := range fields {
			if _, ok := hash[field]; ok {
				delete(hash, field)
				removed++
			}
		}
		return removed
	}
	if kv.clusterEnabled {
		s := kv.shard(key)
		s.mu.Lock()
		defer s.mu.Unlock()
		v, ok := s.m[key]
		if !ok || v.Type != HashType {
			return 0
		}
		removed := removeFields(v.Hash)
		if len(v.Hash) == 0 {
			delete(s.m, key)
		}
		return removed
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	hash := kv.Hashes[key]
	removed := removeFields(hash)
	if hash != nil && len(hash) == 0 {
		delete(kv.Hashes, key)
	}
	return removed
}

// --- sets ---

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
		s := kv.shard(key)
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
		s := kv.shard(key)
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
		s := kv.shard(key)
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
		s := kv.shard(key)
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

// --- sorted sets ---

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
		s := kv.shard(key)
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
		s := kv.shard(key)
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
		s := kv.shard(key)
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

// --- whole-store and cluster helpers ---

// FlushAll clears every key. In cluster mode slots are cleared one at a time,
// so a write landing mid-flush in an already-cleared slot survives, matching
// Redis's FLUSHALL ASYNC semantics.
func (kv *KVStore) FlushAll() {
	if kv.clusterEnabled {
		for i := range kv.slots {
			s := &kv.slots[i]
			s.mu.Lock()
			s.m = nil
			s.mu.Unlock()
		}
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.Strings = make(map[string]string)
	kv.Lists = make(map[string][]string)
	kv.Hashes = make(map[string]map[string]string)
	kv.Sets = make(map[string]map[string]struct{})
	kv.SortedSets = make(map[string][]SortedSetMember)
	kv.Expirations = make(map[string]time.Time)
}

// GetKeysInSlot returns up to count keys stored in slot. Keys may be added or
// removed after it returns, so callers (MIGRATE) must re-read each key.
func (kv *KVStore) GetKeysInSlot(slot uint64, count int) []string {
	if slot >= uint64(len(kv.slots)) || count <= 0 {
		return []string{}
	}
	s := &kv.slots[slot]
	s.mu.RLock()
	defer s.mu.RUnlock()

	keys := make([]string, 0, min(count, len(s.m)))
	for k := range s.m {
		if len(keys) == count {
			break
		}
		keys = append(keys, k)
	}
	return keys
}
