package kvstore

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// rdbSnapshot is the on-disk shape of KVStore data.
type rdbSnapshot struct {
	Strings     map[string]string
	Lists       map[string][]string
	Hashes      map[string]map[string]string
	Sets        map[string]map[string]struct{}
	SortedSets  map[string][]SortedSetMember
	Expirations map[string]time.Time
	SlotKeys    [16384]map[string]Value
}

func (kv *KVStore) applyAOF(args []string) error {
	cmd := strings.ToUpper(args[0])
	args = args[1:]
	switch cmd {
	case "SET":
		if len(args) != 2 {
			break
		}
		kv.Set(args[0], args[1])
		return nil
	case "DEL":
		if len(args) < 1 {
			break
		}
		for _, key := range args {
			kv.Delete(key)
		}
		return nil
	case "FLUSHALL":
		if len(args) != 0 {
			break
		}
		kv.FlushAll()
		return nil
	case "LPUSH":
		if len(args) < 2 {
			break
		}
		kv.LPush(args[0], args[1:]...)
		return nil
	case "LPOP":
		if len(args) != 1 {
			break
		}
		kv.LPop(args[0])
		return nil
	case "RPUSH":
		if len(args) < 2 {
			break
		}
		kv.RPush(args[0], args[1:]...)
		return nil
	case "RPOP":
		if len(args) != 1 {
			break
		}
		kv.RPop(args[0])
		return nil
	case "HSET", "HMSET":
		if len(args) < 3 || len(args)%2 == 0 {
			break
		}
		for i := 1; i < len(args); i += 2 {
			kv.HSet(args[0], args[i], args[i+1])
		}
		return nil
	case "HDEL":
		if len(args) < 2 {
			break
		}
		kv.HDel(args[0], args[1:]...)
		return nil
	case "SADD":
		if len(args) < 2 {
			break
		}
		kv.SAdd(args[0], args[1:]...)
		return nil
	case "SREM":
		if len(args) < 2 {
			break
		}
		kv.SRem(args[0], args[1:]...)
		return nil
	case "ZADD":
		if len(args) < 3 || len(args)%2 == 0 {
			break
		}
		_, err := kv.ZAdd(args[0], args[1:]...)
		return err
	case "ZREM":
		if len(args) < 2 {
			break
		}
		kv.ZRem(args[0], args[1:]...)
		return nil
	}
	return fmt.Errorf("invalid %s command", cmd)
}

func (kv *KVStore) snapshot() rdbSnapshot {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	return kv.snapshotLocked()
}

func (kv *KVStore) snapshotLocked() rdbSnapshot {
	s := rdbSnapshot{
		Strings:     cloneMap(kv.Strings),
		Lists:       cloneSlices(kv.Lists),
		Hashes:      cloneNestedMap(kv.Hashes),
		Sets:        cloneNestedMap(kv.Sets),
		SortedSets:  cloneSlices(kv.SortedSets),
		Expirations: cloneMap(kv.Expirations),
	}
	for slot, values := range kv.SlotKeys {
		if values == nil {
			continue
		}
		s.SlotKeys[slot] = make(map[string]Value, len(values))
		for key, value := range values {
			value.List = append([]string(nil), value.List...)
			value.Hash = cloneMap(value.Hash)
			value.Set = cloneMap(value.Set)
			value.SortedSet = append([]SortedSetMember(nil), value.SortedSet...)
			s.SlotKeys[slot][key] = value
		}
	}
	return s
}

func cloneMap[K comparable, V any](source map[K]V) map[K]V {
	clone := make(map[K]V, len(source))
	for key, value := range source {
		clone[key] = value
	}
	return clone
}

func cloneSlices[K comparable, V any](source map[K][]V) map[K][]V {
	clone := make(map[K][]V, len(source))
	for key, value := range source {
		clone[key] = append([]V(nil), value...)
	}
	return clone
}

func cloneNestedMap[K1, K2 comparable, V any](source map[K1]map[K2]V) map[K1]map[K2]V {
	clone := make(map[K1]map[K2]V, len(source))
	for key, value := range source {
		clone[key] = cloneMap(value)
	}
	return clone
}

func (kv *KVStore) restore(s rdbSnapshot) {
	kv.Strings = s.Strings
	kv.Lists = s.Lists
	kv.Hashes = s.Hashes
	kv.Sets = s.Sets
	kv.SortedSets = s.SortedSets
	kv.Expirations = s.Expirations
	kv.SlotKeys = s.SlotKeys
}

func (kv *KVStore) keyCount() int {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	count := len(kv.Strings) + len(kv.Lists) + len(kv.Hashes) + len(kv.Sets) + len(kv.SortedSets)
	for _, values := range kv.SlotKeys {
		count += len(values)
	}
	return count
}

func snapshotCommands(s rdbSnapshot) [][]string {
	var commands [][]string
	for _, key := range sortedKeys(s.Strings) {
		commands = append(commands, []string{"SET", key, s.Strings[key]})
	}
	for _, key := range sortedKeys(s.Lists) {
		if len(s.Lists[key]) > 0 {
			commands = append(commands, append([]string{"RPUSH", key}, s.Lists[key]...))
		}
	}
	for _, key := range sortedKeys(s.Hashes) {
		command := []string{"HSET", key}
		for _, field := range sortedKeys(s.Hashes[key]) {
			command = append(command, field, s.Hashes[key][field])
		}
		if len(command) > 2 {
			commands = append(commands, command)
		}
	}
	for _, key := range sortedKeys(s.Sets) {
		if len(s.Sets[key]) > 0 {
			commands = append(commands, append([]string{"SADD", key}, sortedKeys(s.Sets[key])...))
		}
	}
	for _, key := range sortedKeys(s.SortedSets) {
		if len(s.SortedSets[key]) > 0 {
			commands = append(commands, zaddCommand(key, s.SortedSets[key]))
		}
	}
	for _, values := range s.SlotKeys {
		for _, key := range sortedKeys(values) {
			value := values[key]
			switch value.Type {
			case StringType:
				commands = append(commands, []string{"SET", key, value.String})
			case ListType:
				if len(value.List) > 0 {
					commands = append(commands, append([]string{"RPUSH", key}, value.List...))
				}
			case HashType:
				command := []string{"HSET", key}
				for _, field := range sortedKeys(value.Hash) {
					command = append(command, field, value.Hash[field])
				}
				if len(command) > 2 {
					commands = append(commands, command)
				}
			case SetType:
				if len(value.Set) > 0 {
					commands = append(commands, append([]string{"SADD", key}, sortedKeys(value.Set)...))
				}
			case SortedSetType:
				if len(value.SortedSet) > 0 {
					commands = append(commands, zaddCommand(key, value.SortedSet))
				}
			}
		}
	}
	return commands
}

func zaddCommand(key string, members []SortedSetMember) []string {
	command := []string{"ZADD", key}
	for _, member := range members {
		command = append(command, strconv.FormatFloat(member.Score, 'g', -1, 64), member.Member)
	}
	return command
}

func sortedKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
