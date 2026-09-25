package kvstore

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"hash/crc64"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/omavashia2005/emberdb/utils"
)

const (
	rdbFilename = "dump.rdb"
	aofFilename = "appendonly.aof"
	rdbMagic    = "EMBERDB001"
)

var rdbChecksumTable = crc64.MakeTable(crc64.ECMA)

type persistence struct {
	dir       string
	aof       *os.File
	mu        sync.Mutex
	saveMu    sync.Mutex
	rewriteMu sync.Mutex
	aofErr    error
	rdbErr    error
	dirty     atomic.Uint64
	lastSave  atomic.Int64
	baseSize  atomic.Int64
	stop      chan struct{}
	done      chan struct{}
}

type rdbSnapshot struct {
	Strings     map[string]string
	Lists       map[string][]string
	Hashes      map[string]map[string]string
	Sets        map[string]map[string]struct{}
	SortedSets  map[string][]SortedSetMember
	Expirations map[string]time.Time
	SlotKeys    [16384]map[string]Value
}

// OpenPersistent loads the AOF when present (otherwise RDB), then enables both
// Redis-style snapshotting and appendfsync-everysec persistence.
func OpenPersistent(dir string, clusterEnabled ...bool) (*KVStore, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create persistence directory: %w", err)
	}

	kv := NewKVStore(clusterEnabled...)
	p := &persistence{
		dir:  dir,
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
	p.lastSave.Store(time.Now().Unix())

	aofPath := filepath.Join(dir, aofFilename)
	aofData, err := os.ReadFile(aofPath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("read AOF: %w", err)
	}

	if len(aofData) > 0 {
		valid, err := kv.replayAOF(aofData)
		if err != nil {
			return nil, err
		}
		if valid != len(aofData) {
			if err := os.Truncate(aofPath, int64(valid)); err != nil {
				return nil, fmt.Errorf("truncate incomplete AOF tail: %w", err)
			}
		}
	} else if err := kv.loadRDB(filepath.Join(dir, rdbFilename)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	p.aof, err = os.OpenFile(aofPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open AOF: %w", err)
	}
	kv.persistence = p
	if info, statErr := p.aof.Stat(); statErr == nil {
		p.baseSize.Store(info.Size())
	}

	// If startup came from RDB, seed AOF before it becomes authoritative.
	if len(aofData) == 0 && kv.keyCount() > 0 {
		if err := kv.RewriteAOF(); err != nil {
			_ = p.aof.Close()
			return nil, err
		}
	}

	go kv.persistenceLoop()
	return kv, nil
}

// Close flushes the AOF and writes a final point-in-time snapshot.
func (kv *KVStore) Close() error {
	p := kv.persistence
	if p == nil {
		return nil
	}
	close(p.stop)
	<-p.done
	rdbErr := kv.SaveRDB()
	p.mu.Lock()
	previousAOFErr := p.aofErr
	aofErr := p.aof.Sync()
	closeErr := p.aof.Close()
	p.mu.Unlock()
	return errors.Join(rdbErr, previousAOFErr, aofErr, closeErr)
}

func (kv *KVStore) persistenceLoop() {
	p := kv.persistence
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	defer close(p.done)

	for {
		select {
		case <-ticker.C:
			p.mu.Lock()
			if err := p.aof.Sync(); err != nil {
				p.aofErr = fmt.Errorf("%w: sync AOF: %v", utils.ErrStartup, err)
				utils.PrintError(p.aofErr)
			}
			info, statErr := p.aof.Stat()
			p.mu.Unlock()

			dirty := p.dirty.Load()
			elapsed := time.Now().Unix() - p.lastSave.Load()
			if dirty >= 10_000 && elapsed >= 60 || dirty >= 100 && elapsed >= 300 || dirty >= 1 && elapsed >= 3600 {
				_ = kv.SaveRDB()
			}
			if statErr == nil && info.Size() >= 64<<20 && info.Size() >= p.baseSize.Load()*2 {
				if err := kv.RewriteAOF(); err != nil {
					utils.PrintError(err)
				}
			}
		case <-p.stop:
			return
		}
	}
}

func (kv *KVStore) persist(args ...string) error {
	p := kv.persistence
	if p == nil {
		return nil
	}
	p.mu.Lock()
	if p.aofErr != nil {
		err := p.aofErr
		p.mu.Unlock()
		utils.PrintError(err)
		return err
	}
	if p.rdbErr != nil {
		err := p.rdbErr
		p.mu.Unlock()
		utils.PrintError(err)
		return err
	}
	err := writeRESP(p.aof, args)
	if err != nil {
		err = fmt.Errorf("%w: append AOF: %v", utils.ErrStartup, err)
		p.aofErr = err
		p.mu.Unlock()
		utils.PrintError(err)
		return err
	}
	p.mu.Unlock()
	p.dirty.Add(1)
	return nil
}

func writeRESP(w io.Writer, args []string) error {
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, arg := range args {
		fmt.Fprintf(&b, "$%d\r\n", len(arg))
		b.WriteString(arg)
		b.WriteString("\r\n")
	}
	_, err := io.WriteString(w, b.String())
	return err
}

func parseRESP(data []byte, offset int) ([]string, int, error) {
	start := offset
	line := func() ([]byte, bool) {
		i := bytes.Index(data[offset:], []byte("\r\n"))
		if i < 0 {
			return nil, false
		}
		value := data[offset : offset+i]
		offset += i + 2
		return value, true
	}

	header, ok := line()
	if !ok {
		return nil, start, io.ErrUnexpectedEOF
	}
	if len(header) < 2 || header[0] != '*' {
		return nil, start, fmt.Errorf("invalid AOF array at byte %d", start)
	}
	count, err := strconv.Atoi(string(header[1:]))
	if err != nil || count < 1 {
		return nil, start, fmt.Errorf("invalid AOF array length at byte %d", start)
	}

	args := make([]string, count)
	for i := range args {
		bulk, ok := line()
		if !ok {
			return nil, start, io.ErrUnexpectedEOF
		}
		if len(bulk) < 2 || bulk[0] != '$' {
			return nil, start, fmt.Errorf("invalid AOF bulk string at byte %d", offset-len(bulk)-2)
		}
		length, err := strconv.Atoi(string(bulk[1:]))
		if err != nil || length < 0 {
			return nil, start, fmt.Errorf("invalid AOF bulk length at byte %d", offset-len(bulk)-2)
		}
		if length > len(data)-offset-2 {
			return nil, start, io.ErrUnexpectedEOF
		}
		args[i] = string(data[offset : offset+length])
		offset += length
		if offset+2 > len(data) {
			return nil, start, io.ErrUnexpectedEOF
		}
		if data[offset] != '\r' || data[offset+1] != '\n' {
			return nil, start, fmt.Errorf("invalid AOF bulk terminator at byte %d", offset)
		}
		offset += 2
	}
	return args, offset, nil
}

func (kv *KVStore) replayAOF(data []byte) (int, error) {
	offset := 0
	for offset < len(data) {
		args, next, err := parseRESP(data, offset)
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return offset, nil
		}
		if err != nil {
			return offset, err
		}
		if err := kv.applyAOF(args); err != nil {
			return offset, fmt.Errorf("replay AOF at byte %d: %w", offset, err)
		}
		offset = next
	}
	return offset, nil
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

// SaveRDB atomically replaces dump.rdb with a checksummed point-in-time snapshot.
func (kv *KVStore) SaveRDB() (result error) {
	p := kv.persistence
	if p == nil {
		return fmt.Errorf("persistence is not enabled")
	}
	p.saveMu.Lock()
	defer p.saveMu.Unlock()
	defer func() {
		if result != nil {
			result = fmt.Errorf("%w: %v", utils.ErrStartup, result)
			utils.PrintError(result)
		}
		p.mu.Lock()
		p.rdbErr = result
		p.mu.Unlock()
	}()

	dirty := p.dirty.Load()
	var body bytes.Buffer
	body.WriteString(rdbMagic)
	if err := gob.NewEncoder(&body).Encode(kv.snapshot()); err != nil {
		return fmt.Errorf("encode RDB: %w", err)
	}
	checksum := crc64.Checksum(body.Bytes(), rdbChecksumTable)
	if err := binary.Write(&body, binary.BigEndian, checksum); err != nil {
		return fmt.Errorf("checksum RDB: %w", err)
	}

	path := filepath.Join(p.dir, rdbFilename)
	tmp := path + ".tmp"
	file, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("create RDB: %w", err)
	}
	if _, err = file.Write(body.Bytes()); err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(tmp, path)
	}
	if err == nil {
		err = syncDirectory(p.dir)
	}
	if err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("save RDB: %w", err)
	}

	p.dirty.Add(0 - dirty)
	p.lastSave.Store(time.Now().Unix())
	return nil
}

func (kv *KVStore) loadRDB(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if len(data) < len(rdbMagic)+8 || string(data[:len(rdbMagic)]) != rdbMagic {
		return fmt.Errorf("invalid RDB header")
	}
	body := data[:len(data)-8]
	want := binary.BigEndian.Uint64(data[len(data)-8:])
	if crc64.Checksum(body, rdbChecksumTable) != want {
		return fmt.Errorf("invalid RDB checksum")
	}
	var snapshot rdbSnapshot
	if err := gob.NewDecoder(bytes.NewReader(body[len(rdbMagic):])).Decode(&snapshot); err != nil {
		return fmt.Errorf("decode RDB: %w", err)
	}
	kv.restore(snapshot)
	return nil
}

// RewriteAOF compacts the log to the minimum commands needed for current state.
func (kv *KVStore) RewriteAOF() error {
	p := kv.persistence
	if p == nil {
		return fmt.Errorf("persistence is not enabled")
	}
	p.rewriteMu.Lock()
	defer p.rewriteMu.Unlock()
	kv.mu.Lock()
	defer kv.mu.Unlock()
	s := kv.snapshotLocked()
	commands := snapshotCommands(s)
	path := filepath.Join(p.dir, aofFilename)
	tmp := path + ".tmp"
	file, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("create rewritten AOF: %w", err)
	}
	for _, command := range commands {
		if err = writeRESP(file, command); err != nil {
			break
		}
	}
	if err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rewrite AOF: %w", err)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if err = p.aof.Close(); err == nil {
		err = os.Rename(tmp, path)
	}
	if err == nil {
		err = syncDirectory(p.dir)
	}
	newAOF, openErr := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if openErr == nil {
		p.aof = newAOF
		p.aofErr = nil
		if info, statErr := p.aof.Stat(); statErr == nil {
			p.baseSize.Store(info.Size())
		}
	}
	if err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("replace AOF: %w", err)
	}
	if openErr != nil {
		p.aofErr = fmt.Errorf("%w: reopen AOF: %v", utils.ErrStartup, openErr)
		return p.aofErr
	}
	return nil
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

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	err = dir.Sync()
	return errors.Join(err, dir.Close())
}
