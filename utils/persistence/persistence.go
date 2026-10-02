package persistence

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
	"sync"
	"sync/atomic"
	"time"

	resp "github.com/Fusl/go-resp"
	"github.com/bytechan/resp3"
	"github.com/omavashia2005/emberdb/utils"
)

const (
	RDBFilename = "dump.rdb"
	AOFFilename = "appendonly.aof"
	rdbMagic    = "EMBERDB001"
)

var rdbChecksumTable = crc64.MakeTable(crc64.ECMA)

// Hooks connect the file format and maintenance to the owning data store.
type Hooks[S any] struct {
	Apply    func([]string) error
	Snapshot func() S
	Restore  func(S)
	Rewrite  func(func([][]string) error) error
	KeyCount func() int
}

type Store[S any] struct {
	dir       string
	aof       *os.File
	hooks     Hooks[S]
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
	closeOnce sync.Once
	closeErr  error
}

// Open loads AOF when present (otherwise RDB), then enables periodic sync and maintenance.
func Open[S any](dir string, hooks Hooks[S]) (*Store[S], error) {
	if os.Getenv("EMBERDB_PERSISTENCE") == "0" {
		return nil, fmt.Errorf("persistence is not enabled")
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create persistence directory: %w", err)
	}
	p := &Store[S]{dir: dir, hooks: hooks, stop: make(chan struct{}), done: make(chan struct{})}
	p.lastSave.Store(time.Now().Unix())

	aofPath := filepath.Join(dir, AOFFilename)
	aofData, err := os.ReadFile(aofPath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("read AOF: %w", err)
	}
	if len(aofData) > 0 {
		valid, err := replayAOF(aofData, hooks.Apply)
		if err != nil {
			return nil, err
		}
		if valid != len(aofData) {
			if err := os.Truncate(aofPath, int64(valid)); err != nil {
				return nil, fmt.Errorf("truncate incomplete AOF tail: %w", err)
			}
		}
	} else if err := p.loadRDB(filepath.Join(dir, RDBFilename)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	p.aof, err = os.OpenFile(aofPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open AOF: %w", err)
	}
	if info, statErr := p.aof.Stat(); statErr == nil {
		p.baseSize.Store(info.Size())
	}
	// AOF becomes authoritative after loading a snapshot.
	if len(aofData) == 0 && hooks.KeyCount() > 0 {
		if err := p.RewriteAOF(); err != nil {
			_ = p.aof.Close()
			return nil, err
		}
	}
	go p.loop()
	return p, nil
}

// Close stops maintenance, writes a final snapshot, and flushes the AOF.
func (p *Store[S]) Close() error {
	p.closeOnce.Do(func() {
		close(p.stop)
		<-p.done
		rdbErr := p.SaveRDB()
		p.mu.Lock()
		previousAOFErr := p.aofErr
		aofErr := p.aof.Sync()
		closeErr := p.aof.Close()
		p.aofErr = os.ErrClosed
		p.mu.Unlock()
		p.closeErr = errors.Join(rdbErr, previousAOFErr, aofErr, closeErr)
	})
	return p.closeErr
}

func (p *Store[S]) loop() {
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
			// Benchmarks keep append/fsync active without full-dataset maintenance.
			if os.Getenv("EMBERDB_DISABLE_AUTO_PERSISTENCE_MAINTENANCE") == "1" {
				continue
			}
			dirty := p.dirty.Load()
			elapsed := time.Now().Unix() - p.lastSave.Load()
			if dirty >= 10_000 && elapsed >= 60 || dirty >= 100 && elapsed >= 300 || dirty >= 1 && elapsed >= 3600 {
				_ = p.SaveRDB()
			}
			if statErr == nil && info.Size() >= 64<<20 && info.Size() >= p.baseSize.Load()*2 {
				if err := p.RewriteAOF(); err != nil {
					utils.PrintError(err)
				}
			}
		case <-p.stop:
			return
		}
	}
}

func (p *Store[S]) Append(args ...string) error {
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
	var b bytes.Buffer
	writer := resp.NewServer(&b)
	if err := writer.WriteArrayString(args); err != nil {
		_ = writer.Close()
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}
	n, err := w.Write(b.Bytes())
	if err == nil && n != b.Len() {
		err = io.ErrShortWrite
	}
	return err
}

func replayAOF(data []byte, apply func([]string) error) (int, error) {
	source := bytes.NewReader(data)
	reader := resp3.NewReader(source)
	offset := 0
	for offset < len(data) {
		value, _, err := reader.ReadValue()
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return offset, nil
		}
		if err != nil {
			return offset, fmt.Errorf("read AOF at byte %d: %w", offset, err)
		}
		if value == nil || value.Type != resp3.TypeArray || len(value.Elems) == 0 {
			return offset, fmt.Errorf("invalid AOF command at byte %d", offset)
		}
		args := make([]string, len(value.Elems))
		for i, element := range value.Elems {
			if element == nil || element.Type != resp3.TypeBlobString {
				return offset, fmt.Errorf("invalid AOF argument at byte %d", offset)
			}
			args[i] = element.Str
		}
		if err := apply(args); err != nil {
			return offset, fmt.Errorf("replay AOF at byte %d: %w", offset, err)
		}
		offset = len(data) - source.Len() - reader.Buffered()
	}
	return offset, nil
}

// SaveRDB atomically replaces dump.rdb with a checksummed snapshot.
func (p *Store[S]) SaveRDB() (result error) {
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
	if err := gob.NewEncoder(&body).Encode(p.hooks.Snapshot()); err != nil {
		return fmt.Errorf("encode RDB: %w", err)
	}
	checksum := crc64.Checksum(body.Bytes(), rdbChecksumTable)
	if err := binary.Write(&body, binary.BigEndian, checksum); err != nil {
		return fmt.Errorf("checksum RDB: %w", err)
	}
	path := filepath.Join(p.dir, RDBFilename)
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

func (p *Store[S]) loadRDB(path string) error {
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
	var snapshot S
	if err := gob.NewDecoder(bytes.NewReader(body[len(rdbMagic):])).Decode(&snapshot); err != nil {
		return fmt.Errorf("decode RDB: %w", err)
	}
	p.hooks.Restore(snapshot)
	return nil
}

// RewriteAOF compacts the log to commands generated from the current snapshot.
func (p *Store[S]) RewriteAOF() error {
	p.rewriteMu.Lock()
	defer p.rewriteMu.Unlock()
	return p.hooks.Rewrite(p.rewriteAOF)
}

func (p *Store[S]) rewriteAOF(commands [][]string) error {
	path := filepath.Join(p.dir, AOFFilename)
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

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	err = dir.Sync()
	return errors.Join(err, dir.Close())
}
