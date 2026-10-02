package doublebuffer

import (
	"context"
	"errors"
	"io"
	"os"
	"sync"
	"sync/atomic"
)

var bytesPool sync.Pool = sync.Pool{
	New: func() any {
		return []byte(nil)
	},
}

// WriteStats counts RESP buffer activity across all connections in this process.
// Counters are populated only when EMBERDB_RESP_WRITE_PROFILE=1.
type WriteStats struct {
	LogicalWrites uint64 `json:"logical_writes"`
	LogicalBytes  uint64 `json:"logical_bytes"`
	Wakeups       uint64 `json:"wakeups"`
	Flushes       uint64 `json:"flushes"`
	FlushBytes    uint64 `json:"flush_bytes"`
	SocketWrites  uint64 `json:"socket_writes"`
	SocketBytes   uint64 `json:"socket_bytes"`
}

var writeStats struct {
	logicalWrites atomic.Uint64
	logicalBytes  atomic.Uint64
	wakeups       atomic.Uint64
	flushes       atomic.Uint64
	flushBytes    atomic.Uint64
	socketWrites  atomic.Uint64
	socketBytes   atomic.Uint64
}

func Snapshot() WriteStats {
	return WriteStats{
		writeStats.logicalWrites.Load(), writeStats.logicalBytes.Load(),
		writeStats.wakeups.Load(), writeStats.flushes.Load(), writeStats.flushBytes.Load(),
		writeStats.socketWrites.Load(), writeStats.socketBytes.Load(),
	}
}

type DoubleBuffer struct {
	dataReadyFlag   chan struct{}
	bufferReadyFlag chan struct{}
	backBuffer      []byte
	bufferLimit     int
	bufferMutex     sync.Mutex
	context         context.Context
	cancel          context.CancelCauseFunc
	profile         bool
}

func NewWriterSize(wr io.Writer, size int) *DoubleBuffer {
	db := &DoubleBuffer{
		dataReadyFlag:   make(chan struct{}, 1),
		bufferReadyFlag: make(chan struct{}, 1),
		backBuffer:      bytesPool.Get().([]byte)[:0],
		bufferLimit:     size,
		profile:         os.Getenv("EMBERDB_RESP_WRITE_PROFILE") == "1",
	}
	db.context, db.cancel = context.WithCancelCause(context.Background())
	go db.flusher(wr)
	return db
}

func (db *DoubleBuffer) flusher(wr io.Writer) {
	defer func() {
		err := io.ErrClosedPipe
		if wc, ok := wr.(io.WriteCloser); ok && err != nil {
			err = wc.Close()
		}
		db.cancel(err)
	}()

	frontBuffer := bytesPool.Get().([]byte)
	defer func() { bytesPool.Put(frontBuffer) }()

	for range db.dataReadyFlag {
		if db.profile {
			writeStats.wakeups.Add(1)
		}
		db.bufferMutex.Lock()
		frontBuffer, db.backBuffer = db.backBuffer, frontBuffer[:0]
		db.bufferMutex.Unlock()
		select {
		case db.bufferReadyFlag <- struct{}{}:
		default:
		}
		if len(frontBuffer) == 0 {
			continue
		}
		if db.profile {
			writeStats.flushes.Add(1)
			writeStats.flushBytes.Add(uint64(len(frontBuffer)))
		}
		n, err := wr.Write(frontBuffer)
		if db.profile {
			writeStats.socketWrites.Add(1)
			writeStats.socketBytes.Add(uint64(n))
		}
		if err != nil {
			db.cancel(err)
			return
		}
	}
}

func (db *DoubleBuffer) Close() error {
	select {
	case <-db.context.Done():
		return context.Cause(db.context)
	default:
	}
	close(db.dataReadyFlag)
	<-db.context.Done()
	err := context.Cause(db.context)
	if errors.Is(err, io.ErrClosedPipe) {
		err = nil
	}
	bytesPool.Put(db.backBuffer)
	return err
}

func (db *DoubleBuffer) Write(p []byte) (n int, err error) {
	if db.profile {
		writeStats.logicalWrites.Add(1)
		defer func() { writeStats.logicalBytes.Add(uint64(n)) }()
	}
	if len(p) > db.bufferLimit {
		totalWritten := 0
		for len(p) > 0 {
			written, err := db.writeChunk(p[:min(db.bufferLimit, len(p))])
			totalWritten += written
			if err != nil {
				return totalWritten, err
			}
			p = p[written:]
		}
		return totalWritten, nil
	}
	return db.writeChunk(p)
}

func (db *DoubleBuffer) writeChunk(p []byte) (n int, err error) {
	for {
		select {
		case <-db.context.Done():
			return 0, context.Cause(db.context)
		default:
		}
		if len(p) == 0 {
			return 0, nil
		}
		db.bufferMutex.Lock()
		if len(db.backBuffer)+len(p) > db.bufferLimit {
			db.bufferMutex.Unlock()
			select {
			case <-db.context.Done():
				return 0, context.Cause(db.context)
			case <-db.bufferReadyFlag:
			}
			continue
		}
		db.backBuffer = append(db.backBuffer, p...)
		db.bufferMutex.Unlock()
		select {
		case db.dataReadyFlag <- struct{}{}:
		default:
		}
		return len(p), nil
	}
}

func (db *DoubleBuffer) Reset(wr io.Writer) {
	db.Close()
	db.backBuffer = db.backBuffer[:0]
	db.dataReadyFlag = make(chan struct{}, 1)
	db.context, db.cancel = context.WithCancelCause(context.Background())
	db.backBuffer = bytesPool.Get().([]byte)[:0]
	go db.flusher(wr)
}
