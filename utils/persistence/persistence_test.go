package persistence

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDisableAutoPersistenceMaintenance(t *testing.T) {
	if os.Getenv("EMBERDB_PERSISTENCE") == "0" {
		t.Skip("persistence disabled")
	}
	t.Setenv("EMBERDB_DISABLE_AUTO_PERSISTENCE_MAINTENANCE", "1")
	dir := t.TempDir()
	p, err := Open(dir, Hooks[int]{
		Apply:    func([]string) error { return nil },
		Snapshot: func() int { return 0 },
		Restore:  func(int) {},
		Rewrite:  func(write func([][]string) error) error { return write(nil) },
		KeyCount: func() int { return 0 },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.Append("SET", "key", "value"); err != nil {
		t.Fatal(err)
	}
	p.dirty.Store(10_000)
	p.lastSave.Store(time.Now().Add(-time.Hour).Unix())
	time.Sleep(1200 * time.Millisecond)
	if _, err := os.Stat(filepath.Join(dir, RDBFilename)); !os.IsNotExist(err) {
		t.Fatalf("automatic snapshot ran with maintenance disabled: %v", err)
	}
	if data, err := os.ReadFile(filepath.Join(dir, AOFFilename)); err != nil || len(data) == 0 {
		t.Fatalf("AOF append stopped with maintenance disabled: %v", err)
	}
}

func TestOpenDisabled(t *testing.T) {
	t.Setenv("EMBERDB_PERSISTENCE", "0")
	dir := filepath.Join(t.TempDir(), "data")
	if _, err := Open(dir, Hooks[int]{}); err == nil {
		t.Fatal("opened persistence with EMBERDB_PERSISTENCE=0")
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("persistence directory exists: %v", err)
	}
}
