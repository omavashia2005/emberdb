package kvstore

import (
	"fmt"
	"os"

	"github.com/omavashia2005/emberdb/utils/persistence"
)

// OpenPersistent restores the store and enables AOF and RDB persistence unless EMBERDB_PERSISTENCE=0.
func OpenPersistent(dir string, clusterEnabled ...bool) (*KVStore, error) {
	kv := NewKVStore(clusterEnabled...)
	if os.Getenv("EMBERDB_PERSISTENCE") == "0" {
		return kv, nil
	}
	p, err := persistence.Open(dir, persistence.Hooks[rdbSnapshot]{
		Apply:    kv.applyAOF,
		Snapshot: kv.snapshot,
		Restore:  kv.restore,
		Rewrite:  kv.rewriteSnapshot,
		KeyCount: kv.keyCount,
	})
	if err != nil {
		return nil, err
	}
	kv.persistence = p
	return kv, nil
}

func (kv *KVStore) PersistenceEnabled() bool {
	return kv.persistence != nil
}

func (kv *KVStore) Close() error {
	if kv.persistence == nil {
		return nil
	}
	return kv.persistence.Close()
}

func (kv *KVStore) persist(args ...string) error {
	if kv.persistence == nil {
		return nil
	}
	return kv.persistence.Append(args...)
}

func (kv *KVStore) SaveRDB() error {
	if kv.persistence == nil {
		return fmt.Errorf("persistence is not enabled")
	}
	return kv.persistence.SaveRDB()
}

func (kv *KVStore) RewriteAOF() error {
	if kv.persistence == nil {
		return fmt.Errorf("persistence is not enabled")
	}
	return kv.persistence.RewriteAOF()
}

func (kv *KVStore) rewriteSnapshot(write func([][]string) error) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return write(snapshotCommands(kv.snapshotLocked()))
}
