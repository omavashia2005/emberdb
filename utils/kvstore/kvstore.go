package kvstore

import (
	"net"
	"sync"
	"time"
)

type KVStore struct {
	Strings                map[string]string
	Expirations            map[string]time.Time
	mu                     sync.RWMutex
	totalCommandsProcessed int
	connectedClients       map[string]net.Conn
}

func NewKVStore() *KVStore {
	return &KVStore{
		Strings:                make(map[string]string),
		Expirations:            make(map[string]time.Time),
		totalCommandsProcessed: 0,
		connectedClients:       make(map[string]net.Conn),
	}
}


func (kv *KVStore) Set (key, value string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.Strings[key] = value;
}


func (kv *KVStore) Get (key string) string{

	kv.mu.RLock()
	defer kv.mu.RUnlock()

	if val, ok := kv.Strings[key]; ok{
		return val
	}

	return "(nil)"


}
