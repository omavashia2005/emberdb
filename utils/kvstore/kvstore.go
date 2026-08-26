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
