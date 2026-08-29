package kvstore

import (
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

)

type KVStore struct {
	Strings           map[string]string
	Expirations       map[string]time.Time
	mu                sync.RWMutex
	CommandsProcessed int
	Clients           map[string]net.Conn
}

func NewKVStore() *KVStore {
	return &KVStore{
		Strings:           make(map[string]string),
		Expirations:       make(map[string]time.Time),
		CommandsProcessed: 0,
		Clients:           make(map[string]net.Conn),
	}
}

func (kv *KVStore) Set(key, value string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.Strings[key] = value
}

func (kv *KVStore) Get(key string) string {

	kv.mu.RLock()
	defer kv.mu.RUnlock()

	if val, ok := kv.Strings[key]; ok {
		return val
	}

	return "(nil)"

}

func (kv *KVStore) Append(key, valueToAppend string) error {

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if val, ok := kv.Strings[key]; ok {
		kv.Strings[key] = val + valueToAppend
		return nil
	}

	return fmt.Errorf("ERR No such value found")
}

func (kv *KVStore) Incr(key string) error {

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if val, ok := kv.Strings[key]; ok {
		intValue, err := strconv.Atoi(val)
		if err != nil {
			return fmt.Errorf("ERR value is not an integer")
		}

		kv.Strings[key] = strconv.Itoa(intValue + 1)

		return nil
	}

	return fmt.Errorf("ERR No such key found")
}

func (kv *KVStore) IncrBy(key, incrByVal string) error {

	kv.mu.Lock()
	defer kv.mu.Unlock()

	incrByValInt, err := strconv.Atoi(incrByVal)
	if err != nil {
		return fmt.Errorf("ERR INCRBY val is not an integer")
	}

	if val, ok := kv.Strings[key]; ok {
		intValue, err := strconv.Atoi(val)
		if err != nil {
			return fmt.Errorf("ERR value is not an integer")
		}

		kv.Strings[key] = strconv.Itoa(intValue + incrByValInt)

		return nil
	}

	return fmt.Errorf("ERR No such key found")
}

func (kv *KVStore) Decr(key string) error {

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if val, ok := kv.Strings[key]; ok {
		intValue, err := strconv.Atoi(val)
		if err != nil {
			return fmt.Errorf("ERR value is not an integer")
		}

		kv.Strings[key] = strconv.Itoa(intValue - 1)

		return nil
	}

	return fmt.Errorf("ERR No such key found")
}

func (kv *KVStore) DecrBy(key, decrByVal string) error {

	kv.mu.Lock()
	defer kv.mu.Unlock()

	decrByValInt, err := strconv.Atoi(decrByVal)
	if err != nil {
		return fmt.Errorf("ERR INCRBY val is not an integer")
	}

	if val, ok := kv.Strings[key]; ok {
		intValue, err := strconv.Atoi(val)
		if err != nil {
			return fmt.Errorf("ERR value is not an integer")
		}

		kv.Strings[key] = strconv.Itoa(intValue - decrByValInt)

		return nil
	}

	return fmt.Errorf("ERR No such key found")
}

func (kv *KVStore) FlushAll() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.Strings = make(map[string]string)

}

