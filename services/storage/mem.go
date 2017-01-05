package storage

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

// MemStore is an in memory only implementation of the storage.Interface.
// This is intend to be used for testing use cases only.
type MemStore struct {
	mu    sync.Mutex
	Name  string
	store map[string][]byte
}

func NewMemStore(name string) *MemStore {
	return &MemStore{
		Name:  name,
		store: make(map[string][]byte),
	}
}

func (s *MemStore) View(f func(tx ReadOnlyTx) error) error {
	return DoView(s, f)
}

func (s *MemStore) Update(f func(tx Tx) error) error {
	return DoUpdate(s, f)
}

func (s *MemStore) Put(key string, value []byte) error {
	s.mu.Lock()
	s.store[key] = value
	s.mu.Unlock()
	return nil
}

func (s *MemStore) Get(key string) (*KeyValue, error) {
	s.mu.Lock()
	value, ok := s.store[key]
	s.mu.Unlock()
	if !ok {
		return nil, ErrNoKeyExists
	}
	return &KeyValue{
		Key:   key,
		Value: value,
	}, nil
}

func (s *MemStore) Delete(key string) error {
	s.mu.Lock()
	delete(s.store, key)
	s.mu.Unlock()
	return nil
}

func (s *MemStore) Exists(key string) (bool, error) {
	s.mu.Lock()
	_, ok := s.store[key]
	s.mu.Unlock()
	return ok, nil
}

type keySortedKVs []*KeyValue

func (s keySortedKVs) Len() int               { return len(s) }
func (s keySortedKVs) Less(i int, j int) bool { return s[i].Key < s[j].Key }
func (s keySortedKVs) Swap(i int, j int)      { s[i], s[j] = s[j], s[i] }

func (s *MemStore) List(prefix string) ([]*KeyValue, error) {
	s.mu.Lock()
	kvs := make([]*KeyValue, 0, len(s.store))
	for k, v := range s.store {
		if strings.HasPrefix(k, prefix) {
			kvs = append(kvs, &KeyValue{Key: k, Value: v})
		}
	}
	s.mu.Unlock()
	sort.Sort(keySortedKVs(kvs))
	return kvs, nil
}

func (s *MemStore) BeginTx() (Tx, error) {
	return s.newTx()
}

func (s *MemStore) BeginReadOnlyTx() (ReadOnlyTx, error) {
	return s.newTx()
}

func (s *MemStore) newTx() (*memTx, error) {
	// A Tx carries the lock, and must be committed or rolledback before another operation can continue.
	s.mu.Lock()
	store := make(map[string][]byte, len(s.store))
	for k, v := range s.store {
		store[k] = v
	}
	return &memTx{
		m:     s,
		store: store,
	}, nil
}

type memTxState int

const (
	unCommitted memTxState = iota
	committed
	rolledback
)

type memTx struct {
	state memTxState
	m     *MemStore
	store map[string][]byte
}

func (t *memTx) Get(key string) (*KeyValue, error) {
	value, ok := t.store[key]
	if !ok {
		return nil, ErrNoKeyExists
	}
	return &KeyValue{Key: key, Value: value}, nil
}

func (t *memTx) Exists(key string) (bool, error) {
	_, ok := t.store[key]
	return ok, nil
}

func (t *memTx) List(prefix string) ([]*KeyValue, error) {
	kvs := make([]*KeyValue, 0, len(t.store))
	for k, v := range t.store {
		if strings.HasPrefix(k, prefix) {
			kvs = append(kvs, &KeyValue{Key: k, Value: v})
		}
	}
	sort.Sort(keySortedKVs(kvs))
	return kvs, nil
}

func (t *memTx) Put(key string, value []byte) error {
	t.store[key] = value
	return nil
}

func (t *memTx) Delete(key string) error {
	delete(t.store, key)
	return nil
}

func (t *memTx) Commit() error {
	if t.state == unCommitted {
		t.m.store = t.store
		t.state = committed
		t.m.mu.Unlock()
		return nil
	}
	return fmt.Errorf("cannot commit transaction, transaction in state %v", t.state)
}

func (t *memTx) Rollback() error {
	if t.state == unCommitted {
		t.state = rolledback
		t.m.mu.Unlock()
	}
	return nil
}
