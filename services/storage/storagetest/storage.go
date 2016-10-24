package storagetest

import (
	"strings"
	"sync"

	"github.com/influxdata/kapacitor/services/storage"
)

type TestStore struct{}

func New() TestStore {
	return TestStore{}
}

func (s TestStore) Store(name string) storage.Interface {
	return NewMemStore(name)
}

// Common interface for interacting with a simple Key/Value storage
type MemStore struct {
	sync.Mutex
	Name  string
	store map[string][]byte
}

func NewMemStore(name string) *MemStore {
	return &MemStore{
		Name:  name,
		store: make(map[string][]byte),
	}
}

func (s *MemStore) Put(key string, value []byte) error {
	s.Lock()
	s.store[key] = value
	s.Unlock()
	return nil
}

func (s *MemStore) Get(key string) (*storage.KeyValue, error) {
	s.Lock()
	value, ok := s.store[key]
	s.Unlock()
	if !ok {
		return nil, storage.ErrNoKeyExists
	}
	return &storage.KeyValue{
		Key:   key,
		Value: value,
	}, nil
}

func (s *MemStore) Delete(key string) error {
	s.Lock()
	delete(s.store, key)
	s.Unlock()
	return nil
}

func (s *MemStore) Exists(key string) (bool, error) {
	s.Lock()
	_, ok := s.store[key]
	s.Unlock()
	return ok, nil
}

func (s *MemStore) List(prefix string) ([]*storage.KeyValue, error) {
	s.Lock()
	kvs := make([]*storage.KeyValue, 0, len(s.store))
	for k, v := range s.store {
		if strings.HasPrefix(k, prefix) {
			kvs = append(kvs, &storage.KeyValue{Key: k, Value: v})
		}
	}
	s.Unlock()
	return kvs, nil
}
