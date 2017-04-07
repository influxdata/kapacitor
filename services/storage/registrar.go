package storage

import "sync"

// StoreActioner exposes and interface for various actions that can be performed on a store.
type StoreActioner interface {
	// Rebuild the entire store, this should be considered to be an expensive action.
	Rebuild() error
}

type StoreActionerRegistrar interface {
	List() []string
	Register(name string, store StoreActioner)
	Get(name string) (StoreActioner, bool)
}

func NewStorageResitrar() StoreActionerRegistrar {
	return &storeActionerRegistrar{
		stores: make(map[string]StoreActioner),
	}
}

type storeActionerRegistrar struct {
	mu     sync.RWMutex
	stores map[string]StoreActioner
}

func (sr *storeActionerRegistrar) List() []string {
	sr.mu.RLock()
	defer sr.mu.RUnlock()
	list := make([]string, 0, len(sr.stores))
	for name := range sr.stores {
		list = append(list, name)
	}
	return list
}

func (sr *storeActionerRegistrar) Register(name string, store StoreActioner) {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	sr.stores[name] = store
}

func (sr *storeActionerRegistrar) Get(name string) (store StoreActioner, ok bool) {
	sr.mu.RLock()
	defer sr.mu.RUnlock()
	store, ok = sr.stores[name]
	return
}
