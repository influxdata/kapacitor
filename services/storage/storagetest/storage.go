package storagetest

import "github.com/influxdata/kapacitor/services/storage"

type TestStore struct{}

func New() TestStore {
	return TestStore{}
}

func (s TestStore) Store(name string) storage.Interface {
	return storage.NewMemStore(name)
}
