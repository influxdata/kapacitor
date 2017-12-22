package storagetest

import "github.com/influxdata/kapacitor/services/storage"

type TestStore struct {
	versions  storage.Versions
	registrar storage.StoreActionerRegistrar
}

func New() TestStore {
	return TestStore{
		versions:  storage.NewVersions(storage.NewMemStore("versions")),
		registrar: storage.NewStorageResitrar(),
	}
}

func (s TestStore) Store(name string) storage.Interface {
	return storage.NewMemStore(name)
}

func (s TestStore) Versions() storage.Versions {
	return s.versions
}
func (s TestStore) Register(name string, store storage.StoreActioner) {
	s.registrar.Register(name, store)
}
