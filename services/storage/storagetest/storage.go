package storagetest

import "github.com/influxdata/kapacitor/services/storage"

type TestStore struct {
	versions storage.Versions
}

func New() TestStore {
	return TestStore{
		versions: storage.NewVersions(storage.NewMemStore("versions")),
	}
}

func (s TestStore) Store(name string) storage.Interface {
	return storage.NewMemStore(name)
}

func (s TestStore) Versions() storage.Versions {
	return s.versions
}
