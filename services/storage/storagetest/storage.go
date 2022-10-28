package storagetest

import (
	"os"
	"path"

	"github.com/influxdata/kapacitor/services/storage"
	bolt "go.etcd.io/bbolt"
)

type TestStore struct {
	db        *BoltDB
	versions  storage.Versions
	registrar *storage.StoreActionerRegistrar
}

// BoltDB is a database that deletes itself when closed
type BoltDB struct {
	*bolt.DB
}

// NewBolt is an in-memory db that deletes itself when closed, do not use except for testing.
func NewBolt() (*BoltDB, error) {
	db, err := bolt.Open(":memory:", 0600, &bolt.Options{
		Timeout:    0,
		NoGrowSync: false,
		MemOnly:    true,
	})
	if err != nil {
		return nil, err
	}
	return &BoltDB{db}, nil
}

func (b BoltDB) Store(bucket string) storage.Interface {
	return storage.NewBolt(b.DB, []byte(bucket))
}

func (b BoltDB) Close() error {
	err := b.DB.Close()
	if err != nil {
		return err
	}
	return os.RemoveAll(path.Dir(b.Path()))
}

func New() TestStore {
	db, err := NewBolt()
	if err != nil {
		panic(err)
	}
	return TestStore{
		db:        db,
		versions:  storage.NewVersions(db.Store("versions")),
		registrar: storage.NewStorageResitrar(),
	}
}

func (s TestStore) Store(name string) storage.Interface {
	return s.db.Store(name)
}

func (s TestStore) Versions() storage.Versions {
	return s.versions
}

func (s TestStore) Register(name string, store storage.StoreActioner) {
	s.registrar.Register(name, store)
}

func (s TestStore) Close() {
	//s.db.Close()
}
