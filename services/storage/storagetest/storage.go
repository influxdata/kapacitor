package storagetest

import (
	"fmt"
	"os"
	"path"

	"github.com/influxdata/kapacitor/services/alert"
	"github.com/influxdata/kapacitor/services/storage"
	bolt "go.etcd.io/bbolt"
)

type CleanedTest interface {
	TempDir() string
}

type TestStore struct {
	db         *BoltDB
	versions   storage.Versions
	registrar  *storage.StoreActionerRegistrar
	diagnostic storage.Diagnostic
}

// BoltDB is a database that deletes itself when closed
type BoltDB struct {
	*bolt.DB
}

// NewBolt is an in-memory db that deletes itself when closed, do not use except for testing.
func NewBolt(t CleanedTest) (*BoltDB, error) {
	dir := t.TempDir()
	f, err := os.CreateTemp(dir, "boltDB*.db")
	if err != nil {
		return nil, err
	}
	dbName := f.Name()
	if err = f.Close(); err != nil {
		return nil, err
	}
	db, err := bolt.Open(dbName, 0600, &bolt.Options{
		Timeout:    0,
		NoGrowSync: false,
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
	dbPath := b.Path()
	err := b.DB.Close()
	if err != nil {
		return err
	}
	return os.RemoveAll(path.Dir(dbPath))
}

func New(t CleanedTest, diagnostic storage.Diagnostic) *TestStore {
	db, err := NewBolt(t)
	if err != nil {
		panic(err)
	}
	return &TestStore{
		db:         db,
		versions:   storage.NewVersions(db.Store("versions")),
		registrar:  storage.NewStorageRegistrar(),
		diagnostic: diagnostic,
	}
}

func (s *TestStore) Store(name string) storage.Interface {
	return s.db.Store(name)
}

func (s *TestStore) Versions() storage.Versions {
	return s.versions
}

func (s *TestStore) Register(name string, store storage.StoreActioner) {
	s.registrar.Register(name, store)
}

func (s *TestStore) Close() error {
	return s.db.Close()
}

func (s *TestStore) Diagnostic() storage.Diagnostic {
	return s.diagnostic
}

func (s *TestStore) BucketEntries(topic string, alertID string) (keys []string, exists bool, err error) {
	store := s.db.Store(alert.TopicStatesNameSpace)
	err = store.View(func(tx storage.ReadOnlyTx) error {
		bucket := tx.Bucket([]byte(topic))
		if bucket == nil {
			return fmt.Errorf("%q: %w", topic, bolt.ErrBucketNotFound)
		}
		if kvs, err := bucket.List(""); err != nil {
			return fmt.Errorf("failed to list contents of bucket %q: %w", topic, err)
		} else {
			keys = make([]string, 0, len(kvs))
			for _, aID := range kvs {
				keys = append(keys, aID.Key)
				if aID.Key == alertID {
					exists = true
				}
			}
		}
		return nil
	})
	return keys, exists, err
}
