package storage

import (
	"log"
	"os"
	"path"
	"sync"

	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
)

type Service struct {
	dbpath string

	boltdb *bolt.DB
	stores map[string]Interface
	mu     sync.Mutex

	versions Versions

	logger *log.Logger
}

func NewService(conf Config, l *log.Logger) *Service {
	return &Service{
		dbpath: conf.BoltDBPath,
		logger: l,
		stores: make(map[string]Interface),
	}
}

const (
	versionsNamespace = "versions"
)

func (s *Service) Open() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := os.MkdirAll(path.Dir(s.dbpath), 0755)
	if err != nil {
		return errors.Wrapf(err, "mkdir dirs %q", s.dbpath)
	}
	db, err := bolt.Open(s.dbpath, 0600, nil)
	if err != nil {
		return errors.Wrapf(err, "open boltdb @ %q", s.dbpath)
	}
	s.boltdb = db

	s.versions = NewVersions(s.store(versionsNamespace))

	return nil
}

func (s *Service) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.boltdb != nil {
		return s.boltdb.Close()
	}
	return nil
}

// Return a namespaced store.
// Calling Store with the same namespace returns the same Store.
func (s *Service) Store(name string) Interface {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.store(name)
}

func (s *Service) store(name string) Interface {
	if store, ok := s.stores[name]; ok {
		return store
	} else {
		store = NewBolt(s.boltdb, name)
		s.stores[name] = store
		return store
	}
}

func (s *Service) Versions() Versions {
	return s.versions
}
