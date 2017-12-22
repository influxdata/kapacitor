package storage

import (
	"os"
	"path"
	"sync"

	"github.com/boltdb/bolt"
	"github.com/influxdata/kapacitor/services/httpd"
	"github.com/pkg/errors"
)

type Diagnostic interface {
	Error(msg string, err error)
}

type Service struct {
	dbpath string

	boltdb *bolt.DB
	stores map[string]Interface
	mu     sync.Mutex

	registrar StoreActionerRegistrar
	apiServer *APIServer

	versions Versions

	HTTPDService interface {
		AddRoutes([]httpd.Route) error
		DelRoutes([]httpd.Route)
	}

	diag Diagnostic
}

func NewService(conf Config, d Diagnostic) *Service {
	return &Service{
		dbpath: conf.BoltDBPath,
		diag:   d,
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

	s.registrar = NewStorageResitrar()
	s.apiServer = &APIServer{
		DB:           s.boltdb,
		Registrar:    s.registrar,
		HTTPDService: s.HTTPDService,
		diag:         s.diag,
	}

	if err := s.apiServer.Open(); err != nil {
		return err
	}

	s.versions = NewVersions(s.store(versionsNamespace))

	return nil
}

func (s *Service) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.apiServer != nil {
		if err := s.apiServer.Close(); err != nil {
			return err
		}
	}
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

func (s *Service) Register(name string, store StoreActioner) {
	s.registrar.Register(name, store)
}
