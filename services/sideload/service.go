package sideload

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sync"

	"github.com/ghodss/yaml"
	"github.com/influxdata/kapacitor/keyvalue"
	"github.com/influxdata/kapacitor/services/httpd"
	"github.com/pkg/errors"
)

const (
	reloadPath = "/sideload/reload"
	basePath   = httpd.BasePath + reloadPath
)

type Diagnostic interface {
	WithContext(ctx ...keyvalue.T) Diagnostic

	Error(msg string, err error)
}

type Service struct {
	diag   Diagnostic
	routes []httpd.Route

	mu      sync.Mutex
	sources map[string]*source

	HTTPDService interface {
		AddRoutes([]httpd.Route) error
		DelRoutes([]httpd.Route)
	}
}

func NewService(d Diagnostic) *Service {
	return &Service{
		diag:    d,
		sources: make(map[string]*source),
	}
}

func (s *Service) Open() error {
	// Define API routes
	s.routes = []httpd.Route{
		{
			Method:      "POST",
			Pattern:     reloadPath,
			HandlerFunc: s.handleReload,
		},
	}

	err := s.HTTPDService.AddRoutes(s.routes)
	return errors.Wrap(err, "failed to add API routes")
}
func (s *Service) Close() error {
	s.HTTPDService.DelRoutes(s.routes)
	return nil
}

func (s *Service) handleReload(w http.ResponseWriter, r *http.Request) {
	err := s.Reload()
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Service) Reload() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for dir, src := range s.sources {
		if err := src.updateCache(); err != nil {
			return errors.Wrapf(err, "failed to update source %q", dir)
		}
	}
	return nil
}

func (s *Service) Source(srcURL string) (Source, error) {
	u, err := url.Parse(srcURL)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "file" {
		return nil, fmt.Errorf("unsupported source scheme %q, must be 'file'", u.Scheme)
	}
	if !filepath.IsAbs(u.Path) {
		return nil, fmt.Errorf("sideload source path must be absolute %q", u.Path)
	}
	dir := filepath.Clean(u.Path)
	s.mu.Lock()
	defer s.mu.Unlock()

	src, ok := s.sources[dir]
	if !ok {
		src = &source{
			s:   s,
			dir: dir,
		}
		err := src.updateCache()
		if err != nil {
			return nil, err
		}
		s.sources[dir] = src
	}
	src.referenceCount++

	return src, nil
}

func (s *Service) removeSource(src *source) {
	s.mu.Lock()
	defer s.mu.Unlock()
	src.referenceCount--
	if src.referenceCount == 0 {
		delete(s.sources, src.dir)
	}
}

type Source interface {
	Lookup(order []string, key string) interface{}
	Close()
}

type source struct {
	s              *Service
	dir            string
	mu             sync.RWMutex
	cache          map[string]map[string]interface{}
	referenceCount int
}

func (s *source) Close() {
	s.s.removeSource(s)
}

func (s *source) updateCache() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cache = make(map[string]map[string]interface{})
	err := filepath.Walk(s.dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		values, err := readValues(path)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(s.dir, path)
		if err != nil {
			return err
		}
		// The relative path must be a child of s.dir.
		// If it starts with '.' then it is either outside of s.dir or equal to s.dir,
		// both cases are invalid.
		if len(rel) == 0 || rel[0] == '.' {
			return errors.New("invalid relative path")
		}
		s.cache[rel] = values
		return nil
	})
	return errors.Wrapf(err, "failed to update sideload cache for source %q", s.dir)
}

func (s *source) Lookup(order []string, key string) (value interface{}) {
	key = filepath.Clean(key)

	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, o := range order {
		values, ok := s.cache[o]
		if !ok {
			continue
		}
		v, ok := values[key]
		if !ok {
			continue
		}
		value = v
		break
	}
	return
}

func readValues(p string) (map[string]interface{}, error) {
	f, err := os.Open(p)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to open values file %q", p)
	}
	defer f.Close()
	data, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read values file %q", p)
	}

	values := make(map[string]interface{})
	ext := filepath.Ext(p)
	switch ext {
	case ".yaml", ".yml":
		if err := yaml.Unmarshal(data, &values); err != nil {
			return nil, errors.Wrapf(err, "failed to unmarshal yaml values %q", p)
		}
	case ".json":
		if err := json.Unmarshal(data, &values); err != nil {
			return nil, errors.Wrapf(err, "failed to unmarshal json values %q", p)
		}
	}
	return values, nil
}
