package sideload

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/ghodss/yaml"
	"github.com/influxdata/kapacitor/keyvalue"
	"github.com/influxdata/kapacitor/services/httpd"
	"github.com/influxdata/kapacitor/services/httppost"
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
	sources map[string]Source

	HTTPDService interface {
		AddRoutes([]httpd.Route) error
		DelRoutes([]httpd.Route)
	}
}

func NewService(d Diagnostic) *Service {
	return &Service{
		diag:    d,
		sources: make(map[string]Source),
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
		if err := src.UpdateCache(); err != nil {
			return errors.Wrapf(err, "failed to update source %q", dir)
		}
	}
	return nil
}

func (s *Service) Source(endpoint *httppost.Endpoint) (Source, error) {
	var src Source
	buf := &bytes.Buffer{}
	if err := endpoint.URL().Execute(buf, map[interface{}]string{}); err != nil {
		return nil, err
	}
	u, err := url.Parse(buf.String())
	if err != nil {
		return nil, err
	}
	if u.Scheme != "file" && u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("unsupported source scheme %q, must be 'file', 'http', or 'https'", u.Scheme)
	}

	if u.Scheme == "file" {
		src, err = s.sourceFile(u.Path)
	} else if u.Scheme == "http" || u.Scheme == "https" {
		src, err = s.sourceHttp(endpoint, u.Scheme)
	}

	return src, err
}

func (s *Service) sourceHttp(endpoint *httppost.Endpoint, scheme string) (Source, error) {
	var err error
	buf := &bytes.Buffer{}
	if err := endpoint.URL().Execute(buf, map[interface{}]string{}); err != nil {
		return nil, fmt.Errorf("Error creating request for sideload data from %s :: %s", buf.String(), err.Error())
	}

	dir := buf.String()
	s.mu.Lock()
	defer s.mu.Unlock()
	src, ok := s.sources[dir]
	if !ok {
		src = &httpSource{
			fileSource: fileSource{
				s:      s,
				dir:    dir,
				scheme: scheme,
			},
			e: endpoint,
		}
		err = src.UpdateCache()
		if err != nil {
			return nil, fmt.Errorf("Error fetching sideload data from %s :: %s", dir, err.Error())
		}
		s.sources[dir] = src
	}
	src.addToReferenceCount(1)

	return src, nil
}
func (s *Service) sourceFile(path string) (Source, error) {
	if !filepath.IsAbs(path) {
		return nil, fmt.Errorf("sideload source path must be absolute %q", path)
	}
	dir := filepath.Clean(path)
	s.mu.Lock()
	defer s.mu.Unlock()
	src, ok := s.sources[dir]
	if !ok {
		src = &fileSource{
			s:      s,
			dir:    dir,
			scheme: "file",
		}
		err := src.UpdateCache()
		if err != nil {
			return nil, err
		}

		s.sources[dir] = src
	}
	src.addToReferenceCount(1)

	return src, nil

}
func (s *Service) removeSource(src *fileSource) {
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
	UpdateCache() error
	addToReferenceCount(int) int
}

type fileSource struct {
	s              *Service
	dir            string
	mu             sync.RWMutex
	scheme         string
	cache          map[string]map[string]interface{}
	referenceCount int
}

func (s *fileSource) addToReferenceCount(i int) int {
	s.referenceCount += i
	return s.referenceCount
}

func (s *fileSource) Close() {
	s.s.removeSource(s)
}

func (s *fileSource) UpdateCache() error {
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
	return errors.Wrapf(err, "failed to update sideload cache for source file %q", s.dir)
}

func (s *fileSource) Lookup(order []string, key string) (value interface{}) {
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

type httpSource struct {
	fileSource
	e *httppost.Endpoint
}

func (s *httpSource) UpdateCache() error {
	s.fileSource.mu.Lock()
	defer s.fileSource.mu.Unlock()
	s.fileSource.cache = make(map[string]map[string]interface{})

	req, err := http.NewRequest("GET", s.dir, nil)
	if err != nil {
		return errors.Wrapf(err, "failed to generate request to update sideload cache for source %q", s.dir)
	}
	if s.e.Auth.Username != "" && s.e.Auth.Password != "" {
		req.SetBasicAuth(s.e.Auth.Username, s.e.Auth.Password)
	}

	client := &http.Client{
		Timeout: time.Second * 10,
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	values, err := loadValues(resp.Body)
	if err != nil {
		return errors.Wrapf(err, "failed to load body to update sideload cache for source %q", s.dir)
	}
	for k, v := range values {
		s.fileSource.cache[k] = v
	}
	return nil
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

func loadValues(resp io.ReadCloser) (map[string]map[string]interface{}, error) {
	data, err := ioutil.ReadAll(resp)
	if err != nil {
		return nil, errors.Wrapf(err, "Failed to read response body")
	}
	values := make(map[string]map[string]interface{})
	if err := json.Unmarshal(data, &values); err != nil {
		return nil, errors.Wrapf(err, "failed to unmarshal json values in response body")
	}

	return values, nil
}
