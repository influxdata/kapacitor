package servicetest

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strings"

	"github.com/influxdata/kapacitor/services/httpd"
	"github.com/pkg/errors"
)

const (
	testPath         = "/tests"
	testPathAnchored = "/tests/"
	basePath         = httpd.BasePath + testPathAnchored
)

type Tester interface {
	// DefaultOptions returns a object.
	// User specified data will be JSON decoded into the object.
	// The object will be JSON encoded to provide as an example to the user of available options.
	TestOptions() interface{}
	// Test a service with the provided options.
	Test(options interface{}) error
}

type Service struct {
	testers map[string]Tester
	routes  []httpd.Route

	HTTPDService interface {
		AddRoutes([]httpd.Route) error
		DelRoutes([]httpd.Route)
	}
}

func NewService(c Config, l *log.Logger) *Service {
	return &Service{
		testers: make(map[string]Tester),
	}
}

func (s *Service) Open() error {
	// Define API routes
	s.routes = []httpd.Route{
		{
			Name:        "tests-list",
			Method:      "GET",
			Pattern:     testPath,
			HandlerFunc: s.handleListTests,
		},
		{
			Name:        "tests-options",
			Method:      "GET",
			Pattern:     testPathAnchored,
			HandlerFunc: s.handleTestOptions,
		},
		{
			Name:        "do-test",
			Method:      "POST",
			Pattern:     testPathAnchored,
			HandlerFunc: s.handleTest,
		},
	}

	err := s.HTTPDService.AddRoutes(s.routes)
	return errors.Wrap(err, "failed to add API routes")
}

func (s *Service) Close() error {
	s.HTTPDService.DelRoutes(s.routes)
	return nil
}

func (s *Service) AddTester(name string, t Tester) error {
	if _, ok := s.testers[name]; ok {
		return fmt.Errorf("tester with name %q already exists", name)
	}
	s.testers[name] = t
	return nil
}

func (s *Service) nameFromPath(p string) string {
	return strings.TrimRight(strings.TrimPrefix(p, basePath), "/")
}

type testResult struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

func (s *Service) handleListTests(w http.ResponseWriter, r *http.Request) {
	tests := make([]string, 0, len(s.testers))
	for name := range s.testers {
		tests = append(tests, name)
	}
	sort.Strings(tests)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(tests)
}

func (s *Service) handleTestOptions(w http.ResponseWriter, r *http.Request) {
	name := s.nameFromPath(r.URL.Path)
	if name == "" {
		httpd.HttpError(w, "must provide service name", true, http.StatusBadRequest)
		return
	}

	test, ok := s.testers[name]
	if !ok {
		httpd.HttpError(w, fmt.Sprintf("service %q not found", name), true, http.StatusNotFound)
		return
	}

	options := test.TestOptions()
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(options)
}

func (s *Service) handleTest(w http.ResponseWriter, r *http.Request) {
	name := s.nameFromPath(r.URL.Path)
	if name == "" {
		httpd.HttpError(w, "must provide service name", true, http.StatusBadRequest)
		return
	}

	test, ok := s.testers[name]
	if !ok {
		httpd.HttpError(w, fmt.Sprintf("service %q not found", name), true, http.StatusNotFound)
		return
	}

	options := test.TestOptions()
	if err := json.NewDecoder(r.Body).Decode(options); err != nil {
		httpd.HttpError(w, fmt.Sprint("failed to decode JSON body:", err), true, http.StatusBadRequest)
		return
	}

	result := testResult{}
	err := test.Test(options)
	if err != nil {
		result.Message = err.Error()
	} else {
		result.Success = true
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(result)
}
