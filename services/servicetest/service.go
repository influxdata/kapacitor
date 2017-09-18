package servicetest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"path"
	"path/filepath"
	"sort"
	"strings"

	client "github.com/influxdata/kapacitor/client/v1"
	"github.com/influxdata/kapacitor/services/httpd"
	"github.com/pkg/errors"
)

const (
	testPath         = "/service-tests"
	testPathAnchored = "/service-tests/"
	basePath         = httpd.BasePath + testPathAnchored
)

var serviceTestsLink = client.Link{Relation: client.Self, Href: path.Join(httpd.BasePath, testPath)}

type Tester interface {
	// TestOptions returns an object that is in turn passed to Test.
	// User specified data will be JSON encode/decoded to/from the object.
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

func NewService(c Config) *Service {
	return &Service{
		testers: make(map[string]Tester),
	}
}

func (s *Service) Open() error {
	// Define API routes
	s.routes = []httpd.Route{
		{
			Method:      "GET",
			Pattern:     testPath,
			HandlerFunc: s.handleListTests,
		},
		{
			Method:      "GET",
			Pattern:     testPathAnchored,
			HandlerFunc: s.handleTestOptions,
		},
		{
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

func (s *Service) serviceTestLink(service string) client.Link {
	return client.Link{Relation: client.Self, Href: path.Join(basePath, service)}
}

type ServiceTests struct {
	Link     client.Link     `json:"link"`
	Services ServiceTestList `json:"services"`
}

type ServiceTestList []ServiceTest

func (l ServiceTestList) Len() int           { return len(l) }
func (l ServiceTestList) Less(i, j int) bool { return l[i].Name < l[j].Name }
func (l ServiceTestList) Swap(i, j int)      { l[i], l[j] = l[j], l[i] }

type ServiceTest struct {
	Link    client.Link        `json:"link"`
	Name    string             `json:"name"`
	Options ServiceTestOptions `json:"options"`
}

type ServiceTestOptions interface{}

type ServiceTestResult struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

func (s *Service) handleListTests(w http.ResponseWriter, r *http.Request) {
	tests := ServiceTests{
		Link: serviceTestsLink,
	}
	pattern := r.URL.Query().Get("pattern")
	if pattern == "" {
		pattern = "*"
	}
	for name, test := range s.testers {
		if ok, err := filepath.Match(pattern, name); err != nil {
			httpd.HttpError(w, fmt.Sprintf("bad pattern: %v", err), true, http.StatusBadRequest)
			return
		} else if ok {
			options := test.TestOptions()
			tests.Services = append(tests.Services, ServiceTest{
				Link:    s.serviceTestLink(name),
				Name:    name,
				Options: options,
			})
		}
	}
	sort.Sort(tests.Services)

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
	serviceTest := ServiceTest{
		Link:    s.serviceTestLink(name),
		Name:    name,
		Options: options,
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(serviceTest)
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
	if options != nil {
		if err := json.NewDecoder(r.Body).Decode(options); err != nil {
			httpd.HttpError(w, fmt.Sprint("failed to decode JSON body:", err), true, http.StatusBadRequest)
			return
		}
	}

	result := ServiceTestResult{}
	err := test.Test(options)
	if err != nil {
		result.Message = err.Error()
	} else {
		result.Success = true
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(result)
}
