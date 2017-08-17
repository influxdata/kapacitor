package httppost

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"

	"github.com/influxdata/kapacitor/alert"
	"github.com/influxdata/kapacitor/bufpool"
	"github.com/influxdata/kapacitor/services/diagnostic"
)

// Only one of name and url should be non-empty
type Endpoint struct {
	mu      sync.RWMutex
	url     string
	headers map[string]string
	auth    BasicAuth
	closed  bool
}

func NewEndpoint(url string, headers map[string]string, auth BasicAuth) *Endpoint {
	return &Endpoint{
		url:     url,
		headers: headers,
		auth:    auth,
	}
}
func (e *Endpoint) Close() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.closed = true
	return
}

func (e *Endpoint) Update(c Config) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.url = c.URL
	e.headers = c.Headers
	e.auth = c.BasicAuth
}

func (e *Endpoint) NewHTTPRequest(body io.Reader) (req *http.Request, err error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closed {
		return nil, errors.New("endpoint was closed")
	}

	req, err = http.NewRequest("POST", e.url, body)
	if err != nil {
		return nil, fmt.Errorf("failed to create POST request: %v", err)
	}

	if e.auth.valid() {
		req.SetBasicAuth(e.auth.Username, e.auth.Password)
	}

	for k, v := range e.headers {
		req.Header.Add(k, v)
	}

	return req, nil
}

type Service struct {
	mu         sync.RWMutex
	endpoints  map[string]*Endpoint
	diagnostic diagnostic.Diagnostic
}

func NewService(c Configs, d diagnostic.Diagnostic) *Service {
	s := &Service{
		diagnostic: d,
		endpoints:  c.index(),
	}
	return s
}

func (s *Service) Endpoint(name string) (*Endpoint, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	e, ok := s.endpoints[name]
	return e, ok
}

func (s *Service) Update(newConfigs []interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	endpointSet := map[string]bool{}

	for _, nc := range newConfigs {
		if c, ok := nc.(Config); ok {
			if err := c.Validate(); err != nil {
				return err
			}
			e, ok := s.endpoints[c.Endpoint]
			if !ok {
				s.endpoints[c.Endpoint] = NewEndpoint(c.URL, c.Headers, c.BasicAuth)
				continue
			}
			e.Update(c)

			endpointSet[c.Endpoint] = true
		} else {
			return fmt.Errorf("unexpected config object type, got %T exp %T", nc, c)
		}
	}

	// Find any deleted endpoints
	for name, endpoint := range s.endpoints {
		if !endpointSet[name] {
			endpoint.Close()
			delete(s.endpoints, name)
		}
	}

	return nil
}

func (s *Service) Open() error {
	return nil
}

func (s *Service) Close() error {
	return nil
}

type testOptions struct {
	Endpoint string            `json:"endpoint"`
	URL      string            `json:"url"`
	Headers  map[string]string `json:"headers"`
}

func (s *Service) TestOptions() interface{} {
	return &testOptions{
		Endpoint: "example",
		URL:      "http://localhost:3000/",
		Headers:  map[string]string{"Auth": "secret"},
	}
}

func (s *Service) Test(options interface{}) error {
	var err error
	o, ok := options.(*testOptions)
	if !ok {
		return fmt.Errorf("unexpected options type %t", options)
	}

	event := alert.Event{}
	body := bytes.NewBuffer(nil)
	ad := event.AlertData()

	err = json.NewEncoder(body).Encode(ad)
	if err != nil {
		return fmt.Errorf("failed to marshal alert data json: %v", err)
	}

	// Create the HTTP request
	var req *http.Request
	e := &Endpoint{
		url:     o.URL,
		headers: o.Headers,
	}
	req, err = e.NewHTTPRequest(body)

	// Execute the request
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to POST alert data: %v", err)
	}
	resp.Body.Close()
	return nil
}

type HandlerConfig struct {
	URL      string            `mapstructure:"url"`
	Endpoint string            `mapstructure:"endpoint"`
	Headers  map[string]string `mapstructure:"headers"`
}

type handler struct {
	s          *Service
	bp         *bufpool.Pool
	endpoint   *Endpoint
	diagnostic diagnostic.Diagnostic
	headers    map[string]string
}

func (s *Service) Handler(c HandlerConfig, d diagnostic.Diagnostic) alert.Handler {
	e, ok := s.Endpoint(c.Endpoint)
	if !ok {
		e = NewEndpoint(c.URL, nil, BasicAuth{})
	}

	return &handler{
		s:          s,
		bp:         bufpool.New(),
		endpoint:   e,
		diagnostic: d,
		headers:    c.Headers,
	}
}

func (h *handler) NewHTTPRequest(body io.Reader) (req *http.Request, err error) {
	req, err = h.endpoint.NewHTTPRequest(body)
	if err != nil {
		return
	}

	for k, v := range h.headers {
		req.Header.Set(k, v)
	}

	return
}

func (h *handler) Handle(event alert.Event) {
	var err error

	// Construct the body of the HTTP request
	body := h.bp.Get()
	defer h.bp.Put(body)
	ad := event.AlertData()

	err = json.NewEncoder(body).Encode(ad)
	if err != nil {
		h.diagnostic.Diag(
			"level", "error",
			"msg", "failed to marshal alert data json",
			"error", err,
		)
		return
	}

	req, err := h.NewHTTPRequest(body)
	if err != nil {
		h.diagnostic.Diag(
			"level", "error",
			"msg", "failed to create HTTP request",
			"error", err,
		)
		return
	}

	// Execute the request
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		h.diagnostic.Diag(
			"level", "error",
			"msg", "failed to POST alert data",
			"error", err,
		)
		return
	}
	resp.Body.Close()
}
