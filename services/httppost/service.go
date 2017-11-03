package httppost

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"sync"
	"text/template"

	"time"

	"context"
	"github.com/influxdata/kapacitor/alert"
	"github.com/influxdata/kapacitor/keyvalue"
	"github.com/pkg/errors"
)

type Diagnostic interface {
	WithContext(ctx ...keyvalue.T) Diagnostic
	Error(msg string, err error, ctx ...keyvalue.T)
}

// Only one of name and url should be non-empty
type Endpoint struct {
	mu            sync.RWMutex
	url           string
	headers       map[string]string
	auth          BasicAuth
	alertTemplate *template.Template
	rowTemplate   *template.Template
	closed        bool
}

func NewEndpoint(url string, headers map[string]string, auth BasicAuth, at, rt *template.Template) *Endpoint {
	return &Endpoint{
		url:           url,
		headers:       headers,
		auth:          auth,
		alertTemplate: at,
		rowTemplate:   rt,
	}
}
func (e *Endpoint) Close() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.closed = true
	return
}

func (e *Endpoint) Update(c Config) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.url = c.URL
	e.headers = c.Headers
	e.auth = c.BasicAuth
	at, err := c.getAlertTemplate()
	if err != nil {
		return err
	}
	e.alertTemplate = at
	rt, err := c.getRowTemplate()
	if err != nil {
		return err
	}
	e.rowTemplate = rt
	return nil
}

func (e *Endpoint) AlertTemplate() *template.Template {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.alertTemplate
}

func (e *Endpoint) RowTemplate() *template.Template {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.rowTemplate
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
	mu        sync.RWMutex
	endpoints map[string]*Endpoint
	diag      Diagnostic
}

func NewService(c Configs, d Diagnostic) (*Service, error) {
	endpoints, err := c.index()
	if err != nil {
		return nil, err
	}
	return &Service{
		diag:      d,
		endpoints: endpoints,
	}, nil
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
				at, err := c.getAlertTemplate()
				if err != nil {
					return errors.Wrapf(err, "failed to get alert template for endpoint %q", c.Endpoint)
				}
				rt, err := c.getRowTemplate()
				if err != nil {
					return errors.Wrapf(err, "failed to get row template for endpoint %q", c.Endpoint)
				}
				s.endpoints[c.Endpoint] = NewEndpoint(c.URL, c.Headers, c.BasicAuth, at, rt)
				continue
			}
			if err := e.Update(c); err != nil {
				return errors.Wrapf(err, "failed to update endpoint %q", c.Endpoint)
			}

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
	Timeout  time.Duration     `json:"timeout"`
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
	URL             string            `mapstructure:"url"`
	Endpoint        string            `mapstructure:"endpoint"`
	Headers         map[string]string `mapstructure:"headers"`
	CaptureResponse bool              `mapstructure:"capture-response"`
	Timeout         time.Duration     `mapstructure:"timeout"`
}

type handler struct {
	s *Service

	endpoint *Endpoint
	headers  map[string]string

	captureResponse bool

	diag Diagnostic

	timeout time.Duration

	hc *http.Client
}

func (s *Service) Handler(c HandlerConfig, ctx ...keyvalue.T) alert.Handler {
	e, ok := s.Endpoint(c.Endpoint)
	if !ok {
		e = NewEndpoint(c.URL, nil, BasicAuth{}, nil, nil)
	}
	return &handler{
		s:               s,
		endpoint:        e,
		diag:            s.diag.WithContext(ctx...),
		headers:         c.Headers,
		captureResponse: c.CaptureResponse,
		timeout:         c.Timeout,
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
	body := new(bytes.Buffer)
	ad := event.AlertData()

	var contentType string
	if h.endpoint.AlertTemplate() != nil {
		err := h.endpoint.AlertTemplate().Execute(body, ad)
		if err != nil {
			h.diag.Error("failed to execute alert template", err)
			return
		}
	} else {
		err = json.NewEncoder(body).Encode(ad)
		if err != nil {
			h.diag.Error("failed to marshal alert data json", err)
			return
		}
		contentType = "application/json"
	}

	req, err := h.NewHTTPRequest(body)
	if err != nil {
		h.diag.Error("failed to create HTTP request", err)
		return
	}

	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	// Set timeout
	if h.timeout > 0 {
		ctx, cancel := context.WithTimeout(req.Context(), h.timeout)
		defer cancel()
		req = req.WithContext(ctx)
	}

	// Execute the request
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		h.diag.Error("failed to POST alert data", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		var err error
		if h.captureResponse {
			var body []byte
			body, err = ioutil.ReadAll(resp.Body)
			if err == nil {
				// Use the body content as the error
				err = errors.New(string(body))
			}
		} else {
			err = errors.New("unknown error, use .captureResponse() to capture the HTTP response")
		}
		h.diag.Error("POST returned non 2xx status code", err, keyvalue.KV("code", strconv.Itoa(resp.StatusCode)))
	}

}
