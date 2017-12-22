package pagerduty

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"sync/atomic"

	"github.com/influxdata/kapacitor/alert"
	"github.com/influxdata/kapacitor/keyvalue"
)

type Diagnostic interface {
	WithContext(ctx ...keyvalue.T) Diagnostic
	Error(msg string, err error)
}

type Service struct {
	configValue atomic.Value

	HTTPDService interface {
		URL() string
	}
	diag Diagnostic
}

func NewService(c Config, d Diagnostic) *Service {
	s := &Service{
		diag: d,
	}
	s.configValue.Store(c)
	return s
}

func (s *Service) Open() error {
	return nil
}

func (s *Service) Close() error {
	return nil
}

func (s *Service) config() Config {
	return s.configValue.Load().(Config)
}

func (s *Service) Update(newConfig []interface{}) error {
	if l := len(newConfig); l != 1 {
		return fmt.Errorf("expected only one new config object, got %d", l)
	}
	if c, ok := newConfig[0].(Config); !ok {
		return fmt.Errorf("expected config object to be of type %T, got %T", c, newConfig[0])
	} else {
		s.configValue.Store(c)
	}
	return nil
}

func (s *Service) Global() bool {
	c := s.config()
	return c.Global
}

type testOptions struct {
	IncidentKey string      `json:"incident-key"`
	Description string      `json:"description"`
	Details     string      `json:"details"`
	Level       alert.Level `json:"level"`
}

func (s *Service) TestOptions() interface{} {
	return &testOptions{
		IncidentKey: "testIncidentKey",
		Description: "test pagerduty message",
		Level:       alert.Critical,
	}
}

func (s *Service) Test(options interface{}) error {
	o, ok := options.(*testOptions)
	if !ok {
		return fmt.Errorf("unexpected options type %T", options)
	}
	c := s.config()
	return s.Alert(
		c.ServiceKey,
		o.IncidentKey,
		o.Description,
		o.Level,
		o.Details,
	)
}

func (s *Service) Alert(serviceKey, incidentKey, desc string, level alert.Level, details string) error {
	url, post, err := s.preparePost(serviceKey, incidentKey, desc, level, details)
	if err != nil {
		return err
	}

	resp, err := http.Post(url, "application/json", post)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		type response struct {
			Message string `json:"message"`
		}
		r := &response{Message: fmt.Sprintf("failed to understand PagerDuty response. code: %d content: %s", resp.StatusCode, string(body))}
		b := bytes.NewReader(body)
		dec := json.NewDecoder(b)
		dec.Decode(r)
		return errors.New(r.Message)
	}
	return nil
}

func (s *Service) preparePost(serviceKey, incidentKey, desc string, level alert.Level, details string) (string, io.Reader, error) {

	c := s.config()
	if !c.Enabled {
		return "", nil, errors.New("service is not enabled")
	}

	var eventType string
	switch level {
	case alert.Warning, alert.Critical:
		eventType = "trigger"
	case alert.Info:
		return "", nil, fmt.Errorf("AlertLevel 'info' is currently ignored by the PagerDuty service")
	default:
		eventType = "resolve"
	}

	pData := make(map[string]string)
	if serviceKey == "" {
		pData["service_key"] = c.ServiceKey
	} else {
		pData["service_key"] = serviceKey
	}
	pData["event_type"] = eventType
	pData["description"] = desc
	pData["incident_key"] = incidentKey
	pData["client"] = "kapacitor"
	pData["client_url"] = s.HTTPDService.URL()

	pData["details"] = details

	// Post data to PagerDuty
	var post bytes.Buffer
	enc := json.NewEncoder(&post)
	err := enc.Encode(pData)
	if err != nil {
		return "", nil, err
	}

	return c.URL, &post, nil
}

type HandlerConfig struct {
	// The service key to use for the alert.
	// Defaults to the value in the configuration if empty.
	ServiceKey string `mapstructure:"service-key"`
}

type handler struct {
	s    *Service
	c    HandlerConfig
	diag Diagnostic
}

func (s *Service) Handler(c HandlerConfig, ctx ...keyvalue.T) alert.Handler {
	return &handler{
		s:    s,
		c:    c,
		diag: s.diag.WithContext(ctx...),
	}
}

func (h *handler) Handle(event alert.Event) {
	if err := h.s.Alert(
		h.c.ServiceKey,
		event.State.ID,
		event.State.Message,
		event.State.Level,
		event.State.Details,
	); err != nil {
		h.diag.Error("failed to send event to PagerDuty", err)
	}
}
