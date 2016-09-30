package pagerduty

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"sync/atomic"

	"github.com/influxdata/kapacitor"
)

type Service struct {
	configValue atomic.Value

	HTTPDService interface {
		URL() string
	}
	logger *log.Logger
}

func NewService(c Config, l *log.Logger) *Service {
	s := &Service{
		logger: l,
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

func (s *Service) Alert(serviceKey, incidentKey, desc string, level kapacitor.AlertLevel, details interface{}) error {
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

func (s *Service) preparePost(serviceKey, incidentKey, desc string, level kapacitor.AlertLevel, details interface{}) (string, io.Reader, error) {

	c := s.config()
	if !c.Enabled {
		return "", nil, errors.New("service is not enabled")
	}

	var eventType string
	switch level {
	case kapacitor.WarnAlert, kapacitor.CritAlert:
		eventType = "trigger"
	case kapacitor.InfoAlert:
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
	pData["client"] = kapacitor.Product
	pData["client_url"] = s.HTTPDService.URL()
	if details != nil {
		b, err := json.Marshal(details)
		if err != nil {
			return "", nil, err
		}
		pData["details"] = string(b)
	}

	// Post data to PagerDuty
	var post bytes.Buffer
	enc := json.NewEncoder(&post)
	err := enc.Encode(pData)
	if err != nil {
		return "", nil, err
	}

	return c.URL, &post, nil
}
