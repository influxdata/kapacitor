package pagerduty

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/influxdata/kapacitor"
)

const eventType = "trigger"

type Service struct {
	HTTPDService interface {
		URL() string
	}
	serviceKey string
	url        string
	global     bool
	logger     *log.Logger
}

func NewService(c Config, l *log.Logger) *Service {
	return &Service{
		serviceKey: c.ServiceKey,
		url:        c.URL,
		global:     c.Global,
		logger:     l,
	}
}

func (s *Service) Open() error {
	return nil
}

func (s *Service) Close() error {
	return nil
}

func (s *Service) Global() bool {
	return s.global
}

func (s *Service) Alert(incidentKey, desc string, details interface{}) error {
	pData := make(map[string]string)
	pData["service_key"] = s.serviceKey
	pData["event_type"] = eventType
	pData["description"] = desc
	pData["client"] = kapacitor.Product
	pData["client_url"] = s.HTTPDService.URL()
	if details != nil {
		b, err := json.Marshal(details)
		if err != nil {
			return err
		}
		pData["details"] = string(b)
	}

	// Post data to PagerDuty
	var post bytes.Buffer
	enc := json.NewEncoder(&post)
	err := enc.Encode(pData)
	if err != nil {
		return err
	}

	resp, err := http.Post(s.url, "application/json", &post)
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
		r := &response{Message: "failed to understand PagerDuty response: " + string(body)}
		b := bytes.NewReader(body)
		dec := json.NewDecoder(b)
		dec.Decode(r)
		return errors.New(r.Message)
	}
	return nil
}
