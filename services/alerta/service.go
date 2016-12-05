package alerta

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"path"
	"sync/atomic"
)

type Service struct {
	configValue atomic.Value
	clientValue atomic.Value
	logger      *log.Logger
}

func NewService(c Config, l *log.Logger) *Service {
	s := &Service{
		logger: l,
	}
	s.configValue.Store(c)
	s.clientValue.Store(&http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: c.InsecureSkipVerify},
		},
	})
	return s
}

type testOptions struct {
	Resource    string   `json:"resource"`
	Event       string   `json:"event"`
	Environment string   `json:"environment"`
	Severity    string   `json:"severity"`
	Group       string   `json:"group"`
	Value       string   `json:"value"`
	Message     string   `json:"message"`
	Origin      string   `json:"origin"`
	Service     []string `json:"service"`
}

func (s *Service) TestOptions() interface{} {
	c := s.config()
	return &testOptions{
		Resource:    "testResource",
		Event:       "testEvent",
		Environment: c.Environment,
		Severity:    "critical",
		Group:       "testGroup",
		Value:       "testValue",
		Message:     "test alerta message",
		Origin:      c.Origin,
		Service:     []string{"testServiceA", "testServiceB"},
	}
}

func (s *Service) Test(options interface{}) error {
	o, ok := options.(*testOptions)
	if !ok {
		return fmt.Errorf("unexpected options type %T", options)
	}
	c := s.config()
	return s.Alert(
		c.Token,
		o.Resource,
		o.Event,
		o.Environment,
		o.Severity,
		o.Group,
		o.Value,
		o.Message,
		o.Origin,
		o.Service,
		nil,
	)
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
		s.clientValue.Store(&http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: c.InsecureSkipVerify},
			},
		})
	}

	return nil
}

func (s *Service) Alert(token, resource, event, environment, severity, group, value, message, origin string, service []string, data interface{}) error {
	if resource == "" || event == "" {
		return errors.New("Resource and Event are required to send an alert")
	}

	req, err := s.preparePost(token, resource, event, environment, severity, group, value, message, origin, service, data)
	if err != nil {
		return err
	}

	client := s.clientValue.Load().(*http.Client)
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		type response struct {
			Message string `json:"message"`
		}
		r := &response{Message: fmt.Sprintf("failed to understand Alerta response. code: %d content: %s", resp.StatusCode, string(body))}
		b := bytes.NewReader(body)
		dec := json.NewDecoder(b)
		dec.Decode(r)
		return errors.New(r.Message)
	}
	return nil
}

func (s *Service) preparePost(token, resource, event, environment, severity, group, value, message, origin string, service []string, data interface{}) (*http.Request, error) {
	c := s.config()

	if !c.Enabled {
		return nil, errors.New("service is not enabled")
	}

	if token == "" {
		token = c.Token
	}

	if environment == "" {
		environment = c.Environment
	}

	if origin == "" {
		origin = c.Origin
	}

	u, err := url.Parse(c.URL)
	if err != nil {
		return nil, err
	}
	u.Path = path.Join(u.Path, "alert")

	postData := make(map[string]interface{})
	postData["resource"] = resource
	postData["event"] = event
	postData["environment"] = environment
	postData["severity"] = severity
	postData["group"] = group
	postData["value"] = value
	postData["text"] = message
	postData["origin"] = origin
	postData["rawData"] = data
	if len(service) > 0 {
		postData["service"] = service
	}

	var post bytes.Buffer
	enc := json.NewEncoder(&post)
	err = enc.Encode(postData)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", u.String(), &post)
	req.Header.Add("Authorization", "Key "+token)
	req.Header.Add("Content-Type", "application/json")
	if err != nil {
		return nil, err
	}

	return req, nil
}
