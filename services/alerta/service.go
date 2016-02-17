package alerta

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
)

type Service struct {
	url         string
	token       string
	environment string
	origin      string
	logger      *log.Logger
}

func NewService(c Config, l *log.Logger) *Service {
	return &Service{
		url:         c.URL,
		token:       c.Token,
		environment: c.Environment,
		origin:      c.Origin,
		logger:      l,
	}
}

func (s *Service) Open() error {
	return nil
}

func (s *Service) Close() error {
	return nil
}

func (s *Service) Alert(token, resource, event, environment, severity, status, group, value, message, origin string, service []string, data interface{}) error {
	if resource == "" || event == "" {
		return errors.New("Resource and Event are required to send an alert")
	}

	if token == "" {
		token = s.token
	}

	if environment == "" {
		environment = s.environment
	}

	if origin == "" {
		origin = s.origin
	}

	var Url *url.URL
	Url, err := url.Parse(s.url + "/alert?api-key=" + token)
	if err != nil {
		return err
	}

	postData := make(map[string]interface{})
	postData["resource"] = resource
	postData["event"] = event
	postData["environment"] = environment
	postData["severity"] = severity
	postData["status"] = status
	postData["group"] = group
	postData["value"] = value
	postData["text"] = message
	postData["origin"] = origin
	postData["data"] = data
	if len(service) > 0 {
		postData["service"] = service
	}

	var post bytes.Buffer
	enc := json.NewEncoder(&post)
	err = enc.Encode(postData)
	if err != nil {
		return err
	}

	resp, err := http.Post(Url.String(), "application/json", &post)
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
