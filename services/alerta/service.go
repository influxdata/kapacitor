package alerta

import (
	"bytes"
	"encoding/json"
	"errors"
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

func (s *Service) Alert(token, resource, event, environment, severity, status, group, value, message, origin string, data interface{}) error {
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
	if resp.StatusCode != http.StatusOK {
		type response struct {
			Error string `json:"error"`
		}
		r := &response{Error: "failed to understand Alerta response"}
		dec := json.NewDecoder(resp.Body)
		dec.Decode(r)
		return errors.New(r.Error)
	}
	return nil
}
