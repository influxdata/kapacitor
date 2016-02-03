package sensu

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/influxdata/kapacitor"
)

type Service struct {
	url    string
	source string
	logger *log.Logger
}

func NewService(c Config, l *log.Logger) *Service {
	return &Service{
		url:    c.URL,
		source: c.Source,
		logger: l,
	}
}

func (s *Service) Open() error {
	return nil
}

func (s *Service) Close() error {
	return nil
}

func (s *Service) Alert(name, output string, level kapacitor.AlertLevel) error {
	var status int
	switch level {
	case kapacitor.OKAlert:
		status = 0
	case kapacitor.InfoAlert:
		status = 0
	case kapacitor.WarnAlert:
		status = 1
	case kapacitor.CritAlert:
		status = 2
	default:
		status = 3
	}

	postData := make(map[string]interface{})
	postData["name"] = name
	postData["source"] = s.source
	postData["output"] = output
	postData["status"] = status

	var post bytes.Buffer
	enc := json.NewEncoder(&post)
	err := enc.Encode(postData)
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
			Error string `json:"error"`
		}
		r := &response{Error: "failed to understand Sensu response: " + string(body)}
		b := bytes.NewReader(body)
		dec := json.NewDecoder(b)
		dec.Decode(r)
		return errors.New(r.Error)
	}
	return nil
}
