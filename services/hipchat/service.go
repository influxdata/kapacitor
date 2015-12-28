package hipchat

import (
	"bytes"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"net/url"

	"github.com/influxdb/kapacitor"
)

type Service struct {
	room   string
	token  string
	url    string
	global bool
	logger *log.Logger
}

func NewService(c Config, l *log.Logger) *Service {
	return &Service{
		room:   c.Room,
		token:  c.Token,
		url:    c.URL,
		global: c.Global,
		logger: l,
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

func (s *Service) Alert(room, token, message string, level kapacitor.AlertLevel) error {

	//Generate HipChat API Url including room and authentication token
	if room == "" {
		room = s.room
	}
	if token == "" {
		token = s.token
	}

	var Url *url.URL
	Url, err := url.Parse(s.url + "/" + room + "/notification?auth_token=" + token)
	if err != nil {
		return err
	}

	var color string
	switch level {
	case kapacitor.WarnAlert:
		color = "yellow"
	case kapacitor.CritAlert:
		color = "red"
	default:
		color = "green"
	}

	postData := make(map[string]interface{})
	postData["from"] = kapacitor.Product
	postData["color"] = color
	postData["message"] = message
	postData["notify"] = true

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
		r := &response{Error: "failed to understand HipChat response"}
		dec := json.NewDecoder(resp.Body)
		dec.Decode(r)
		return errors.New(r.Error)
	}
	return nil
}
