package slack

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"

	"github.com/influxdata/kapacitor"
)

type Service struct {
	channel string
	url     string
	global  bool
	logger  *log.Logger
}

func NewService(c Config, l *log.Logger) *Service {
	return &Service{
		channel: c.Channel,
		url:     c.URL,
		global:  c.Global,
		logger:  l,
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

// slack attachment info
type attachment struct {
	Fallback string `json:"fallback"`
	Color    string `json:"color"`
	Text     string `json:"text"`
}

func (s *Service) Alert(channel, message string, level kapacitor.AlertLevel) error {
	if channel == "" {
		channel = s.channel
	}
	var color string
	switch level {
	case kapacitor.WarnAlert:
		color = "warning"
	case kapacitor.CritAlert:
		color = "danger"
	default:
		color = "good"
	}
	a := attachment{
		Fallback: message,
		Text:     message,
		Color:    color,
	}
	postData := make(map[string]interface{})
	postData["channel"] = channel
	postData["username"] = kapacitor.Product
	postData["text"] = ""
	postData["attachments"] = []attachment{a}

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
		type response struct {
			Error string `json:"error"`
		}
		r := &response{Error: fmt.Sprintf("failed to understand Slack response %s", err)}
		dec := json.NewDecoder(resp.Body)
		dec.Decode(r)
		return errors.New(r.Error)
	}
	return nil
}
