package slack

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"sync/atomic"

	"github.com/influxdata/kapacitor"
	"github.com/pkg/errors"
)

type Service struct {
	configValue atomic.Value
	logger      *log.Logger
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

func (s *Service) StateChangesOnly() bool {
	c := s.config()
	return c.StateChangesOnly
}

// slack attachment info
type attachment struct {
	Fallback string `json:"fallback"`
	Color    string `json:"color"`
	Text     string `json:"text"`
}

type testOptions struct {
	Channel   string               `json:"channel"`
	Message   string               `json:"message"`
	Level     kapacitor.AlertLevel `json:"level"`
	IconEmoji string               `json:"icon-emoji"`
}

func (s *Service) TestOptions() interface{} {
	c := s.config()
	return &testOptions{
		Channel: c.Channel,
		Message: "test slack message",
		Level:   kapacitor.CritAlert,
	}
}

func (s *Service) Test(options interface{}) error {
	o, ok := options.(*testOptions)
	if !ok {
		return fmt.Errorf("unexpected options type %T", options)
	}
	return s.Alert(o.Channel, o.Message, o.IconEmoji, o.Level)
}

func (s *Service) Alert(channel, message, iconEmoji string, level kapacitor.AlertLevel) error {
	url, post, err := s.preparePost(channel, message, iconEmoji, level)
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
			Error string `json:"error"`
		}
		r := &response{Error: fmt.Sprintf("failed to understand Slack response. code: %d content: %s", resp.StatusCode, string(body))}
		b := bytes.NewReader(body)
		dec := json.NewDecoder(b)
		dec.Decode(r)
		return errors.New(r.Error)
	}
	return nil
}

func (s *Service) preparePost(channel, message, iconEmoji string, level kapacitor.AlertLevel) (string, io.Reader, error) {
	c := s.config()

	if !c.Enabled {
		return "", nil, errors.New("service is not enabled")
	}
	if channel == "" {
		channel = c.Channel
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
	emoji := c.IconEmoji
	if iconEmoji != "" {
		emoji = iconEmoji
	}
	postData["as_user"] = emoji != ""
	postData["icon_emoji"] = emoji

	var post bytes.Buffer
	enc := json.NewEncoder(&post)
	err := enc.Encode(postData)
	if err != nil {
		return "", nil, err
	}

	return c.URL, &post, nil
}
