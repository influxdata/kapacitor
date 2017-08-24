package pushover

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
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
	diag        Diagnostic
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

type testOptions struct {
	UserKey  string      `json:"user-key"`
	Message  string      `json:"message"`
	Device   string      `json:"device"`
	Title    string      `json:"title"`
	URL      string      `json:"url"`
	URLTitle string      `json:"url-title"`
	Sound    string      `json:"sound"`
	Level    alert.Level `json:"level"`
}

func (s *Service) TestOptions() interface{} {
	c := s.config()
	return &testOptions{
		UserKey: c.UserKey,
		Message: "test pushover message",
		Level:   alert.Critical,
	}
}

func (s *Service) Test(options interface{}) error {
	o, ok := options.(*testOptions)
	if !ok {
		return fmt.Errorf("unexpected options type %t", options)
	}

	return s.Alert(
		o.Message,
		o.Device,
		o.Title,
		o.URL,
		o.URLTitle,
		o.Sound,
		o.Level,
	)
}

func (s *Service) config() Config {
	return s.configValue.Load().(Config)
}

func (s *Service) Alert(message, device, title, URL, URLTitle, sound string, level alert.Level) error {
	url, post, err := s.preparePost(message, device, title, URL, URLTitle, sound, level)
	if err != nil {
		return err
	}

	resp, err := http.PostForm(url, post)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		pushoverResponse := struct {
			Errors []string `json:"errors"`
		}{}
		err = json.Unmarshal(body, pushoverResponse)
		if err != nil {
			return err
		}
		type response struct {
			Error string `json:"error"`
		}
		r := &response{Error: fmt.Sprintf("failed to understand Pushover response. code: %d content: %s", resp.StatusCode, strings.Join(pushoverResponse.Errors, ", "))}
		b := bytes.NewReader(body)
		dec := json.NewDecoder(b)
		dec.Decode(r)
		return errors.New(r.Error)
	}

	return nil
}

// priority returns the pushover priority as defined by the Pushover API
// documentation https://pushover.net/api
func priority(level alert.Level) int {
	switch level {
	case alert.OK:
		// send as -2 to generate no notification/alert
		return -2
	case alert.Info:
		// -1 to always send as a quiet notification
		return -1
	case alert.Warning:
		// 0 to display as high-priority and bypass the user's quiet hours,
		return 0
	case alert.Critical:
		// 1 to also require confirmation from the user
		return 1
	}

	return 0
}

type postData struct {
	Token    string
	UserKey  string
	Message  string
	Device   string
	Title    string
	URL      string
	URLTitle string
	Priority int
	Sound    string
}

func (p *postData) Values() url.Values {
	v := url.Values{}

	v.Set("token", p.Token)
	v.Set("user", p.UserKey)
	v.Set("message", p.Message)
	v.Set("priority", strconv.Itoa(p.Priority))

	if p.Device != "" {
		v.Set("device", p.Device)
	}

	if p.Title != "" {
		v.Set("title", p.Title)
	}

	if p.URL != "" {
		v.Set("url", p.URL)
	}

	if p.URLTitle != "" {
		v.Set("url_title", p.URLTitle)
	}

	if p.Sound != "" {
		v.Set("sound", p.Sound)
	}

	return v

}

func (s *Service) preparePost(message, device, title, URL, URLTitle, sound string, level alert.Level) (string, url.Values, error) {
	c := s.config()

	if !c.Enabled {
		return "", nil, errors.New("service is not enabled")
	}

	p := postData{
		Token:   c.Token,
		UserKey: c.UserKey,
		Message: message,
	}

	p.Device = device
	p.Title = title
	p.URL = URL
	p.URLTitle = URLTitle
	p.Sound = sound

	p.Priority = priority(level)

	return c.URL, p.Values(), nil
}

type HandlerConfig struct {
	// rather than all of a user's devices (multiple device names may
	// be separated by a comma)
	Device string `mapstructure:"device"`

	// Your message's title, otherwise your apps name is used
	Title string `mapstructure:"title"`

	// A supplementary URL to show with your message
	URL string `mapstructure:"url"`

	// A title for your supplementary URL, otherwise just URL is shown
	URLTitle string `mapstructure:"url-title"`

	// The name of one of the sounds supported by the device clients to override
	// the user's default sound choice
	Sound string `mapstructure:"sound"`
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
		event.State.Message,
		h.c.Device,
		h.c.Title,
		h.c.URL,
		h.c.URLTitle,
		h.c.Sound,
		event.State.Level,
	); err != nil {
		h.diag.Error("failed to send event to Pushover", err)
	}
}
