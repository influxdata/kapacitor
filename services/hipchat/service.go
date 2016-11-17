package hipchat

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"path"
	"sync/atomic"

	"github.com/influxdata/kapacitor/services/alert"
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

type testOptions struct {
	Room    string      `json:"room"`
	Message string      `json:"message"`
	Level   alert.Level `json:"level"`
}

func (s *Service) TestOptions() interface{} {
	c := s.config()
	return &testOptions{
		Room:    c.Room,
		Message: "test hipchat message",
		Level:   alert.Critical,
	}
}

func (s *Service) Test(options interface{}) error {
	o, ok := options.(*testOptions)
	if !ok {
		return fmt.Errorf("unexpected options type %T", options)
	}
	c := s.config()
	return s.Alert(nil, o.Room, c.Token, o.Message, o.Level)
}

func (s *Service) Alert(ctxt context.Context, room, token, message string, level alert.Level) error {
	url, post, err := s.preparePost(room, token, message, level)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", url, post)
	req.Header.Set("Content-Type", "application/json")
	if ctxt != nil {
		req = req.WithContext(ctxt)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		type response struct {
			Error string `json:"error"`
		}
		r := &response{Error: fmt.Sprintf("failed to understand HipChat response. code: %d content: %s", resp.StatusCode, string(body))}
		b := bytes.NewReader(body)
		dec := json.NewDecoder(b)
		dec.Decode(r)
		return errors.New(r.Error)
	}
	return nil
}

func (s *Service) preparePost(room, token, message string, level alert.Level) (string, io.Reader, error) {
	c := s.config()

	if !c.Enabled {
		return "", nil, errors.New("service is not enabled")
	}
	//Generate HipChat API URL including room and authentication token
	if room == "" {
		room = c.Room
	}
	if token == "" {
		token = c.Token
	}

	u, err := url.Parse(c.URL)
	if err != nil {
		return "", nil, err
	}
	u.Path = path.Join(u.Path, room, "notification")
	v := url.Values{}
	v.Set("auth_token", token)
	u.RawQuery = v.Encode()

	var color string
	switch level {
	case alert.Warning:
		color = "yellow"
	case alert.Critical:
		color = "red"
	default:
		color = "green"
	}

	postData := make(map[string]interface{})
	postData["from"] = "kapacitor"
	postData["color"] = color
	postData["message"] = message
	postData["notify"] = true

	var post bytes.Buffer
	enc := json.NewEncoder(&post)
	err = enc.Encode(postData)
	if err != nil {
		return "", nil, err
	}
	return u.String(), &post, nil
}

type HandlerConfig struct {
	// HipChat room in which to post messages.
	// If empty uses the channel from the configuration.
	Room string

	// HipChat authentication token.
	// If empty uses the token from the configuration.
	Token string
}

type handler struct {
	s *Service
	c HandlerConfig
}

func (s *Service) Handler(c HandlerConfig) alert.Handler {
	return &handler{
		s: s,
		c: c,
	}
}

func (h *handler) Name() string {
	return "HipChat"
}

func (h *handler) Handle(ctxt context.Context, event alert.Event) error {
	return h.s.Alert(
		ctxt,
		h.c.Room,
		h.c.Token,
		event.State.Message,
		event.State.Level,
	)
}
