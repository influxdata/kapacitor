package foo

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sync/atomic"

	"github.com/influxdata/kapacitor/alert"
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
	// Perform any initialization needed here
	return nil
}

func (s *Service) Close() error {
	// Perform any actions needed to properly close the service here.
	// For example signal and wait for all go routines to finish.
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

// config loads the config struct stored in the configValue field.
func (s *Service) config() Config {
	return s.configValue.Load().(Config)
}

// Alert sends a message to the specified room.
func (s *Service) Alert(room, message string) error {
	c := s.config()
	if !c.Enabled {
		return errors.New("service is not enabled")
	}
	type postMessage struct {
		Room    string `json:"room"`
		Message string `json:"message"`
	}
	data, err := json.Marshal(postMessage{
		Room:    room,
		Message: message,
	})
	if err != nil {
		return err
	}
	r, err := http.Post(c.URL, "application/json", bytes.NewReader(data))
	if err != nil {
		return err
	}
	r.Body.Close()
	if r.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected response code %d from Foo service", r.StatusCode)
	}
	return nil
}

type HandlerConfig struct {
	//Room specifies the destination room for the chat messages.
	Room string `mapstructure:"room"`
}

// handler provides the implementation of the alert.Handler interface for the Foo service.
type handler struct {
	s      *Service
	c      HandlerConfig
	logger *log.Logger
}

// DefaultHandlerConfig returns a HandlerConfig struct with defaults applied.
func (s *Service) DefaultHandlerConfig() HandlerConfig {
	// return a handler config populated with the default room from the service config.
	c := s.config()
	return HandlerConfig{
		Room: c.Room,
	}
}

// Handler creates a handler from the config.
func (s *Service) Handler(c HandlerConfig, l *log.Logger) alert.Handler {
	// handlers can operate in differing contexts, as a result a logger is passed
	// in so that logs from this handler can be correctly associatied with a given context.
	return &handler{
		s:      s,
		c:      c,
		logger: l,
	}
}

// Handle takes an event and posts its message to the Foo service chat room.
func (h *handler) Handle(event alert.Event) {
	if err := h.s.Alert(h.c.Room, event.State.Message); err != nil {
		h.logger.Println("E! failed to handle event", err)
	}
}

type testOptions struct {
	Room    string `json:"room"`
	Message string `json:"message"`
}

func (s *Service) TestOptions() interface{} {
	c := s.config()
	return &testOptions{
		Room:    c.Room,
		Message: "test foo message",
	}
}

func (s *Service) Test(o interface{}) error {
	options, ok := o.(*testOptions)
	if !ok {
		return fmt.Errorf("unexpected options type %T", options)
	}
	return s.Alert(options.Room, options.Message)
}
