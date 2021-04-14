package hipchat

import (
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

func (s *Service) config() Config {
	return s.configValue.Load().(Config)
}

func (s *Service) Update(newConfig []interface{}) error {
	s.diag.Error("HipChat support has been removed", configerr)
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
		Message: "test removed message",
		Level:   alert.Critical,
	}
}

func (s *Service) Test(options interface{}) error {
	s.diag.Error("HipChat support has been removed", configerr)
	return configerr
}

func (s *Service) Alert(room, token, message string, level alert.Level) error {
	s.diag.Error("HipChat support has been removed", configerr)
	return nil
}

type HandlerConfig struct {
}

type handler struct {
	diag Diagnostic
}

func (s *Service) Handler(c HandlerConfig, ctx ...keyvalue.T) alert.Handler {
	return &handler{
		diag: s.diag.WithContext(ctx...),
	}
}

func (h *handler) Handle(event alert.Event) {
	h.diag.Error("HipChat support has been removed")
}
