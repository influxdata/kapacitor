package sensu

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"regexp"
	"sync/atomic"

	"github.com/influxdata/kapacitor/alert"
	"github.com/uber-go/zap"
)

type Service struct {
	configValue atomic.Value
	logger      zap.Logger
}

var validNamePattern = regexp.MustCompile(`^[\w\.-]+$`)

func NewService(c Config, l zap.Logger) *Service {
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

type testOptions struct {
	Name   string      `json:"name"`
	Output string      `json:"output"`
	Level  alert.Level `json:"level"`
}

func (s *Service) TestOptions() interface{} {
	return &testOptions{
		Name:   "testName",
		Output: "testOutput",
		Level:  alert.Critical,
	}
}

func (s *Service) Test(options interface{}) error {
	o, ok := options.(*testOptions)
	if !ok {
		return fmt.Errorf("unexpected options type %T", options)
	}
	return s.Alert(
		o.Name,
		o.Output,
		o.Level,
	)
}

func (s *Service) Alert(name, output string, level alert.Level) error {
	if !validNamePattern.MatchString(name) {
		return fmt.Errorf("invalid name %q for sensu alert. Must match %v", name, validNamePattern)
	}

	addr, postData, err := s.prepareData(name, output, level)
	if err != nil {
		return err
	}

	conn, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	enc := json.NewEncoder(conn)
	err = enc.Encode(postData)
	if err != nil {
		return err
	}
	resp, err := ioutil.ReadAll(conn)
	if err != nil {
		return err
	}
	if string(resp) != "ok" {
		return errors.New("sensu socket error: " + string(resp))
	}
	return nil
}

func (s *Service) prepareData(name, output string, level alert.Level) (*net.TCPAddr, map[string]interface{}, error) {

	c := s.config()

	if !c.Enabled {
		return nil, nil, errors.New("service is not enabled")
	}

	var status int
	switch level {
	case alert.OK:
		status = 0
	case alert.Info:
		status = 0
	case alert.Warning:
		status = 1
	case alert.Critical:
		status = 2
	default:
		status = 3
	}

	postData := make(map[string]interface{})
	postData["name"] = name
	postData["source"] = c.Source
	postData["output"] = output
	postData["status"] = status

	addr, err := net.ResolveTCPAddr("tcp", c.Addr)
	if err != nil {
		return nil, nil, err
	}

	return addr, postData, nil
}

type handler struct {
	s      *Service
	logger zap.Logger
}

func (s *Service) Handler(l zap.Logger) alert.Handler {
	return &handler{
		s:      s,
		logger: l,
	}
}

func (h *handler) Handle(event alert.Event) {
	if err := h.s.Alert(
		event.State.ID,
		event.State.Message,
		event.State.Level,
	); err != nil {
		h.logger.Error("failed to send event to Sensu", zap.Error(err))
	}
}
