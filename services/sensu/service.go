package sensu

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"regexp"
	"sync/atomic"

	"github.com/influxdata/kapacitor/alert"
)

type Service struct {
	configValue atomic.Value
	logger      *log.Logger
}

var validNamePattern = regexp.MustCompile(`^[\w\.-]+$`)

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

type testOptions struct {
	Name     string      `json:"name"`
	Hostname string      `json:"hostname"`
	Output   string      `json:"output"`
	Level    alert.Level `json:"level"`
}

func (s *Service) TestOptions() interface{} {
	return &testOptions{
		Name:     "testName",
		Hostname: "testHostname",
		Output:   "testOutput",
		Level:    alert.Critical,
	}
}

func (s *Service) Test(options interface{}) error {
	o, ok := options.(*testOptions)
	if !ok {
		return fmt.Errorf("unexpected options type %T", options)
	}
	return s.Alert(
		o.Name,
		o.Hostname,
		o.Output,
		o.Level,
	)
}

func (s *Service) Alert(name, hostname, output string, level alert.Level) error {
	if !validNamePattern.MatchString(name) {
		return fmt.Errorf("invalid name %q for sensu alert. Must match %v", name, validNamePattern)
	}

	addr, postData, err := s.prepareData(name, hostname, output, level)
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

func (s *Service) prepareData(name, hostname, output string, level alert.Level) (*net.TCPAddr, map[string]interface{}, error) {

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
	postData["source"] = hostname
	postData["output"] = output
	postData["status"] = status
	postData["auto_resolve"] = false

	addr, err := net.ResolveTCPAddr("tcp", c.Addr)
	if err != nil {
		return nil, nil, err
	}

	return addr, postData, nil
}

type handler struct {
	s      *Service
	logger *log.Logger
}

func (s *Service) Handler(l *log.Logger) alert.Handler {
	return &handler{
		s:      s,
		logger: l,
	}
}

func (h *handler) Handle(event alert.Event) {
	if err := h.s.Alert(
		event.State.ID,
		event.State.Hostname,
		event.State.Message,
		event.State.Level,
	); err != nil {
		h.logger.Println("E! failed to send event to Sensu", err)
	}
}
