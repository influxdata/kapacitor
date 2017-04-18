package sensu

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"regexp"
	"sync/atomic"
	text "text/template"

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
	Name   string      `json:"name"`
	Source string      `json:"source"`
	Output string      `json:"output"`
	Level  alert.Level `json:"level"`
}

func (s *Service) TestOptions() interface{} {
	return &testOptions{
		Name:   "testName",
		Source: "Kapacitor",
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
		o.Source,
		o.Output,
		o.Level,
	)
}

func (s *Service) Alert(name, source, output string, level alert.Level) error {
	if !validNamePattern.MatchString(name) {
		return fmt.Errorf("invalid name %q for sensu alert. Must match %v", name, validNamePattern)
	}

	addr, postData, err := s.prepareData(name, source, output, level)
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

func (s *Service) prepareData(name, source, output string, level alert.Level) (*net.TCPAddr, map[string]interface{}, error) {

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
	if source == "" {
		source = c.Source
	}
	postData["source"] = source
	postData["output"] = output
	postData["status"] = status

	addr, err := net.ResolveTCPAddr("tcp", c.Addr)
	if err != nil {
		return nil, nil, err
	}

	return addr, postData, nil
}

type HandlerConfig struct {
	// Sensu source for which to post messages.
	// If empty uses the source from the configuration.
	Source string `mapstructure:"source"`
}

type handler struct {
	s      *Service
	c      HandlerConfig
	logger *log.Logger

	sourceTmpl *text.Template
}

func (s *Service) Handler(c HandlerConfig, l *log.Logger) (alert.Handler, error) {
	srcTmpl, err := text.New("source").Parse(c.Source)
	if err != nil {
		return nil, err
	}
	return &handler{
		s:          s,
		c:          c,
		logger:     l,
		sourceTmpl: srcTmpl,
	}, nil
}

func (h *handler) Handle(event alert.Event) {
	td := event.TemplateData()
	var buf bytes.Buffer
	err := h.sourceTmpl.Execute(&buf, td)
	if err != nil {
		h.logger.Printf("E! failed to evaluate Sensu source template %s: %v", h.c.Source, err)
		return
	}
	sourceStr := buf.String()

	if err := h.s.Alert(
		event.State.ID,
		sourceStr,
		event.State.Message,
		event.State.Level,
	); err != nil {
		h.logger.Println("E! failed to send event to Sensu", err)
	}
}
