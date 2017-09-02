package sensu

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"regexp"
	"sync/atomic"
	text "text/template"

	"github.com/influxdata/kapacitor/alert"
	"github.com/influxdata/kapacitor/services/diagnostic"
)

type Service struct {
	configValue atomic.Value
	diagnostic  diagnostic.Diagnostic
}

var validNamePattern = regexp.MustCompile(`^[\w\.-]+$`)

func NewService(c Config, d diagnostic.Diagnostic) *Service {
	s := &Service{
		diagnostic: d,
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
	Source   string      `json:"source"`
	Output   string      `json:"output"`
	Handlers []string    `json:"handlers"`
	Level    alert.Level `json:"level"`
}

func (s *Service) TestOptions() interface{} {
	return &testOptions{
		Name:     "testName",
		Source:   "Kapacitor",
		Output:   "testOutput",
		Handlers: []string{},
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
		o.Source,
		o.Output,
		o.Handlers,
		o.Level,
	)
}

func (s *Service) Alert(name, source, output string, handlers []string, level alert.Level) error {
	if !validNamePattern.MatchString(name) {
		return fmt.Errorf("invalid name %q for sensu alert. Must match %v", name, validNamePattern)
	}

	addr, postData, err := s.prepareData(name, source, output, handlers, level)
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

func (s *Service) prepareData(name, source, output string, handlers []string, level alert.Level) (*net.TCPAddr, map[string]interface{}, error) {

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
	if len(handlers) == 0 {
		handlers = c.Handlers
	}
	postData["handlers"] = handlers

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

	// Sensu handler list
	// If empty uses the handler list from the configuration
	Handlers []string `mapstructure:"handlers"`
}

type handler struct {
	s          *Service
	c          HandlerConfig
	diagnostic diagnostic.Diagnostic

	sourceTmpl *text.Template
}

func (s *Service) Handler(c HandlerConfig, d diagnostic.Diagnostic) (alert.Handler, error) {
	srcTmpl, err := text.New("source").Parse(c.Source)
	if err != nil {
		return nil, err
	}
	return &handler{
		s:          s,
		c:          c,
		diagnostic: d,
		sourceTmpl: srcTmpl,
	}, nil
}

func (h *handler) Handle(event alert.Event) {
	td := event.TemplateData()
	var buf bytes.Buffer
	err := h.sourceTmpl.Execute(&buf, td)
	if err != nil {
		h.diagnostic.Diag(
			"level", "error",
			"msg", "failed to evaluate Sensu source template",
			"source", h.c.Source,
			"error", err,
		)
		return
	}
	sourceStr := buf.String()

	if err := h.s.Alert(
		event.State.ID,
		sourceStr,
		event.State.Message,
		h.c.Handlers,
		event.State.Level,
	); err != nil {
		h.diagnostic.Diag(
			"level", "error",
			"msg", "failed to send event to Sensu",
			"error", err,
		)
	}
}
