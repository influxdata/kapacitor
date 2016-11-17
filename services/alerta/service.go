package alerta

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"path"
	"sync/atomic"
	text "text/template"

	"github.com/influxdata/kapacitor/services/alert"
	"github.com/pkg/errors"
)

type Service struct {
	configValue atomic.Value
	clientValue atomic.Value
	logger      *log.Logger
}

func NewService(c Config, l *log.Logger) *Service {
	s := &Service{
		logger: l,
	}
	s.configValue.Store(c)
	s.clientValue.Store(&http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: c.InsecureSkipVerify},
		},
	})
	return s
}

type testOptions struct {
	Resource    string   `json:"resource"`
	Event       string   `json:"event"`
	Environment string   `json:"environment"`
	Severity    string   `json:"severity"`
	Group       string   `json:"group"`
	Value       string   `json:"value"`
	Message     string   `json:"message"`
	Origin      string   `json:"origin"`
	Service     []string `json:"service"`
}

func (s *Service) TestOptions() interface{} {
	c := s.config()
	return &testOptions{
		Resource:    "testResource",
		Event:       "testEvent",
		Environment: c.Environment,
		Severity:    "critical",
		Group:       "testGroup",
		Value:       "testValue",
		Message:     "test alerta message",
		Origin:      c.Origin,
		Service:     []string{"testServiceA", "testServiceB"},
	}
}

func (s *Service) Test(options interface{}) error {
	o, ok := options.(*testOptions)
	if !ok {
		return fmt.Errorf("unexpected options type %T", options)
	}
	c := s.config()
	return s.Alert(
		nil,
		c.Token,
		o.Resource,
		o.Event,
		o.Environment,
		o.Severity,
		o.Group,
		o.Value,
		o.Message,
		o.Origin,
		o.Service,
		nil,
	)
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
		s.clientValue.Store(&http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: c.InsecureSkipVerify},
			},
		})
	}

	return nil
}

func (s *Service) Alert(ctxt context.Context, token, resource, event, environment, severity, group, value, message, origin string, service []string, data interface{}) error {
	if resource == "" || event == "" {
		return errors.New("Resource and Event are required to send an alert")
	}

	req, err := s.preparePost(token, resource, event, environment, severity, group, value, message, origin, service, data)
	if err != nil {
		return err
	}

	if ctxt != nil {
		req = req.WithContext(ctxt)
	}
	client := s.clientValue.Load().(*http.Client)
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		type response struct {
			Message string `json:"message"`
		}
		r := &response{Message: fmt.Sprintf("failed to understand Alerta response. code: %d content: %s", resp.StatusCode, string(body))}
		b := bytes.NewReader(body)
		dec := json.NewDecoder(b)
		dec.Decode(r)
		return errors.New(r.Message)
	}
	return nil
}

func (s *Service) preparePost(token, resource, event, environment, severity, group, value, message, origin string, service []string, data interface{}) (*http.Request, error) {
	c := s.config()

	if !c.Enabled {
		return nil, errors.New("service is not enabled")
	}

	if token == "" {
		token = c.Token
	}

	if environment == "" {
		environment = c.Environment
	}

	if origin == "" {
		origin = c.Origin
	}

	u, err := url.Parse(c.URL)
	if err != nil {
		return nil, err
	}
	u.Path = path.Join(u.Path, "alert")

	postData := make(map[string]interface{})
	postData["resource"] = resource
	postData["event"] = event
	postData["environment"] = environment
	postData["severity"] = severity
	postData["group"] = group
	postData["value"] = value
	postData["text"] = message
	postData["origin"] = origin
	postData["rawData"] = data
	if len(service) > 0 {
		postData["service"] = service
	}

	var post bytes.Buffer
	enc := json.NewEncoder(&post)
	err = enc.Encode(postData)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", u.String(), &post)
	req.Header.Add("Authorization", "Bearer "+token)
	req.Header.Add("Content-Type", "application/json")
	if err != nil {
		return nil, err
	}

	return req, nil
}

type HandlerConfig struct {
	// Alerta authentication token.
	// If empty uses the token from the configuration.
	Token string

	// Alerta resource.
	// Can be a template and has access to the same data as the AlertNode.Details property.
	// Default: {{ .Name }}
	Resource string

	// Alerta event.
	// Can be a template and has access to the same data as the idInfo property.
	// Default: {{ .ID }}
	Event string

	// Alerta environment.
	// Can be a template and has access to the same data as the AlertNode.Details property.
	// Defaut is set from the configuration.
	Environment string

	// Alerta group.
	// Can be a template and has access to the same data as the AlertNode.Details property.
	// Default: {{ .Group }}
	Group string

	// Alerta value.
	// Can be a template and has access to the same data as the AlertNode.Details property.
	// Default is an empty string.
	Value string

	// Alerta origin.
	// If empty uses the origin from the configuration.
	Origin string

	// List of effected Services
	// tick:ignore
	Service []string `tick:"Services"`
}

type handler struct {
	s *Service
	c HandlerConfig

	resourceTmpl    *text.Template
	eventTmpl       *text.Template
	environmentTmpl *text.Template
	valueTmpl       *text.Template
	groupTmpl       *text.Template
}

func (s *Service) Handler(c HandlerConfig) (alert.Handler, error) {
	// Parse and validate alerta templates
	rtmpl, err := text.New("resource").Parse(c.Resource)
	if err != nil {
		return nil, err
	}
	evtmpl, err := text.New("event").Parse(c.Event)
	if err != nil {
		return nil, err
	}
	etmpl, err := text.New("environment").Parse(c.Environment)
	if err != nil {
		return nil, err
	}
	gtmpl, err := text.New("group").Parse(c.Group)
	if err != nil {
		return nil, err
	}
	vtmpl, err := text.New("value").Parse(c.Value)
	if err != nil {
		return nil, err
	}
	return handler{
		s:               s,
		c:               c,
		resourceTmpl:    rtmpl,
		eventTmpl:       evtmpl,
		environmentTmpl: etmpl,
		groupTmpl:       gtmpl,
		valueTmpl:       vtmpl,
	}, nil
}

type eventData struct {
	ID string
	// Measurement name
	Name string

	// Task name
	TaskName string

	// Concatenation of all group-by tags of the form [key=value,]+.
	// If not groupBy is performed equal to literal 'nil'
	Group string

	// Map of tags
	Tags map[string]string
}

func (h handler) Name() string {
	return "Alerta"
}

func (h handler) Handle(ctxt context.Context, event alert.Event) error {
	td := event.TemplateData()
	var buf bytes.Buffer
	err := h.resourceTmpl.Execute(&buf, td)
	if err != nil {
		return errors.Wrapf(err, "failed to evaluate Alerta Resource template %s", h.c.Resource)
	}
	resource := buf.String()
	buf.Reset()

	data := eventData{
		ID:       td.ID,
		Name:     td.Name,
		TaskName: td.TaskName,
		Group:    td.Group,
		Tags:     td.Tags,
	}
	err = h.eventTmpl.Execute(&buf, data)
	if err != nil {
		return errors.Wrapf(err, "failed to evaluate Alerta Event template %s", h.c.Event)
	}
	eventStr := buf.String()
	buf.Reset()

	err = h.environmentTmpl.Execute(&buf, td)
	if err != nil {
		return errors.Wrapf(err, "failed to evaluate Alerta Environment template %s", h.c.Environment)
	}
	environment := buf.String()
	buf.Reset()

	err = h.groupTmpl.Execute(&buf, td)
	if err != nil {
		return errors.Wrapf(err, "failed to evaluate Alerta Group template %s", h.c.Group)
	}
	group := buf.String()
	buf.Reset()

	err = h.valueTmpl.Execute(&buf, td)
	if err != nil {
		return errors.Wrapf(err, "failed to evaluate Alerta Value template %s", h.c.Value)
	}
	value := buf.String()

	service := h.c.Service
	if len(service) == 0 {
		service = []string{td.Name}
	}

	var severity string

	switch event.State.Level {
	case alert.OK:
		severity = "ok"
	case alert.Info:
		severity = "informational"
	case alert.Warning:
		severity = "warning"
	case alert.Critical:
		severity = "critical"
	default:
		severity = "indeterminate"
	}

	return h.s.Alert(
		ctxt,
		h.c.Token,
		resource,
		eventStr,
		environment,
		severity,
		group,
		value,
		event.State.Message,
		h.c.Origin,
		service,
		event.Data.Result,
	)
}
