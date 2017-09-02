package alerta

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"sync/atomic"
	text "text/template"

	"github.com/influxdata/kapacitor/alert"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/services/diagnostic"
	"github.com/pkg/errors"
)

const (
	defaultResource    = "{{ .Name }}"
	defaultEvent       = "{{ .ID }}"
	defaultGroup       = "{{ .Group }}"
	defaultTokenPrefix = "Bearer"
)

type Service struct {
	configValue atomic.Value
	clientValue atomic.Value
	diagnostic  diagnostic.Diagnostic
}

func NewService(c Config, d diagnostic.Diagnostic) *Service {
	s := &Service{
		diagnostic: d,
	}
	s.configValue.Store(c)
	s.clientValue.Store(&http.Client{
		Transport: &http.Transport{
			Proxy:           http.ProxyFromEnvironment,
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
		c.Token,
		c.TokenPrefix,
		o.Resource,
		o.Event,
		o.Environment,
		o.Severity,
		o.Group,
		o.Value,
		o.Message,
		o.Origin,
		o.Service,
		models.Result{},
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
				Proxy:           http.ProxyFromEnvironment,
				TLSClientConfig: &tls.Config{InsecureSkipVerify: c.InsecureSkipVerify},
			},
		})
	}

	return nil
}

func (s *Service) Alert(token, tokenPrefix, resource, event, environment, severity, group, value, message, origin string, service []string, data models.Result) error {
	if resource == "" || event == "" {
		return errors.New("Resource and Event are required to send an alert")
	}

	req, err := s.preparePost(token, tokenPrefix, resource, event, environment, severity, group, value, message, origin, service, data)
	if err != nil {
		return err
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

func (s *Service) preparePost(token, tokenPrefix, resource, event, environment, severity, group, value, message, origin string, service []string, data models.Result) (*http.Request, error) {
	c := s.config()

	if !c.Enabled {
		return nil, errors.New("service is not enabled")
	}

	if token == "" {
		token = c.Token
	}

	if tokenPrefix == "" {
		if c.TokenPrefix == "" {
			tokenPrefix = defaultTokenPrefix
		} else {
			tokenPrefix = c.TokenPrefix
		}
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
	req.Header.Add("Authorization", tokenPrefix+" "+token)
	req.Header.Add("Content-Type", "application/json")
	if err != nil {
		return nil, err
	}

	return req, nil
}

type HandlerConfig struct {
	// Alerta authentication token.
	// If empty uses the token from the configuration.
	Token string `mapstructure:"token"`

	// Alerta authentication token prefix.
	// If empty uses Bearer.
	TokenPrefix string `mapstructure:"token-prefix"`

	// Alerta resource.
	// Can be a template and has access to the same data as the AlertNode.Details property.
	// Default: {{ .Name }}
	Resource string `mapstructure:"resource"`

	// Alerta event.
	// Can be a template and has access to the same data as the idInfo property.
	// Default: {{ .ID }}
	Event string `mapstructure:"event"`

	// Alerta environment.
	// Can be a template and has access to the same data as the AlertNode.Details property.
	// Defaut is set from the configuration.
	Environment string `mapstructure:"environment"`

	// Alerta group.
	// Can be a template and has access to the same data as the AlertNode.Details property.
	// Default: {{ .Group }}
	Group string `mapstructure:"group"`

	// Alerta value.
	// Can be a template and has access to the same data as the AlertNode.Details property.
	// Default is an empty string.
	Value string `mapstructure:"value"`

	// Alerta origin.
	// If empty uses the origin from the configuration.
	Origin string `mapstructure:"origin"`

	// List of effected Services
	Service []string `mapstructure:"service"`
}

type handler struct {
	s          *Service
	c          HandlerConfig
	diagnostic diagnostic.Diagnostic

	resourceTmpl    *text.Template
	eventTmpl       *text.Template
	environmentTmpl *text.Template
	valueTmpl       *text.Template
	groupTmpl       *text.Template
}

func (s *Service) DefaultHandlerConfig() HandlerConfig {
	return HandlerConfig{
		Resource: defaultResource,
		Event:    defaultEvent,
		Group:    defaultGroup,
	}
}

func (s *Service) Handler(c HandlerConfig, d diagnostic.Diagnostic) (alert.Handler, error) {
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
	return &handler{
		s:               s,
		c:               c,
		diagnostic:      d,
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

func (h *handler) Handle(event alert.Event) {
	td := event.TemplateData()
	var buf bytes.Buffer
	err := h.resourceTmpl.Execute(&buf, td)
	if err != nil {
		h.diagnostic.Diag(
			"level", "error",
			"msg", "failed to evaluate Alerta Resource template",
			"resource", h.c.Resource,
			"error", err,
		)
		return
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
		h.diagnostic.Diag(
			"level", "error",
			"msg", "failed to evaluate Alerta Event template",
			"event", h.c.Event,
			"error", err,
		)
		return
	}
	eventStr := buf.String()
	buf.Reset()

	err = h.environmentTmpl.Execute(&buf, td)
	if err != nil {
		h.diagnostic.Diag(
			"level", "error",
			"msg", "failed to evaluate Alerta Environment template",
			"environment", h.c.Environment,
			"error", err,
		)
		return
	}
	environment := buf.String()
	buf.Reset()

	err = h.groupTmpl.Execute(&buf, td)
	if err != nil {
		h.diagnostic.Diag(
			"level", "error",
			"msg", "failed to evaluate Alerta Group template",
			"group", h.c.Group,
			"error", err,
		)
		return
	}
	group := buf.String()
	buf.Reset()

	err = h.valueTmpl.Execute(&buf, td)
	if err != nil {
		h.diagnostic.Diag(
			"level", "error",
			"msg", "failed to evaluate Alerta Value template",
			"alerta-value", h.c.Value,
			"error", err,
		)
		return
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

	if err := h.s.Alert(
		h.c.Token,
		h.c.TokenPrefix,
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
	); err != nil {
		h.diagnostic.Diag(
			"level", "error",
			"msg", "failed to send event to Alerta",
			"error", err,
		)
	}
}
