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
	"time"

	"github.com/influxdata/kapacitor/alert"
	khttp "github.com/influxdata/kapacitor/http"
	"github.com/influxdata/kapacitor/keyvalue"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/tick/ast"
	"github.com/influxdata/kapacitor/tick/stateful"
	"github.com/pkg/errors"
)

const (
	defaultResource    = "{{ .Name }}"
	defaultEvent       = "{{ .ID }}"
	defaultGroup       = "{{ .Group }}"
	defaultTimeout     = time.Duration(24 * time.Hour)
	defaultTokenPrefix = "Bearer"
)

type Diagnostic interface {
	WithContext(ctx ...keyvalue.T) Diagnostic
	TemplateError(err error, kv keyvalue.T)
	Error(msg string, err error)
}

type Service struct {
	configValue atomic.Value
	clientValue atomic.Value
	diag        Diagnostic
}

func NewService(c Config, d Diagnostic) *Service {
	s := &Service{
		diag: d,
	}
	s.configValue.Store(c)
	s.clientValue.Store(&http.Client{
		Transport: khttp.NewDefaultTransportWithTLS(&tls.Config{InsecureSkipVerify: c.InsecureSkipVerify}),
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
	Correlate   []string `json:"correlate"`
	Timeout     string   `json:"timeout"`
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
		Correlate:   []string{"testServiceX", "testServiceY"},
		Timeout:     "24h0m0s",
	}
}

func (s *Service) Test(options interface{}) error {
	o, ok := options.(*testOptions)
	if !ok {
		return fmt.Errorf("unexpected options type %T", options)
	}
	c := s.config()
	timeout, _ := time.ParseDuration(o.Timeout)
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
		o.Correlate,
		timeout,
		map[string]string{},
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
			Transport: khttp.NewDefaultTransportWithTLS(&tls.Config{InsecureSkipVerify: c.InsecureSkipVerify}),
		})
	}

	return nil
}

func (s *Service) Alert(token, tokenPrefix, resource, event, environment, severity, group, value, message, origin string, service []string, correlate []string, timeout time.Duration, tags map[string]string, data models.Result) error {
	if resource == "" || event == "" {
		return errors.New("Resource and Event are required to send an alert")
	}

	req, err := s.preparePost(token, tokenPrefix, resource, event, environment, severity, group, value, message, origin, service, correlate, timeout, tags, data)
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

func (s *Service) preparePost(token, tokenPrefix, resource, event, environment, severity, group, value, message, origin string, service []string, correlate []string, timeout time.Duration, tags map[string]string, data models.Result) (*http.Request, error) {
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
	if len(correlate) > 0 {
		postData["correlate"] = correlate
	}
	postData["timeout"] = int64(timeout / time.Second)

	tagList := make([]string, 0)
	for k, v := range tags {
		tagList = append(tagList, fmt.Sprintf("%s=%s", k, v))
	}
	postData["tags"] = tagList

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

	// Renaming rules for severities
	// Allows to rewrite build-in kapacitor severities (info, warn, crit) to any of Alerta multiple severities
	RenameSeverities map[string]string `mapstructure:"rename-severities"`

	// Expressions for custom Alerta severities
	// Allows to fine tune severity levels of kapacitor
	ExtraSeverityExpressions []stateful.Expression `mapstructure:"severity-expressions"`
	ExtraSeverityNames       []string              `mapstructure:"severity-names"`
	ExtraSeverityScopePools  []stateful.ScopePool  `mapstructure:"severity-scope-pool"`

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

	// List of correlated events
	Correlate []string `mapstructure:"correlate"`

	// Alerta timeout.
	// Default: 24h
	Timeout time.Duration `mapstructure:"timeout"`
}

type handler struct {
	s    *Service
	c    HandlerConfig
	diag Diagnostic

	resourceTmpl        *text.Template
	eventTmpl           *text.Template
	environmentTmpl     *text.Template
	renameSeverities    map[string]string
	severityLevels      []string
	severityExpressions []stateful.Expression
	scopePools          []stateful.ScopePool
	valueTmpl           *text.Template
	groupTmpl           *text.Template
	serviceTmpl         []*text.Template
	correlateTmpl       []*text.Template
}

func (s *Service) DefaultHandlerConfig() HandlerConfig {
	return HandlerConfig{
		Resource: defaultResource,
		Event:    defaultEvent,
		Group:    defaultGroup,
		Timeout:  defaultTimeout,
	}
}

func (s *Service) Handler(c HandlerConfig, ctx ...keyvalue.T) (alert.Handler, error) {
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

	var stmpl []*text.Template
	for _, service := range c.Service {
		tmpl, err := text.New("service").Parse(service)
		if err != nil {
			return nil, err
		}
		stmpl = append(stmpl, tmpl)
	}

	var ctmpl []*text.Template
	for _, correlate := range c.Correlate {
		tmpl, err := text.New("correlate").Parse(correlate)
		if err != nil {
			return nil, err
		}
		ctmpl = append(ctmpl, tmpl)
	}

	return &handler{
		s:                   s,
		c:                   c,
		diag:                s.diag.WithContext(ctx...),
		resourceTmpl:        rtmpl,
		eventTmpl:           evtmpl,
		environmentTmpl:     etmpl,
		renameSeverities:    c.RenameSeverities,
		severityLevels:      c.ExtraSeverityNames,
		severityExpressions: c.ExtraSeverityExpressions,
		scopePools:          c.ExtraSeverityScopePools,
		groupTmpl:           gtmpl,
		valueTmpl:           vtmpl,
		serviceTmpl:         stmpl,
		correlateTmpl:       ctmpl,
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
		h.diag.TemplateError(err, keyvalue.KV("resource", h.c.Resource))
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
		h.diag.TemplateError(err, keyvalue.KV("event", h.c.Event))
		return
	}
	eventStr := buf.String()
	buf.Reset()

	err = h.environmentTmpl.Execute(&buf, td)
	if err != nil {
		h.diag.TemplateError(err, keyvalue.KV("environment", h.c.Environment))
		return
	}
	environment := buf.String()
	buf.Reset()

	err = h.groupTmpl.Execute(&buf, td)
	if err != nil {
		h.diag.TemplateError(err, keyvalue.KV("group", h.c.Group))
		return
	}
	group := buf.String()
	buf.Reset()

	err = h.valueTmpl.Execute(&buf, td)
	if err != nil {
		h.diag.TemplateError(err, keyvalue.KV("value", h.c.Value))
		return
	}
	value := buf.String()
	buf.Reset()

	var service []string
	if len(h.serviceTmpl) == 0 {
		service = []string{td.Name}
	} else {
		for _, tmpl := range h.serviceTmpl {
			err = tmpl.Execute(&buf, td)
			if err != nil {
				h.diag.TemplateError(err, keyvalue.KV("service", tmpl.Name()))
				return
			}
			service = append(service, buf.String())
			buf.Reset()
		}
	}
	buf.Reset()

	var correlate []string
	if len(h.correlateTmpl) == 0 {
		correlate = []string{td.Name}
	} else {
		for _, tmpl := range h.correlateTmpl {
			err = tmpl.Execute(&buf, td)
			if err != nil {
				h.diag.TemplateError(err, keyvalue.KV("correlate", tmpl.Name()))
				return
			}
			correlate = append(correlate, buf.String())
			buf.Reset()
		}
	}

	var severity string
	var severityKey string

	switch event.State.Level {
	case alert.OK:
		severity = "ok"
		severityKey = "ok"
	case alert.Info:
		severity = "informational"
		severityKey = "info"
	case alert.Warning:
		severity = "warning"
		severityKey = "warn"
	case alert.Critical:
		severity = "critical"
		severityKey = "crit"
	default:
		severity = "indeterminate"
	}

	if val, ok := h.renameSeverities[severityKey]; ok {
		severity = val
	}

	if len(h.severityLevels) != 0 {
		for i, expression := range h.severityExpressions {
			if pass, err := EvalPredicate(expression, h.scopePools[i], event.State.Time, event.Data.Fields, event.Data.Tags); err != nil {
				h.diag.Error("error evaluating expression for Alerta severity", err)
			} else if pass {
				severity = h.severityLevels[i]
				break
			}
		}
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
		correlate,
		h.c.Timeout,
		event.Data.Tags,
		event.Data.Result,
	); err != nil {
		h.diag.Error("failed to send event to Alerta", err)
	}
}

func EvalPredicate(se stateful.Expression, scopePool stateful.ScopePool, now time.Time, fields models.Fields, tags models.Tags) (bool, error) {
	vars := scopePool.Get()
	defer scopePool.Put(vars)
	err := fillScope(vars, scopePool.ReferenceVariables(), now, fields, tags)
	if err != nil {
		return false, err
	}

	// for function signature check
	if _, err := se.Type(vars); err != nil {
		return false, err
	}

	return se.EvalBool(vars)
}

// fillScope - given a scope and reference variables, we fill the exact variables from the now, fields and tags.
func fillScope(vars *stateful.Scope, referenceVariables []string, now time.Time, fields models.Fields, tags models.Tags) error {
	for _, refVariableName := range referenceVariables {
		if refVariableName == "time" {
			vars.Set("time", now.Local())
			continue
		}

		// Support the error with tags/fields collision
		var fieldValue interface{}
		var isFieldExists bool
		var tagValue interface{}
		var isTagExists bool

		if fieldValue, isFieldExists = fields[refVariableName]; isFieldExists {
			vars.Set(refVariableName, fieldValue)
		}

		if tagValue, isTagExists = tags[refVariableName]; isTagExists {
			if isFieldExists {
				return fmt.Errorf("cannot have field and tags with same name %q", refVariableName)
			}
			vars.Set(refVariableName, tagValue)
		}
		if !isFieldExists && !isTagExists {
			if !vars.Has(refVariableName) {
				vars.Set(refVariableName, ast.MissingValue)
			}

		}
	}

	return nil
}
