package alertmanager

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/influxdata/kapacitor/alert"
	"github.com/influxdata/kapacitor/keyvalue"
	"net/http"
	"sync/atomic"
	text "text/template"
)

type Diagnostic interface {
	WithContext(ctx ...keyvalue.T) Diagnostic
	TemplateError(err error, kv keyvalue.T)
	Error(msg string, err error)
}

type Service struct {
	configValue atomic.Value
	diag        Diagnostic
}

type AlertmanagerRequest struct {
	Status      string                  `json:"status"`
	Labels      AlertmanagerLabels      `json:"labels"`
	Annotations AlertmanagerAnnotations `json:"annotations"`
}
type AlertmanagerLabels struct {
	Instance    string   `json:"instance"`
	Event       string   `json:"event"`
	Environment string   `json:"environment"`
	Origin      string   `json:"origin"`
	Service     []string `json:"service"`
	Group       string   `json:"group"`
	Customer    string   `json:"customer"`
}
type AlertmanagerAnnotations struct {
	Summary  string `json:"summary"`
	Value    string `json:"value"`
	Severity string `json:"severity"`
}

func NewService(c Config, d Diagnostic) *Service {
	s := &Service{
		diag: d,
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

type PostAlertManager []AlertManagerAlert
type AlertManagerAlert struct {
	Status      string
	Labels      map[string]string
	Annotations map[string]string
}

// Alert sends a to alertmanager .
func (s *Service) Alert(tagName []string, tagValue []string, annotationName []string, annotationValue []string, alertLevel interface{}) error {
	c := s.config()
	if len(tagName) != len(tagValue) {
		return errors.New("Lenght of tagName and tagValue is not equal")
	}
	if len(annotationName) != len(annotationValue) {
		return errors.New("Lenght of annotationName and annotationValue is not equal")
	}

	if !c.Enabled {
		return errors.New("service is not enabled")
	}

	alertStatus := "firing"
	//if alertLevel == alert.OK {
	//	alertStatus = "resolved"
	//}
	alertLabels := map[string]string{}
	for i := 0; i < len(tagName); i++ {
		alertLabels[tagName[i]] = tagValue[i]
	}

	alertAnnotations := map[string]string{}
	for i := 0; i < len(annotationName); i++ {
		alertAnnotations[annotationName[i]] = annotationValue[i]
	}

	newAlert := AlertManagerAlert{
		Status:      alertStatus,
		Labels:      alertLabels,
		Annotations: alertAnnotations,
	}

	postMessage := PostAlertManager{newAlert}

	data, err := json.Marshal(postMessage)
	if err != nil {
		return err
	}

	r, err := http.Post(c.URL, "application/json", bytes.NewReader(data))
	if err != nil {
		return err
	}
	r.Body.Close()
	if r.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected response code %d from Alertmanager service", r.StatusCode)
	}
	return nil
}

type HandlerConfig struct {
	// tag name for alert in alertmanager
	AlertManagerTagName []string `mapstructure:"alertManagerTagName"`
	// tag value of alertmanager
	AlertManagerTagValue []string `mapstructure:"alertManagerTagValue"`
	// annotation name for alert in alertmanager
	AlertManagerAnnotationName []string `mapstructure:"alertManagerAnnotationName"`
	// annotation value for alert in alertmanager
	AlertManagerAnnotationValue []string `mapstructure: "alertManagerAnnotationValue"`
}

// handler provides the implementation of the alert.Handler interface for the Foo service.
type handler struct {
	s    *Service
	c    HandlerConfig
	diag Diagnostic

	tagNametmpl   []*text.Template
	tagValuetmpl  []*text.Template
	annoNametmpl  []*text.Template
	annoValuetmpl []*text.Template
}

// DefaultHandlerConfig returns a HandlerConfig struct with defaults applied.
func (s *Service) DefaultHandlerConfig() HandlerConfig {
	return HandlerConfig{}
}

func (s *Service) Handler(c HandlerConfig, ctx ...keyvalue.T) (alert.Handler, error) {
	var tagNametmpl []*text.Template
	for _, tagName := range c.AlertManagerTagName {
		tmpl, err := text.New("service").Parse(tagName)
		if err != nil {
			return nil, err
		}
		tagNametmpl = append(tagNametmpl, tmpl)
	}
	var tagValuetmpl []*text.Template
	for _, tagValue := range c.AlertManagerTagValue {
		tmpl, err := text.New("service").Parse(tagValue)
		if err != nil {
			return nil, err
		}
		tagValuetmpl = append(tagValuetmpl, tmpl)
	}
	var annoNametmpl []*text.Template
	for _, annoName := range c.AlertManagerAnnotationName {
		tmpl, err := text.New("service").Parse(annoName)
		if err != nil {
			return nil, err
		}
		annoNametmpl = append(annoNametmpl, tmpl)
	}

	var annoValuetmpl []*text.Template
	for _, annoValue := range c.AlertManagerAnnotationValue {
		tmpl, err := text.New("service").Parse(annoValue)
		if err != nil {
			return nil, err
		}
		annoValuetmpl = append(annoValuetmpl, tmpl)
	}

	return &handler{
		s:    s,
		c:    c,
		diag: s.diag.WithContext(ctx...),

		tagNametmpl:   tagNametmpl,
		tagValuetmpl:  tagValuetmpl,
		annoNametmpl:  annoNametmpl,
		annoValuetmpl: annoValuetmpl,
	}, nil
}

// Handle takes an event and posts its message to the alertmanager
func (h *handler) Handle(event alert.Event) {
	td := event.TemplateData()
	var buf bytes.Buffer
	var err error
	var tagName,tagValue,annoName, annoValue []string
	for _, tmpl := range h.tagNametmpl {
		err = tmpl.Execute(&buf, td)
		if err != nil {
			h.diag.TemplateError(err, keyvalue.KV("alertManagerTagName", tmpl.Name()))
			return
		}
		tagName = append(tagName, buf.String())
		buf.Reset()
	}

	for _, tmpl := range h.tagValuetmpl {
		err = tmpl.Execute(&buf, td)
		if err != nil {
			h.diag.TemplateError(err, keyvalue.KV("alertManagerTagValue", tmpl.Name()))
			return
		}
		tagValue = append(tagValue, buf.String())
		buf.Reset()
	}
	for _, tmpl := range h.annoNametmpl {
		err = tmpl.Execute(&buf, td)
		if err != nil {
			h.diag.TemplateError(err, keyvalue.KV("alertManagerAnnotationName", tmpl.Name()))
			return
		}
		annoName = append(annoName, buf.String())
		buf.Reset()
	}
	for _, tmpl := range h.annoValuetmpl {
		err = tmpl.Execute(&buf, td)
		if err != nil {
			h.diag.TemplateError(err, keyvalue.KV("alertManagerAnnotationValue", tmpl.Name()))
			return
		}
		annoValue = append(annoValue, buf.String())
		buf.Reset()
	}

	if err := h.s.Alert(tagName, tagValue, annoName, annoValue, event.State.Level); err != nil {
		h.diag.Error("E! failed to handle event", err)
	}
}

type testOptions struct {
	//Message                     string   `json:"message"`
	AlertManagerTagName         []string `json:"alertManagerTagName"`
	AlertManagerTagValue        []string `json:"alertManagerTagValue"`
	AlertManagerAnnotationName  []string `json:"alertManagerAnnotationName"`
	AlertManagerAnnotationValue []string `json:"alertManagerAnnotationValue"`
}

func (s *Service) TestOptions() interface{} {
	return &testOptions{
		AlertManagerTagName:         []string{"tagA", "tagB"},
		AlertManagerTagValue:        []string{"tag_valueA", "tag_valueB"},
		AlertManagerAnnotationName:  []string{"annA", "annB"},
		AlertManagerAnnotationValue: []string{"ann_valueA", "ann_valueB"},
	}
}

func (s *Service) Test(o interface{}) error {
	options, ok := o.(*testOptions)
	if !ok {
		return fmt.Errorf("unexpected options type %T", options)
	}
	return s.Alert(options.AlertManagerTagName, options.AlertManagerTagValue, options.AlertManagerAnnotationName, options.AlertManagerAnnotationValue, alert.Critical)
}
