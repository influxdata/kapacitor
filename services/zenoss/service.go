package zenoss

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"strings"
	"sync/atomic"
	text "text/template"
	"time"

	"github.com/influxdata/kapacitor/alert"
	"github.com/influxdata/kapacitor/keyvalue"
	"github.com/pkg/errors"
)

//const usualCutoff = 100

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

func (s *Service) Global() bool {
	return s.config().Global
}

func (s *Service) StateChangesOnly() bool {
	return s.config().StateChangesOnly
}

type testOptions struct {
	AlertID       string                 `json:"alert_id"`
	Level         alert.Level            `json:"level"`
	Message       string                 `json:"message"`
	Action        string                 `json:"action"`
	Method        string                 `json:"method"`
	Type          string                 `json:"type"`
	TID           int64                  `json:"tid"`
	Summary       string                 `json:"summary"`
	Device        string                 `json:"device"`
	Component     string                 `json:"component"`
	EventClassKey string                 `json:"eventclasskey"`
	EventClass    string                 `json:"eventclass"`
	Collector     string                 `json:"collector"`
	CustomFields  map[string]interface{} `json:"custom_fields"`
}

func (s *Service) TestOptions() interface{} {
	c := s.config()
	return &testOptions{
		AlertID:      "1001",
		Level:        alert.Critical,
		Message:      "test zenoss message",
		Action:       c.Action,
		Method:       c.Method,
		Type:         c.Type,
		TID:          c.TID,
		Collector:    c.Collector,
		CustomFields: map[string]interface{}{},
	}
}

func (s *Service) Test(options interface{}) error {
	o, ok := options.(*testOptions)
	if !ok {
		return fmt.Errorf("unexpected options type %T", options)
	}
	hc := &HandlerConfig{
		Action:        o.Action,
		Method:        o.Method,
		Type:          o.Type,
		TID:           o.TID,
		Summary:       o.Summary,
		Device:        o.Device,
		Component:     o.Component,
		EventClassKey: o.EventClassKey,
		EventClass:    o.EventClass,
		Collector:     o.Collector,
		CustomFields:  o.CustomFields,
	}
	data := &alert.EventData{
		Fields: map[string]interface{}{},
		Tags:   map[string]string{},
	}
	state := &alert.EventState{
		ID:      o.AlertID,
		Level:   o.Level,
		Message: o.Message,
	}

	return s.Alert(state, data, hc)
}

func (s *Service) Alert(state *alert.EventState, data *alert.EventData, hc *HandlerConfig) error {
	postUrl, postBody, err := s.preparePost(state, data, hc)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", postUrl, postBody)
	if err != nil {
		return err
	}

	req.Header.Add("Content-Type", "application/json")
	c := s.config()
	if c.Username != "" && c.Password != "" {
		req.SetBasicAuth(c.Username, c.Password)
	}
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}

		type response struct {
			Error string `json:"error"`
		}
		r := &response{Error: fmt.Sprintf("failed to understand Zenoss response. code: %d content: %s", resp.StatusCode, string(body))}
		b := bytes.NewReader(body)
		dec := json.NewDecoder(b)
		dec.Decode(r)

		return errors.New(r.Error)
	}

	return nil
}

// EventData represents Zenoss event data. All fields should be present in JSON.
type EventData struct {
	Summary    string      `json:"summary"`
	Device     string      `json:"device"`
	Component  string      `json:"component"`
	Severity   interface{} `json:"severity"`
	EvClassKey string      `json:"evclasskey"`
	EvClass    string      `json:"evclass"`
	Collector  string      `json:"collector,omitempty"`
	Message    string      `json:"message,omitempty"`
}

// Event is a structure representing Zenoss event.
type Event struct {
	Action string                   `json:"action"`
	Method string                   `json:"method"`
	Data   []map[string]interface{} `json:"data"`
	Type   string                   `json:"type"`
	TID    int64                    `json:"tid"`
}

func (s *Service) preparePost(state *alert.EventState, data *alert.EventData, hc *HandlerConfig) (string, io.Reader, error) {
	c := s.config()
	if !c.Enabled {
		return "", nil, errors.New("service is not enabled")
	}

	// fallback to config values for handler options not set
	action := hc.Action
	if action == "" {
		action = c.Action
	}
	method := hc.Method
	if method == "" {
		method = c.Method
	}
	typ := hc.Type
	if typ == "" {
		typ = c.Type
	}
	tid := hc.TID
	if tid == 0 {
		tid = c.TID
	}

	cutoff := func(text string, max int) string {
		return text[:int(math.Min(float64(max), float64(len(text))))]
	}

	// resolve templates for event standard data fields
	var buffer bytes.Buffer
	info := struct {
		Name        string
		TaskName    string
		ID          string
		Message     string
		Details     string
		Time        time.Time
		Duration    time.Duration
		Level       alert.Level
		Recoverable bool
		Tags        map[string]string
	}{
		data.Name,
		data.TaskName,
		state.ID,
		state.Message,
		state.Details,
		state.Time,
		state.Duration,
		state.Level,
		data.Recoverable,
		data.Tags,
	}
	render := func(name, template string) (string, error) {
		if template != "" {
			buffer.Reset()
			templateImpl, err := text.New(name).Parse(template)
			if err != nil {
				return "", err
			}
			templateImpl.Execute(&buffer, &info)
			if err != nil {
				return "", err
			}
			return buffer.String(), nil
		}
		return "", nil
	}
	summary, err := render("summary", hc.Summary)
	if err != nil {
		return "", nil, err
	}
	device, err := render("device", hc.Device)
	if err != nil {
		return "", nil, err
	}
	component, err := render("component", hc.Component)
	if err != nil {
		return "", nil, err
	}
	evclasskey, err := render("evclasskey", hc.EventClassKey)
	if err != nil {
		return "", nil, err
	}
	evclass, err := render("evclass", hc.EventClass)
	if err != nil {
		return "", nil, err
	}
	message, err := render("message", hc.Message)
	if err != nil {
		return "", nil, err
	}
	collector, err := render("collector", hc.Collector)
	if err != nil {
		return "", nil, err
	}
	severity := c.SeverityMap.ValueFor(state.Level)

	if summary == "" { // fallback to default (ie. alert message)
		summary = state.Message
	}
	if collector == "" {
		collector = c.Collector
	}

	eventData := &EventData{
		Summary:    cutoff(summary, 256),
		Device:     device,
		Component:  component,
		Severity:   severity,
		EvClassKey: evclasskey,
		EvClass:    evclass,
		Message:    cutoff(message, 4096),
		Collector:  collector,
	}
	eventDataMap, err := s.toMap(eventData)
	if err != nil {
		return "", nil, err
	}

	event := &Event{
		Action: action,
		Method: method,
		Data: []map[string]interface{}{
			eventDataMap,
		},
		Type: typ,
		TID:  tid,
	}

	if len(hc.CustomFields) > 0 {
		customDataMap := make(map[string]interface{})
		for key, value := range hc.CustomFields {
			switch v := value.(type) {
			case string:
				// resolve templates
				rv, err := render("customData."+key, v)
				if err != nil {
					return "", nil, err
				}
				customDataMap[key] = rv
			default:
				customDataMap[key] = v
			}
		}
		s.addCustomData(eventDataMap, customDataMap)
	}

	postBytes, err := json.Marshal(event)
	if err != nil {
		return "", nil, errors.Wrap(err, "error marshaling event struct")
	}

	return c.URL, bytes.NewBuffer(postBytes), nil
}

func (s *Service) addCustomData(basicData, customData map[string]interface{}) {
	// add custom fields
	for key, value := range customData {
		var cValue interface{}
		switch v := value.(type) {
		case string:
			if strings.HasPrefix(v, "{") || strings.HasPrefix(v, "[") { // can be JSON
				err := json.Unmarshal([]byte(v), &cValue)
				if err != nil { // guess not, it is just a text
					cValue = v
				}
			} else {
				cValue = v
			}
		default:
			cValue = v
		}
		basicData[key] = cValue
	}
}

func (s *Service) toMap(data *EventData) (map[string]interface{}, error) {
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	m := make(map[string]interface{})
	err = json.Unmarshal(dataBytes, &m)
	if err != nil {
		return nil, err
	}

	return m, nil
}

type HandlerConfig struct {
	// Action (router name).
	// If empty uses action from the configuration.
	Action string `mapstructure:"action"`

	// Router method.
	// If empty uses method from the configuration.
	Method string `mapstructure:"method"`

	// Event type.
	// If empty uses type from the configuration.
	Type string `mapstructure:"type"`

	// Event TID.
	// If empty uses TID from the configuration.
	TID int64 `mapstructure:"tid"`

	// Summary of the event.
	Summary string `json:"summary"`

	// Device related to the event.
	Device string `json:"device"`

	// Component related to the event.
	Component string `json:"component"`

	// Event Class Key of the event.
	EventClassKey string `json:"evclasskey"`

	// Event Class of the event.
	EventClass string `json:"evclass"`

	// Collector (typically IP or hostname).
	Collector string `json:"collector"`

	// Message related to the event.
	Message string `json:"message"`

	// Custom fields is a map of key value pairs.
	CustomFields map[string]interface{} `mapstructure:"customField"`
}

type handler struct {
	s    *Service
	c    HandlerConfig
	diag Diagnostic
}

func (s *Service) Handler(c HandlerConfig, ctx ...keyvalue.T) alert.Handler {
	return &handler{
		s:    s,
		c:    c,
		diag: s.diag.WithContext(ctx...),
	}
}

func (h *handler) Handle(event alert.Event) {
	if err := h.s.Alert(
		&event.State,
		&event.Data,
		&h.c,
	); err != nil {
		h.diag.Error("failed to send event to Zenoss", err)
	}
}
