package servicenow

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	neturl "net/url"
	"strconv"
	"sync/atomic"
	text "text/template"

	"github.com/influxdata/kapacitor/alert"
	"github.com/influxdata/kapacitor/keyvalue"
	"github.com/pkg/errors"
)

const usualCutoff = 100

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
	AlertID string      `json:"alert_id"`
	Source  string      `json:"source"`
	Level   alert.Level `json:"level"`
	Message string      `json:"message"`
}

func (s *Service) TestOptions() interface{} {
	return &testOptions{
		AlertID: "id",
		Source:  "Kapacitor",
		Level:   alert.Critical,
		Message: "test servicenow alert",
	}
}

func (s *Service) Test(options interface{}) error {
	o, ok := options.(*testOptions)
	if !ok {
		return fmt.Errorf("unexpected options type %T", options)
	}
	c := s.config()
	hc := &HandlerConfig{
		Source: o.Source,
	}
	data := &alert.EventData{
		Fields: map[string]interface{}{},
		Tags:   map[string]string{},
	}

	return s.Alert(c.URL, o.AlertID, o.Message, o.Level, data, hc)
}

func (s *Service) Alert(url, alertID string, message string, level alert.Level, data *alert.EventData, hc *HandlerConfig) error {
	postUrl, post, err := s.preparePost(url, alertID, message, level, data, hc)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", postUrl, post)
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
		r := &response{Error: fmt.Sprintf("failed to understand ServiceNow response. code: %d content: %s", resp.StatusCode, string(body))}
		b := bytes.NewReader(body)
		dec := json.NewDecoder(b)
		dec.Decode(r)

		return errors.New(r.Error)
	}

	return nil
}

// Event is a structure representing ServiceNow event. It can also represent an alert.
type Event struct {
	Source         string `json:"source"`
	Node           string `json:"node,omitempty"`
	Type           string `json:"type,omitempty"`
	Resource       string `json:"resource,omitempty"`
	MetricName     string `json:"metric_name,omitempty"`
	MessageKey     string `json:"message_key,omitempty"`
	Severity       string `json:"severity,omitempty"`
	Description    string `json:"description,omitempty"`
	AdditionalInfo string `json:"additional_info,omitempty"`
}

// ServiceNow provides web service API which is recommended for publishing events. Multiple events can be published in a single call.
// https://docs.servicenow.com/bundle/paris-it-operations-management/page/product/event-management/task/send-events-via-web-service.html
type Events struct {
	Records []Event `json:"records"`
}

func (s *Service) preparePost(url, alertID, message string, level alert.Level, data *alert.EventData, hc *HandlerConfig) (string, io.Reader, error) {
	c := s.config()
	if !c.Enabled {
		return "", nil, errors.New("service is not enabled")
	}

	if url == "" {
		url = c.URL
	}
	u, err := neturl.Parse(url)
	if err != nil {
		return "", nil, err
	}

	cutoff := func(text string, max int) string {
		return text[:int(math.Min(float64(max), float64(len(text))))]
	}

	// fallback to config value if empty
	source := hc.Source
	if source == "" {
		source = c.Source
	}

	// resolve templates for node, type, resource, metric name and message key fields
	var buffer bytes.Buffer
	dataInfo := dataInfo{
		ID:       alertID,
		Name:     data.Name,
		TaskName: data.TaskName,
		Fields:   data.Fields,
		Tags:     data.Tags,
	}
	render := func(name, template string) (string, error) {
		if template != "" {
			buffer.Reset()
			templateImpl, err := text.New(name).Parse(template)
			if err != nil {
				return "", err
			}
			templateImpl.Execute(&buffer, &dataInfo)
			if err != nil {
				return "", err
			}
			return buffer.String(), nil
		}
		return "", nil
	}
	node, err := render("node", hc.Node)
	if err != nil {
		return "", nil, err
	}
	metricType, err := render("type", hc.Type)
	if err != nil {
		return "", nil, err
	}
	resource, err := render("resource", hc.Resource)
	if err != nil {
		return "", nil, err
	}
	metricName, err := render("metricName", hc.MetricName)
	if err != nil {
		return "", nil, err
	}
	messageKey, err := render("messageKey", hc.MessageKey)
	if err != nil {
		return "", nil, err
	}
	aiBytes := []byte("")
	if len(hc.AdditionalInfo) > 0 {
		additionalInfo := make(map[string]string, 0)
		for key, value := range hc.AdditionalInfo {
			switch v := value.(type) {
			case string:
				rv, err := render("additional_info."+key, v)
				if err != nil {
					return "", nil, err
				}
				additionalInfo[key] = rv
			default:
				additionalInfo[key] = fmt.Sprintf("%v", v)
			}
		}
		aiBytes, err = json.Marshal(additionalInfo)
		if err != nil {
			return "", nil, errors.Wrap(err, "error marshaling additional_info map")
		}
	}
	if messageKey == "" { // fallback to alert ID if empty
		messageKey = alertID
	}

	// convert event level to ServiceNow severity (OK (5), Warning (4), Minor (3), Major (2), Critical (1))
	severity := 0
	switch level {
	case alert.OK:
		fallthrough
	case alert.Info:
		severity = 5
	case alert.Warning:
		severity = 4
	case alert.Critical:
		severity = 1
	}

	payload := &Events{
		Records: []Event{
			{
				Source:         cutoff(source, usualCutoff),
				Node:           cutoff(node, usualCutoff),
				Type:           cutoff(metricType, usualCutoff),
				Resource:       cutoff(resource, usualCutoff),
				MetricName:     cutoff(metricName, usualCutoff),
				MessageKey:     cutoff(messageKey, 1024),
				Severity:       strconv.Itoa(severity),
				Description:    cutoff(message, 4000),
				AdditionalInfo: string(aiBytes),
			},
		},
	}

	postBytes, err := json.Marshal(payload)
	if err != nil {
		return "", nil, errors.Wrap(err, "error marshaling event struct")
	}

	return u.String(), bytes.NewBuffer(postBytes), nil
}

type dataInfo struct {
	ID       string
	Name     string
	TaskName string
	Fields   map[string]interface{}
	Tags     map[string]string
}

type HandlerConfig struct {
	// web service URL used to post messages.
	// If empty uses the service URL from the configuration.
	URL string `mapstructure:"url"`

	// Username for BASIC authentication.
	// If empty uses username from the configuration.
	Username string `mapstructure:"username"`

	// Password for BASIC authentication.
	// If empty uses password from the configuration.
	Password string `mapstructure:"password"`

	// Event source.
	// If empty uses source from the configuration.
	Source string `mapstructure:"source"`

	// Node name.
	Node string `json:"node"`

	// Metric type.
	Type string `json:"type"`

	// Node resource relevant to the event.
	Resource string `json:"resource"`

	// Metric name for which event has been created.
	MetricName string `json:"metric_name"`

	// Message key that identifies related event.
	MessageKey string `json:"message_key"`

	// Addition info is a map of key value pairs.
	AdditionalInfo map[string]interface{} `mapstructure:"additional_info"`
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
		h.c.URL,
		event.State.ID,
		event.State.Message,
		event.State.Level,
		&event.Data,
		&h.c,
	); err != nil {
		h.diag.Error("failed to send event to ServiceNow", err)
	}
}
