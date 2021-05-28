package bigpanda

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"html"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/influxdata/kapacitor/alert"
	khttp "github.com/influxdata/kapacitor/http"
	"github.com/influxdata/kapacitor/keyvalue"
	"github.com/influxdata/kapacitor/models"
	"github.com/pkg/errors"
)

const (
	defaultTokenPrefix = "Bearer"
)

type Diagnostic interface {
	WithContext(ctx ...keyvalue.T) Diagnostic
	Error(msg string, err error)
}

type Service struct {
	configValue atomic.Value
	clientValue atomic.Value
	diag        Diagnostic
}

func NewService(c Config, d Diagnostic) (*Service, error) {
	s := &Service{
		diag: d,
	}
	s.configValue.Store(c)
	s.clientValue.Store(&http.Client{
		Transport: khttp.NewDefaultTransportWithTLS(&tls.Config{InsecureSkipVerify: c.InsecureSkipVerify}),
	})

	return s, nil
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

func (s *Service) Global() bool {
	return s.config().Global
}

func (s *Service) StateChangesOnly() bool {
	return s.config().StateChangesOnly
}

type testOptions struct {
	AppKey            string          `json:"app_key"`
	Message           string          `json:"message"`
	Level             alert.Level     `json:"level"`
	Data              alert.EventData `json:"event_data"`
	Timestamp         time.Time       `json:"timestamp"`
	PrimaryProperty   string          `json:"primary_property"`
	SecondaryProperty string          `json:"secondary_property"`
}

func (s *Service) TestOptions() interface{} {
	return &testOptions{
		AppKey:  s.config().AppKey,
		Message: "test bigpanda message",
		Level:   alert.Critical,
		Data: alert.EventData{
			Name:   "testBigPanda",
			Tags:   make(map[string]string),
			Fields: make(map[string]interface{}),
			Result: models.Result{},
		},
		Timestamp: time.Now(),
	}
}

func (s *Service) Test(options interface{}) error {
	o, ok := options.(*testOptions)
	if !ok {
		return fmt.Errorf("unexpected options type %T", options)
	}
	hc := &HandlerConfig{
		AppKey:            o.AppKey,
		PrimaryProperty:   o.PrimaryProperty,
		SecondaryProperty: o.SecondaryProperty,
	}
	return s.Alert("", o.Message, "", o.Level, o.Timestamp, o.Data, hc)
}

func (s *Service) Alert(id string, message string, details string, level alert.Level, timestamp time.Time, data alert.EventData, hc *HandlerConfig) error {
	req, err := s.preparePost(id, message, details, level, timestamp, data, hc)

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
			Error string `json:"error"`
		}
		r := &response{Error: fmt.Sprintf("failed to understand BigPanda response. code: %d content: %s", resp.StatusCode, string(body))}
		b := bytes.NewReader(body)
		dec := json.NewDecoder(b)
		dec.Decode(r)
		return errors.New(r.Error)
	}

	return nil
}

// BigPanda alert
// See https://docs.bigpanda.io/reference#alerts
/*

curl -X POST -H "Content-Type: application/json" \
    -H "Authorization: Bearer <YOUR TOKEN>" \
    https://api.bigpanda.io/data/v2/alerts \
    -d '{ "app_key": "<YOUR APP KEY>", "status": "critical", "host": "production-database-1", "check": "CPU overloaded" }'

{
  "app_key": "123",
  "status": "critical",
  "host": "production-database-1",
  "timestamp": 1402302570,
  "check": "CPU overloaded",
  "description": "CPU is above upper limit (70%)",
  "cluster": "production-databases",
  "my_unique_attribute": "my_unique_value"
}

   statuses: ok, critical, warning, acknowledged

  "primary_property": "application",
  "secondary_property": "host"
*/
func (s *Service) preparePost(id string, message string, details string, level alert.Level, timestamp time.Time, data alert.EventData, hc *HandlerConfig) (*http.Request, error) {
	c := s.config()
	if !c.Enabled {
		return nil, errors.New("service is not enabled")
	}

	var status string
	switch level {
	case alert.OK:
		status = "ok"
	case alert.Warning:
		status = "warning"
	case alert.Critical:
		status = "critical"
	case alert.Info:
		status = "ok"
	default:
		status = "critical"
	}

	bpData := make(map[string]interface{})

	if message != "" {
		bpData["description"] = message
	}

	//ignore default details containing full json event
	if details != "" {
		unescapeString := html.UnescapeString(details)
		if !strings.HasPrefix(unescapeString, "{") {
			bpData["details"] = unescapeString
		}
	}

	if id != "" {
		bpData["check"] = id
	}

	bpData["task"] = fmt.Sprintf("%s:%s", data.TaskName, data.Name)
	bpData["timestamp"] = timestamp.Unix()
	bpData["status"] = status

	// primary and secondary property
	if hc.PrimaryProperty != "" {
		bpData["primary_property"] = hc.PrimaryProperty
	}
	if hc.SecondaryProperty != "" {
		bpData["secondary_property"] = hc.SecondaryProperty
	}

	if hc.AppKey != "" {
		bpData["app_key"] = hc.AppKey
	} else {
		bpData["app_key"] = c.AppKey
	}

	for k, v := range data.Tags {
		bpData[k] = v
	}

	for k, v := range data.Fields {
		bpData[k] = fmt.Sprintf("%v", v)
	}

	var post bytes.Buffer
	enc := json.NewEncoder(&post)
	if err := enc.Encode(bpData); err != nil {
		return nil, err
	}

	bpUrl := hc.URL
	if bpUrl == "" {
		bpUrl = c.URL
	}

	alertUrl, err := url.Parse(bpUrl)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", alertUrl.String(), &post)
	req.Header.Add("Authorization", defaultTokenPrefix+" "+c.Token)
	req.Header.Add("Content-Type", "application/json")
	if err != nil {
		return nil, err
	}
	return req, nil
}

// HandlerConfig defines the high-level struct required to connect to BigPanda
type HandlerConfig struct {
	// BigPanda AppKey
	AppKey string `mapstructure:"app-key"`

	// webhook URL used to post alert.
	// If empty uses the service URL from the configuration.
	URL string `mapstructure:"url"`

	// custom primary BigPanda property
	PrimaryProperty string `mapstructure:"primary-property"`
	// custom secondary BigPanda property
	SecondaryProperty string `mapstructure:"secondary-property"`
}

type handler struct {
	s    *Service
	c    HandlerConfig
	diag Diagnostic
}

func (s *Service) Handler(c HandlerConfig, ctx ...keyvalue.T) (alert.Handler, error) {
	return &handler{
		s:    s,
		c:    c,
		diag: s.diag.WithContext(ctx...),
	}, nil
}

func (h *handler) Handle(event alert.Event) {
	if err := h.s.Alert(
		event.State.ID,
		event.State.Message,
		event.State.Details,
		event.State.Level,
		event.State.Time,
		event.Data,
		&h.c,
	); err != nil {
		h.diag.Error("failed to send event to BigPanda", err)
	}
}
