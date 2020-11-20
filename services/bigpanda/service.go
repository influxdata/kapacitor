package bigpanda

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	khttp "github.com/influxdata/kapacitor/http"
	"github.com/influxdata/kapacitor/models"
	"html"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/influxdata/kapacitor/alert"
	"github.com/influxdata/kapacitor/keyvalue"
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
	AppKey    string          `json:"app_key"`
	Message   string          `json:"message"`
	Level     alert.Level     `json:"level"`
	Data      alert.EventData `json:"event_data"`
	Timestamp time.Time       `json:"timestamp"`
}

func (s *Service) TestOptions() interface{} {
	t, _ := time.Parse(time.RFC3339, "1970-01-01T00:00:01Z")

	return &testOptions{
		AppKey:  "my-app-key-123456",
		Message: "test bigpanda message",
		Level:   alert.Critical,
		Data: alert.EventData{
			Name:   "testBigPanda",
			Tags:   make(map[string]string),
			Fields: make(map[string]interface{}),
			Result: models.Result{},
		},
		Timestamp: t,
	}

}

func (s *Service) Test(options interface{}) error {
	o, ok := options.(*testOptions)
	if !ok {
		return fmt.Errorf("unexpected options type %T", options)
	}
	return s.Alert(o.AppKey, "", o.Message, "", o.Level, o.Timestamp, o.Data)
}

func (s *Service) Alert(appKey, id string, message string, details string, level alert.Level, timestamp time.Time, data alert.EventData) error {
	req, err := s.preparePost(appKey, id, message, details, level, timestamp, data)

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
func (s *Service) preparePost(appKey, id string, message string, details string, level alert.Level, timestamp time.Time, data alert.EventData) (*http.Request, error) {
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

	if appKey == "" {
		appKey = c.AppKey
	}
	bpData["app_key"] = appKey

	if len(data.Tags) > 0 {
		for k, v := range data.Tags {
			bpData[k] = v
		}
	}

	var post bytes.Buffer
	enc := json.NewEncoder(&post)
	if err := enc.Encode(bpData); err != nil {
		return nil, err
	}

	alertUrl, err := url.Parse(c.URL)
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
	AppKey string `mapstructure:"app-key"`
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
		h.c.AppKey,
		event.State.ID,
		event.State.Message,
		event.State.Details,
		event.State.Level,
		event.State.Time,
		event.Data,
	); err != nil {
		h.diag.Error("failed to send event to BigPanda", err)
	}
}
