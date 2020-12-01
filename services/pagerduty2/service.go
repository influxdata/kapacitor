package pagerduty2

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"sync/atomic"
	text "text/template"
	"time"

	"github.com/influxdata/kapacitor/alert"
	"github.com/influxdata/kapacitor/keyvalue"
	"github.com/influxdata/kapacitor/models"
)

// This example shows how to send a trigger event without a dedup_key.
// In this case, PagerDuty will automatically assign a random and unique key
// and return it in the response object.
//
// You should store this key in case you want to send an acknowledge or resolve
// event to this incident in the future.
//
//{
//    "payload": {
//        "summary": "Example alert on host1.example.com",
//        "timestamp": "2015-07-17T08:42:58.315+0000",
//        "source": "monitoringtool:cloudvendor:central-region-dc-01:852559987:cluster/api-stats-prod-003",
//        "severity": "info",
//        "component": "postgres",
//        "group": "prod-datapipe",
//        "class": "deploy",
//        "custom_details": {
//            "ping time": "1500ms",
//            "load avg": 0.75
//        }
//    },
//    "routing_key": "samplekeyhere",
//    "dedup_key": "samplekeyhere",
//    "images": [{
//        "src": "https://www.pagerduty.com/wp-content/uploads/2016/05/pagerduty-logo-green.png",
//		  "href": "https://example.com/",
//		  "alt": "Example text"
//    }],
//    "links": [{
//        "href": "https://example.com/",
//        "text": "Link text"
//    }],
//    "event_action": "trigger",
//    "client": "Sample Monitoring Service",
//    "client_url": "https://monitoring.example.com"
//}

// PDCEF is the PagerDuty - Common Event Format (PD-CEF) as outlined in the v2 API
// https://v2.developer.pagerduty.com/docs/events-api-v2
// https://support.pagerduty.com/docs/pd-cef
//
// API entry point is now https://events.pagerduty.com/v2/enqueue
type PDCEF struct {
	Summary       string                 `json:"summary"`
	Source        string                 `json:"source"`
	Severity      string                 `json:"severity"`
	Timestamp     string                 `json:"timestamp"`
	Class         string                 `json:"class"`
	Component     string                 `json:"component"`
	Group         string                 `json:"group"`
	CustomDetails map[string]interface{} `json:"custom_details"`
}

// Image is the struct of elements for an image in the payload
type Image struct {
	Src  string `json:"src"`
	Href string `json:"href"`
	Alt  string `json:"alt"`
}

// Link is the struct of elements for a link in the payload
type Link struct {
	Href string `json:"href"`
	Text string `json:"text"`
}

// AlertPayload is the default struct to send an element through to PagerDuty
type AlertPayload struct {
	RoutingKey  string  `json:"routing_key"`
	EventAction string  `json:"event_action"`
	DedupKey    string  `json:"dedup_key"`
	Payload     *PDCEF  `json:"payload"`
	Images      []Image `json:"images"`
	Links       []Link  `json:"links"`
	Client      string  `json:"client"`
	ClientURL   string  `json:"client_url"`
}

// Diagnostic defines the interface of a diagnostic event
type Diagnostic interface {
	WithContext(ctx ...keyvalue.T) Diagnostic
	Error(msg string, err error)
}

// Service is the default struct for the HTTP service
type Service struct {
	configValue atomic.Value

	HTTPDService interface {
		URL() string
	}
	diag Diagnostic
}

// NewService returns a newly instantiated Service
func NewService(c Config, d Diagnostic) *Service {
	s := &Service{
		diag: d,
	}
	s.configValue.Store(c)
	return s
}

// Open is a bound method of the Service struct
func (s *Service) Open() error {
	return nil
}

// Close is a bound method of the Service struct
func (s *Service) Close() error {
	return nil
}

func (s *Service) config() Config {
	return s.configValue.Load().(Config)
}

// Update is a bound method of the Service struct, handles updates to the existing service
func (s *Service) Update(newConfig []interface{}) error {
	if l := len(newConfig); l != 1 {
		return fmt.Errorf("expected only one new config object, got %d", l)
	}

	c, ok := newConfig[0].(Config)
	if !ok {
		return fmt.Errorf("expected config object to be of type %T, got %T", c, newConfig[0])
	}

	s.configValue.Store(c)
	return nil
}

// Global is a bound method of the Service struct, returns whether the Service configuration is global
func (s *Service) Global() bool {
	c := s.config()
	return c.Global
}

type testOptions struct {
	AlertID     string          `json:"alert_id"`
	Description string          `json:"description"`
	Level       alert.Level     `json:"level"`
	Data        alert.EventData `json:"event_data"`
	Timestamp   time.Time       `json:"timestamp"`
	Links       []LinkTemplate  `json:"links"`
}

// TestOptions returns optional values for the test harness
func (s *Service) TestOptions() interface{} {
	layout := "2006-01-02T15:04:05.000Z"
	str := "2014-11-12T11:45:26.371Z"
	t, _ := time.Parse(layout, str)

	return &testOptions{
		AlertID:     "testAlertID",
		Description: "test pagerduty2 message",
		Level:       alert.Critical,
		Timestamp:   t,
		Data: alert.EventData{
			Name:   "testPagerDuty2",
			Tags:   make(map[string]string),
			Fields: make(map[string]interface{}),
			Result: models.Result{},
		},
		Links: []LinkTemplate{{
			Href: "https://example.com/a",
			Text: "a",
		}, {
			Href: "https://example.com/b",
			Text: "b",
		},
		},
	}
}

// Test is a bound method of the Service struct that handles testing the Alert function
func (s *Service) Test(options interface{}) error {
	o, ok := options.(*testOptions)
	if !ok {
		return fmt.Errorf("unexpected options type %T", options)
	}
	c := s.config()
	return s.Alert(
		c.RoutingKey,
		o.Links,
		o.AlertID,
		o.Description,
		o.Level,
		o.Timestamp,
		o.Data,
	)
}

// Alert is a bound method of the Service struct that processes a given alert to PagerDuty
//
// The req headers are now required with the API v2:
// https://v2.developer.pagerduty.com/docs/migrating-to-api-v2
func (s *Service) Alert(routingKey string, links []LinkTemplate, alertID, desc string, level alert.Level, timestamp time.Time, data alert.EventData) error {
	url, post, err := s.preparePost(routingKey, links, alertID, desc, level, timestamp, data)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", url, post)
	if err != nil {
		return err
	}

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Accept", "application/vnd.pagerduty+json;version=2")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusOK {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			fmt.Printf("error parsing error body\n")
			return err
		}
		type response struct {
			Status  string   `json:"status"`
			Message string   `json:"message"`
			Errors  []string `json:"errors"`
		}
		r := &response{Message: fmt.Sprintf("failed to understand PagerDuty2 response. code: %d content: %s", resp.StatusCode, string(body))}
		b := bytes.NewReader(body)
		dec := json.NewDecoder(b)
		dec.Decode(r)
		return fmt.Errorf("Status: %s, Message: %s Errors: %v", r.Status, r.Message, r.Errors)
	}
	return nil
}

func (s *Service) sendResolve(c Config, routingKey, alertID string) (string, io.Reader, error) {
	// create a new AlertPayload for us to fire off
	type Resolve struct {
		RoutingKey  string `json:"routing_key"`
		DedupKey    string `json:"dedup_key"`
		EventAction string `json:"event_action"`
	}

	ap := Resolve{}

	if routingKey == "" {
		ap.RoutingKey = c.RoutingKey
	} else {
		ap.RoutingKey = routingKey
	}

	ap.DedupKey = alertID
	ap.EventAction = "resolve"

	var post bytes.Buffer
	enc := json.NewEncoder(&post)
	err := enc.Encode(ap)
	if err != nil {
		return "", nil, err
	}

	return c.URL, &post, nil
}

// preparePost is a helper method that sets up the payload for transmission to PagerDuty
func (s *Service) preparePost(routingKey string, links []LinkTemplate, alertID, desc string, level alert.Level, timestamp time.Time, data alert.EventData) (string, io.Reader, error) {
	c := s.config()
	if !c.Enabled {
		return "", nil, errors.New("service is not enabled")
	}

	var severity string
	eventType := "trigger"

	switch level {
	case alert.Warning:
		severity = "warning"
	case alert.Critical:
		severity = "critical"
	case alert.Info:
		severity = "info"
	default:
		// default is a 'resolve' function
		return s.sendResolve(c, routingKey, alertID)
	}

	// create a new AlertPayload for us to fire off
	ap := &AlertPayload{
		Payload: &PDCEF{},
	}

	if routingKey == "" {
		ap.RoutingKey = c.RoutingKey
	} else {
		ap.RoutingKey = routingKey
	}

	ap.Client = "kapacitor"
	ap.ClientURL = s.HTTPDService.URL()
	ap.DedupKey = alertID
	ap.EventAction = eventType

	ap.Payload.CustomDetails = make(map[string]interface{})
	ap.Payload.CustomDetails["result"] = data.Result

	ap.Payload.Class = data.TaskName
	ap.Payload.Severity = severity
	ap.Payload.Source = "unknown"
	ap.Payload.Summary = desc
	ap.Payload.Timestamp = timestamp.Format("2006-01-02T15:04:05.000000000Z07:00")

	if len(links) > 0 {
		ap.Links = make([]Link, len(links))
		for i, l := range links {
			ap.Links[i] = Link{Href: l.Href, Text: l.Text}
		}
	}

	if _, ok := data.Tags["host"]; ok {
		ap.Payload.Source = data.Tags["host"]
	}

	// Post data to PagerDuty
	var post bytes.Buffer
	enc := json.NewEncoder(&post)
	err := enc.Encode(ap)
	if err != nil {
		return "", nil, err
	}

	return c.URL, &post, nil
}

type LinkTemplate struct {
	Href     string `mapstructure:"href" json:"href"`
	Text     string `mapstructure:"text" json:"text"`
	hrefTmpl *text.Template
	textTmpl *text.Template
}

// HandlerConfig defines the high-level struct required to connect to PagerDuty
type HandlerConfig struct {
	// The routing key to use for the alert.
	// Defaults to the value in the configuration if empty.
	RoutingKey string         `mapstructure:"routing-key"`
	Links      []LinkTemplate `mapstructure:"links"`
}

type handler struct {
	s    *Service
	c    HandlerConfig
	diag Diagnostic
}

// Handler is a bound method to the Service struct that returns the appropriate alert handler for PagerDuty
func (s *Service) Handler(c HandlerConfig, ctx ...keyvalue.T) (alert.Handler, error) {
	// Compile link templates
	for i, l := range c.Links {
		hrefTmpl, err := text.New("href").Parse(l.Href)
		if err != nil {
			return nil, err
		}
		c.Links[i].hrefTmpl = hrefTmpl
		if l.Text != "" {
			textTmpl, err := text.New("text").Parse(l.Text)
			if err != nil {
				return nil, err
			}
			c.Links[i].textTmpl = textTmpl
		}
	}
	return &handler{
		s:    s,
		c:    c,
		diag: s.diag.WithContext(ctx...),
	}, nil
}

// Handle is a bound method to the handler that processes a given alert
func (h *handler) Handle(event alert.Event) {
	// Execute templates
	td := event.TemplateData()
	var hrefBuf bytes.Buffer
	var textBuf bytes.Buffer
	for i, l := range h.c.Links {
		err := l.hrefTmpl.Execute(&hrefBuf, td)
		if err != nil {
			h.diag.Error("failed to handle event", err)
			return
		}
		h.c.Links[i].Href = hrefBuf.String()
		hrefBuf.Reset()

		if l.textTmpl != nil {
			err = l.textTmpl.Execute(&textBuf, td)
			if err != nil {
				h.diag.Error("failed to handle event", err)
				return
			}
			h.c.Links[i].Text = textBuf.String()
			textBuf.Reset()
		} else {
			h.c.Links[i].Text = h.c.Links[i].Href
		}
	}

	if err := h.s.Alert(
		h.c.RoutingKey,
		h.c.Links,
		event.State.ID,
		event.State.Message,
		event.State.Level,
		event.State.Time,
		event.Data,
	); err != nil {
		h.diag.Error("failed to send event to PagerDuty", err)
	}
}
