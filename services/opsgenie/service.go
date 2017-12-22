package opsgenie

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/influxdata/kapacitor/alert"
	"github.com/influxdata/kapacitor/keyvalue"
	"github.com/influxdata/kapacitor/models"
)

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
	c := s.config()
	return c.Global
}

type testOptions struct {
	Teams       []string `json:"teams"`
	Recipients  []string `json:"recipients"`
	MessageType string   `json:"message-type"`
	Message     string   `json:"message"`
	EntityID    string   `json:"entity-id"`
}

func (s *Service) TestOptions() interface{} {
	c := s.config()
	return &testOptions{
		Teams:       c.Teams,
		Recipients:  c.Recipients,
		MessageType: "CRITICAL",
		Message:     "test opsgenie message",
		EntityID:    "testEntityID",
	}
}

func (s *Service) Test(options interface{}) error {
	o, ok := options.(*testOptions)
	if !ok {
		return fmt.Errorf("unexpected options type %T", options)
	}
	return s.Alert(
		o.Teams,
		o.Recipients,
		o.MessageType,
		o.Message,
		o.EntityID,
		time.Now(),
		models.Result{},
	)
}

func (s *Service) Alert(teams []string, recipients []string, messageType, message, entityID string, t time.Time, details models.Result) error {
	url, post, err := s.preparePost(teams, recipients, messageType, message, entityID, t, details)
	if err != nil {
		return err
	}

	resp, err := http.Post(url, "application/json", post)
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
			Message string `json:"message"`
		}
		r := &response{Message: fmt.Sprintf("failed to understand OpsGenie response. code: %d content: %s", resp.StatusCode, string(body))}
		b := bytes.NewReader(body)
		dec := json.NewDecoder(b)
		dec.Decode(r)
		return errors.New(r.Message)
	}
	return nil
}

func (s *Service) preparePost(teams []string, recipients []string, messageType, message, entityID string, t time.Time, details models.Result) (string, io.Reader, error) {
	c := s.config()
	if !c.Enabled {
		return "", nil, errors.New("service is not enabled")
	}

	ogData := make(map[string]interface{})
	url := c.URL + "/"

	ogData["apiKey"] = c.APIKey
	ogData["entity"] = entityID
	ogData["alias"] = entityID
	ogData["message"] = message
	ogData["note"] = ""
	ogData["monitoring_tool"] = "kapacitor"

	//Extra Fields (can be used for filtering)
	ogDetails := make(map[string]interface{})
	ogDetails["Level"] = messageType
	ogDetails["Monitoring Tool"] = "Kapacitor"

	ogData["details"] = ogDetails

	switch messageType {
	case "RECOVERY":
		url = c.RecoveryURL + "/"
		ogData["note"] = message
	}

	b, err := json.Marshal(details)
	if err != nil {
		return "", nil, err
	}
	ogData["description"] = string(b)

	if len(teams) == 0 {
		teams = c.Teams
	}

	if len(teams) > 0 {
		ogData["teams"] = teams
	}

	if len(recipients) == 0 {
		recipients = c.Recipients
	}

	if len(recipients) > 0 {
		ogData["recipients"] = recipients
	}

	// Post data to VO
	var post bytes.Buffer
	enc := json.NewEncoder(&post)
	err = enc.Encode(ogData)
	if err != nil {
		return "", nil, err
	}

	return url, &post, nil
}

type HandlerConfig struct {
	// OpsGenie Teams.
	TeamsList []string `mapstructure:"teams-list"`

	// OpsGenie Recipients.
	RecipientsList []string `mapstructure:"recipients-list"`
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
	var messageType string
	switch event.State.Level {
	case alert.OK:
		messageType = "RECOVERY"
	default:
		messageType = event.State.Level.String()
	}
	if err := h.s.Alert(
		h.c.TeamsList,
		h.c.RecipientsList,
		messageType,
		event.State.Message,
		event.State.ID,
		event.State.Time,
		event.Data.Result,
	); err != nil {
		h.diag.Error("failed to send event to OpsGenie", err)
	}
}
