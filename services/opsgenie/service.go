package opsgenie

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/influxdata/kapacitor/services/alert"
)

type Service struct {
	configValue atomic.Value
	logger      *log.Logger
}

func NewService(c Config, l *log.Logger) *Service {
	s := &Service{
		logger: l,
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
		nil,
		o.Teams,
		o.Recipients,
		o.MessageType,
		o.Message,
		o.EntityID,
		time.Now(),
		nil,
	)
}

func (s *Service) Alert(ctxt context.Context, teams []string, recipients []string, messageType, message, entityID string, t time.Time, details interface{}) error {
	url, post, err := s.preparePost(teams, recipients, messageType, message, entityID, t, details)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", url, post)
	req.Header.Set("Content-Type", "application/json")
	if ctxt != nil {
		req = req.WithContext(ctxt)
	}
	resp, err := http.DefaultClient.Do(req)
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

func (s *Service) preparePost(teams []string, recipients []string, messageType, message, entityID string, t time.Time, details interface{}) (string, io.Reader, error) {
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

	if details != nil {
		b, err := json.Marshal(details)
		if err != nil {
			return "", nil, err
		}
		ogData["description"] = string(b)
	}

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
	err := enc.Encode(ogData)
	if err != nil {
		return "", nil, err
	}

	return url, &post, nil
}

type HandlerConfig struct {
	// OpsGenie Teams.
	// tick:ignore
	TeamsList []string

	// OpsGenie Recipients.
	// tick:ignore
	RecipientsList []string
}

type handler struct {
	s *Service
	c HandlerConfig
}

func (s *Service) Handler(c HandlerConfig) alert.Handler {
	return &handler{
		s: s,
		c: c,
	}
}

func (h *handler) Name() string {
	return "OpsGenie"
}

func (h *handler) Handle(ctxt context.Context, event alert.Event) error {
	var messageType string
	switch event.State.Level {
	case alert.OK:
		messageType = "RECOVERY"
	default:
		messageType = event.State.Level.String()
	}
	return h.s.Alert(
		ctxt,
		h.c.TeamsList,
		h.c.RecipientsList,
		messageType,
		event.State.Message,
		event.State.ID,
		event.State.Time,
		event.Data.Result,
	)
}
