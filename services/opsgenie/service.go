package opsgenie

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"io/ioutil"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/influxdata/kapacitor/alert"
	"github.com/influxdata/kapacitor/keyvalue"
)

type Diagnostic interface {
	WithContext(ctx ...keyvalue.T) Diagnostic

	Error(msg string, err error)
}

type Service struct {
	configValue atomic.Value
	diag        Diagnostic
	client      http.Client
}

func NewService(c Config, d Diagnostic) *Service {
	s := &Service{
		diag: d,
	}
	s.configValue.Store(c)
	s.client = http.Client{}
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
	Alias       string   `json:"entity-id"`
	Message     string   `json:"message"`
	Priority    string   `json:"priority"`
	Description string   `json:"details"`
	Teams       []string `json:"teams"`
	Recipients  []string `json:"recipients"`
	Entity      string   `json:"entity"`
}

func (s *Service) TestOptions() interface{} {
	c := s.config()
	return &testOptions{
		Alias:       "testEntityID",
		Message:     "test opsgenie message",
		Priority:    "P2",
		Description: "",
		Teams:       c.Teams,
		Recipients:  c.Recipients,
		Entity:      c.Entity,
	}
}

func (s *Service) Test(options interface{}) error {
	o, ok := options.(*testOptions)
	if !ok {
		return fmt.Errorf("unexpected options type %T", options)
	}
	return s.Alert(
		o.Alias,
		o.Message,
		o.Priority,
		o.Description,
		o.Teams,
		o.Recipients,
		o.Entity,
		time.Now(),
	)
}

func (s *Service) runRequest(method string, url string, post io.Reader) error {
	c := s.config()

	req, err := http.NewRequest(method, url, post)
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("GenieKey %s", c.APIKey))

	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// read entire body to reuse connection
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusAccepted {
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

func (s *Service) preparePost(alias, message, priority, description string, teams, recipients []string, entity string, t time.Time) (io.Reader, error) {
	c := s.config()

	ogData := make(map[string]interface{})

	ogData["alias"] = alias
	ogData["message"] = message
	ogData["priority"] = priority
	ogData["description"] = html.UnescapeString(description)
	ogData["teams"] = teams
	ogData["recipients"] = recipients
	ogData["entity"] = c.Entity

	var post bytes.Buffer
	enc := json.NewEncoder(&post)
	err := enc.Encode(ogData)
	if err != nil {
		return nil, err
	}

	return &post, nil
}

func (s *Service) Alert(alias, message, priority, description string, teams, recipients []string, entity string, t time.Time) error {
	c := s.config()
	if !c.Enabled {
		return errors.New("service is not enabled")
	}

	post, err := s.preparePost(alias, message, priority, description, teams, recipients, entity, t)
	if err != nil {
		return err
	}

	// Run initial create
	if err := s.runRequest(http.MethodPost, c.URL, post); err != nil {
		return err
	}

	// Run Priority update
	url := c.URL + "/" + alias + "/priority?identifierType=alias"
	payload := bytes.NewBufferString(fmt.Sprintf(`{"priority":"%s"}`, priority))
	if err := s.runRequest(http.MethodPut, url, payload); err != nil {
		return err
	}

	return nil
}

func (s *Service) Recover(entityID string) error {
	c := s.config()
	url := c.URL
	post := bytes.NewBufferString(`{}`)
	url += "/" + entityID + "/close?identifierType=alias"
	return s.runRequest(http.MethodPost, url, post)
}

type HandlerConfig struct {
	// OpsGenie Teams.
	TeamsList []string `mapstructure:"teams-list"`

	// OpsGenie Recipients.
	RecipientsList []string `mapstructure:"recipients-list"`

	// OpsGenie Entity.
	Entity string `mapstructure:"entity"`
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
	c := h.s.config()
	if !c.Enabled {
		h.diag.Error("wouldn't send event to OpsGenie", errors.New("service is not enabled"))
		return
	}

	if event.State.Level == alert.OK {
		err := h.s.Recover(event.State.ID)
		if err != nil {
			h.diag.Error("failed to recover at OpsGenie", err)
		}
		return
	}

	priorityMap := map[alert.Level]string{
		alert.Info:     "P5",
		alert.Warning:  "P3",
		alert.Critical: "P2",
	}

	teams := h.c.TeamsList
	if len(teams) == 0 {
		teams = c.Teams
	}
	recipients := h.c.RecipientsList
	if len(recipients) == 0 {
		recipients = c.Recipients
	}
	entity := h.c.Entity
	if len(entity) == 0 {
		entity = c.Entity
	}

	err := h.s.Alert(
		event.State.ID,
		event.State.Message,
		priorityMap[event.State.Level],
		event.State.Details,
		teams,
		recipients,
		entity,
		event.State.Time,
	)
	if err != nil {
		h.diag.Error("failed to send event to OpsGenie", err)
	}
}
