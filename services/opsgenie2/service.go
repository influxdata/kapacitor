package opsgenie2

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"sync/atomic"
	"time"

	"github.com/influxdata/kapacitor/alert"
	"github.com/influxdata/kapacitor/keyvalue"
	"github.com/influxdata/kapacitor/models"
	"github.com/pkg/errors"
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
	var level alert.Level
	if o.MessageType == "RECOVERY" {
		level = alert.OK
	} else {
		l, err := alert.ParseLevel(o.MessageType)
		if err != nil {
			return err
		}
		level = l
	}
	return s.Alert(
		o.Teams,
		o.Recipients,
		level,
		o.Message,
		o.EntityID,
		time.Now(),
		models.Result{},
	)
}

func (s *Service) Alert(teams []string, recipients []string, level alert.Level, message, entityID string, t time.Time, details models.Result) error {
	req, err := s.preparePost(teams, recipients, level, message, entityID, t, details)
	if err != nil {
		return errors.Wrap(err, "failed to prepare API request")
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return errors.Wrap(err, "failed to execute API request")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return errors.Wrap(err, "failed to read API response")
		}
		type response struct {
			Message string `json:"message"`
		}
		r := &response{Message: fmt.Sprintf("failed to understand OpsGenie response. code: %d content: %s", resp.StatusCode, string(body))}
		b := bytes.NewReader(body)
		dec := json.NewDecoder(b)
		dec.Decode(r)
		return fmt.Errorf("opsgenie error: %s", r.Message)
	}
	return nil
}

func (s *Service) preparePost(teams []string, recipients []string, level alert.Level, message, entityID string, t time.Time, details models.Result) (*http.Request, error) {
	c := s.config()
	if !c.Enabled {
		return nil, errors.New("service is not enabled")
	}

	alias := base64.URLEncoding.EncodeToString([]byte(entityID))

	ogData := make(map[string]interface{})
	u := c.URL
	switch level {
	case alert.OK:
		recoveryURL, err := url.Parse(c.URL)
		if err != nil {
			return nil, err
		}
		recoveryURL.Path = path.Join(recoveryURL.Path, alias, c.RecoveryAction)
		recoveryURL.RawQuery = "identifierType=alias"
		u = recoveryURL.String()
		ogData["note"] = message
	default:
		var priority string
		switch level {
		case alert.Info:
			priority = "P5"
		case alert.Warning:
			priority = "P3"
		case alert.Critical:
			priority = "P1"
		}

		ogData["entity"] = entityID
		ogData["alias"] = alias
		ogData["message"] = message
		ogData["note"] = ""
		ogData["priority"] = priority

		// Encode details as description
		b, err := json.Marshal(details)
		if err != nil {
			return nil, err
		}
		ogData["description"] = string(b)

		//Extra Fields (can be used for filtering)
		ogDetails := make(map[string]string)
		ogDetails["Monitoring Tool"] = "Kapacitor"
		ogDetails["Level"] = level.String()

		if len(details.Series) > 0 {
			row := details.Series[0]
			for k, v := range row.Tags {
				ogDetails[k] = v
			}
			ogDetails["Kapacitor Task Name"] = row.Name
		}

		ogData["details"] = ogDetails

		// Create responders list
		var responders []map[string]string
		if len(teams) == 0 {
			teams = c.Teams
		}
		if len(teams) > 0 {
			for _, t := range teams {
				responders = append(responders, map[string]string{
					"name": t,
					"type": "team",
				})
			}
		}

		if len(recipients) == 0 {
			recipients = c.Recipients
		}
		if len(recipients) > 0 {
			for _, u := range recipients {
				responders = append(responders, map[string]string{
					"username": u,
					"type":     "user",
				})
			}
		}

		if len(responders) > 0 {
			ogData["responders"] = responders
		}
	}

	var post bytes.Buffer
	enc := json.NewEncoder(&post)
	if err := enc.Encode(ogData); err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", u, &post)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "GenieKey "+c.APIKey)

	return req, nil
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
	if err := h.s.Alert(
		h.c.TeamsList,
		h.c.RecipientsList,
		event.State.Level,
		event.State.Message,
		event.State.ID,
		event.State.Time,
		event.Data.Result,
	); err != nil {
		h.diag.Error("failed to send event to OpsGenie", err)
	}
}
