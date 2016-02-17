package opsgenie

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"github.com/influxdata/kapacitor"
)

type Service struct {
	apikey       string
	teams        []string
	recipients   []string
	url          string
	recovery_url string
	global       bool
	logger       *log.Logger
}

func NewService(c Config, l *log.Logger) *Service {
	return &Service{
		teams:        c.Teams,
		recipients:   c.Recipients,
		apikey:       c.APIKey,
		url:          c.URL + "/",
		recovery_url: c.RecoveryURL + "/",
		global:       c.Global,
		logger:       l,
	}
}

func (s *Service) Open() error {
	return nil
}

func (s *Service) Close() error {
	return nil
}

func (s *Service) Global() bool {
	return s.global
}

func (s *Service) Alert(teams []string, recipients []string, messageType, message, entityID string, t time.Time, details interface{}) error {
	ogData := make(map[string]interface{})
	url := s.url

	ogData["apiKey"] = s.apikey
	ogData["entity"] = entityID
	ogData["alias"] = entityID
	ogData["message"] = message
	ogData["note"] = ""
	ogData["monitoring_tool"] = kapacitor.Product

	//Extra Fields (can be used for filtering)
	ogDetails := make(map[string]interface{})
	ogDetails["Level"] = messageType
	ogDetails["Monitoring Tool"] = "Kapacitor"

	ogData["details"] = ogDetails

	switch messageType {
	case "RECOVERY":
		url = s.recovery_url
		ogData["note"] = message
	}

	if details != nil {
		b, err := json.Marshal(details)
		if err != nil {
			return err
		}
		ogData["description"] = string(b)
	}

	if len(teams) == 0 {
		teams = s.teams
	}

	if len(teams) > 0 {
		ogData["teams"] = teams
	}

	if len(recipients) == 0 {
		recipients = s.recipients
	}

	if len(recipients) > 0 {
		ogData["recipients"] = recipients
	}

	// Post data to VO
	var post bytes.Buffer
	enc := json.NewEncoder(&post)
	err := enc.Encode(ogData)
	if err != nil {
		return err
	}

	resp, err := http.Post(url, "application/json", &post)
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
