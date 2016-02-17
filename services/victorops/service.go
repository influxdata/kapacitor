package victorops

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
	routingKey string
	url        string
	global     bool
	logger     *log.Logger
}

func NewService(c Config, l *log.Logger) *Service {
	return &Service{
		routingKey: c.RoutingKey,
		url:        c.URL + "/" + c.APIKey + "/",
		global:     c.Global,
		logger:     l,
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

func (s *Service) Alert(routingKey, messageType, message, entityID string, t time.Time, details interface{}) error {
	voData := make(map[string]interface{})
	voData["message_type"] = messageType
	voData["entity_id"] = entityID
	voData["entity_display_name"] = message
	voData["timestamp"] = t.Unix()
	voData["monitoring_tool"] = kapacitor.Product
	if details != nil {
		b, err := json.Marshal(details)
		if err != nil {
			return err
		}
		voData["data"] = string(b)
	}

	if routingKey == "" {
		routingKey = s.routingKey
	}

	// Post data to VO
	var post bytes.Buffer
	enc := json.NewEncoder(&post)
	err := enc.Encode(voData)
	if err != nil {
		return err
	}

	resp, err := http.Post(s.url+routingKey, "application/json", &post)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusNotFound {
			return errors.New("URL or API key not found: 404")
		}
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		type response struct {
			Message string `json:"message"`
		}
		r := &response{Message: fmt.Sprintf("failed to understand VictorOps response. code: %d content: %s", resp.StatusCode, string(body))}
		b := bytes.NewReader(body)
		dec := json.NewDecoder(b)
		dec.Decode(r)
		return errors.New(r.Message)
	}
	return nil
}
