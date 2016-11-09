package alert

import (
	"time"

	"github.com/influxdata/influxdb/influxql"
)

type Service struct {
	topics map[string]Topic
	// Map topic name -> []Handler
	handlers map[string][]Handler
}

func (s *Service) Collect(event Event) {

}

func (s *Service) RegisterHandler(topics []string, h Handler) {
}

func (s *Service) DeregisterHandler(topics []string, h Handler) {
}

func (s *Service) TopicsStatus(pattern string, minLevel AlertLevel) map[string]AlertLevel {
}

func (s *Service) TopicsStatusDetails(pattern string, minLevel AlertLevel) map[string]map[string]EventState {
}

type Topic struct {
	Name   string
	Events map[string]EventState
}

func (t *Topic) MaxLevel() AlertLevel {
}

func (t *Topic) UpdateEvent(id string, state EventState) {
}

type Event struct {
	ID    string
	Topic string
	State EventState
}

type Handler interface {
	Handle(event Event)
}

type EventState struct {
	Message  string
	Details  string
	Time     time.Time
	Duration time.Duration
	Level    AlertLevel
	Data     influxql.Result
}
