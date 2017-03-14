package alert

import (
	"bytes"
	"fmt"
	"strings"
	"time"

	"github.com/influxdata/kapacitor/models"
)

type Event struct {
	Topic         string
	State         EventState
	Data          EventData
	NoExternal    bool
	previousState EventState
}

func (e Event) PreviousState() EventState {
	return e.previousState
}

func (e Event) TemplateData() TemplateData {
	return TemplateData{
		ID:       e.State.ID,
		Message:  e.State.Message,
		Level:    e.State.Level.String(),
		Time:     e.State.Time,
		Duration: e.State.Duration,
		Name:     e.Data.Name,
		TaskName: e.Data.TaskName,
		Group:    e.Data.Group,
		Tags:     e.Data.Tags,
		Fields:   e.Data.Fields,
	}
}

type Handler interface {
	// Handle is responsible for taking action on the event.
	Handle(event Event)
}

type EventState struct {
	ID       string
	Message  string
	Details  string
	Time     time.Time
	Duration time.Duration
	Level    Level
}

type EventData struct {
	// Measurement name
	Name string

	// TaskName is the name of the task that generated this event.
	TaskName string

	// Concatenation of all group-by tags of the form [key=value,]+.
	// If not groupBy is performed equal to literal 'nil'
	Group string

	// Map of tags
	Tags map[string]string

	// Fields of alerting data point.
	Fields map[string]interface{}

	Result models.Result
}

// TemplateData is a structure containing all information available to use in templates for an Event.
type TemplateData struct {
	// The ID of the alert.
	ID string

	// The Message of the Alert
	Message string

	// Alert Level, one of: INFO, WARNING, CRITICAL.
	Level string

	// Time the event occurred.
	Time time.Time

	// Duration of the event
	Duration time.Duration

	// Measurement name
	Name string

	// Task name
	TaskName string

	// Concatenation of all group-by tags of the form [key=value,]+.
	// If not groupBy is performed equal to literal 'nil'
	Group string

	// Map of tags
	Tags map[string]string

	// Fields of alerting data point.
	Fields map[string]interface{}
}

type Level int

const (
	OK Level = iota
	Info
	Warning
	Critical
	maxLevel
)

const levelStrings = "OKINFOWARNINGCRITICAL"

var levelBytes = []byte(levelStrings)

var levelOffsets = []int{0, 2, 6, 13, 21}

func (l Level) String() string {
	if l < maxLevel {
		return levelStrings[levelOffsets[l]:levelOffsets[l+1]]
	}
	return "unknown"
}

func (l Level) MarshalText() ([]byte, error) {
	return []byte(l.String()), nil
}

func (l *Level) UnmarshalText(text []byte) error {
	idx := bytes.Index(levelBytes, text)
	if idx >= 0 {
		for i := 0; i < int(maxLevel); i++ {
			if idx == levelOffsets[i] {
				*l = Level(i)
				return nil
			}
		}
	}

	return fmt.Errorf("unknown alert level '%s'", text)
}

func ParseLevel(s string) (l Level, err error) {
	err = l.UnmarshalText([]byte(strings.ToUpper(s)))
	return
}

type TopicState struct {
	Level     Level
	Collected int64
}
