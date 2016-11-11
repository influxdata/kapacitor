package alert

import (
	"fmt"
	"time"

	"github.com/influxdata/influxdb/influxql"
)

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
	Level    Level
	Data     influxql.Result
}

type Level int

const (
	OK Level = iota
	Info
	Warning
	Critical
)

func (l Level) String() string {
	switch l {
	case OK:
		return "OK"
	case Info:
		return "INFO"
	case Warning:
		return "WARNING"
	case Critical:
		return "CRITICAL"
	default:
		panic("unknown AlertLevel")
	}
}

func (l Level) MarshalText() ([]byte, error) {
	return []byte(l.String()), nil
}

func (l *Level) UnmarshalText(text []byte) error {
	s := string(text)
	switch s {
	case "OK":
		*l = OK
	case "INFO":
		*l = Info
	case "WARNING":
		*l = Warning
	case "CRITICAL":
		*l = Critical
	default:
		return fmt.Errorf("unknown AlertLevel %s", s)
	}
	return nil
}
