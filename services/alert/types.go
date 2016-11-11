package alert

import (
	"bytes"
	"fmt"
	"time"

	"github.com/influxdata/influxdb/influxql"
)

type Event struct {
	Topic string
	State EventState
}

type Handler chan Event

type EventState struct {
	ID       string
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
