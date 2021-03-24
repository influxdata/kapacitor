package alert

import (
	"testing"
	"time"

	"github.com/influxdata/kapacitor/alert"
)

func TestMatchHandlerAlertDuration(t *testing.T) {
	cases := []struct {
		name        string
		event       alert.Event
		shouldMatch bool
		match       string
	}{{
		event: alert.Event{
			Topic: "topi",
			State: alert.EventState{
				ID:       "woot",
				Message:  "message",
				Details:  "details",
				Time:     time.Now(),
				Duration: time.Second * 30,
				Level:    alert.Critical,
			},
			Data: alert.EventData{},
		},
		shouldMatch: true,
		match:       "alertDuration() < 1m",
	}, {
		event: alert.Event{
			Topic: "topi",
			State: alert.EventState{
				ID:       "woot",
				Message:  "message",
				Details:  "details",
				Time:     time.Now(),
				Duration: time.Second * 130,
				Level:    alert.Critical,
			},
			Data: alert.EventData{},
		},
		shouldMatch: false,
		match:       "alertDuration() < 1m",
	}}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			var h alert.Handler
			mh, err := newMatchHandler(testCase.match, h, nil)
			if err != nil {
				t.Fatal(err)
			}

			matched, err := mh.match(testCase.event)
			if err != nil {
				t.Fatal(err)
			}
			if matched != testCase.shouldMatch {
				if matched {
					t.Errorf("expected event to match %s but it didn't", testCase.match)
				} else {
					t.Errorf("expected event to not match %s but it did", testCase.match)
				}
			}
		})
	}
}
