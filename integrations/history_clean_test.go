package integrations

import (
	"fmt"
	"testing"
	"time"

	"github.com/influxdata/kapacitor/alert"
	salert "github.com/influxdata/kapacitor/services/alert"
	"github.com/influxdata/kapacitor/services/storage/storagetest"
)

func TestAlertHistory_Persistence(t *testing.T) {
	const topic = "Big Alert Topic"
	const alertID = "The special ID"

	const (
		criticalAlert1 = iota
		resetAlert1
		warningAlert2
	)

	as := salert.NewService(diagService.NewAlertServiceHandler(), nil, 0)
	as.PersistTopics = true
	store := storagetest.New(t, diagService.NewStorageHandler())
	as.StorageService = store
	as.HTTPDService = newHTTPDService()
	if err := as.Open(); err != nil {
		t.Error(err)
	}
	defer checkDeferredErrors(t, store.Close)()
	defer checkDeferredErrors(t, as.Close)

	events := []alert.Event{
		{
			Topic: topic,
			State: alert.EventState{
				ID:       alertID,
				Message:  "Critical Message",
				Details:  "Critical details",
				Time:     time.Now(),
				Duration: 0,
				Level:    alert.Critical,
			},
			NoExternal: false,
		},
		{
			Topic: topic,
			State: alert.EventState{
				ID:       alertID,
				Message:  "Reset to OK message",
				Details:  "Reset to OK details",
				Time:     time.Now(),
				Duration: 0,
				Level:    alert.OK,
			},
			NoExternal: false,
		},
		{
			Topic: topic,
			State: alert.EventState{
				ID:       alertID + " new ID",
				Message:  "New Alert ID",
				Details:  "Second alert ID details",
				Time:     time.Now(),
				Duration: 0,
				Level:    alert.Warning,
			},
			NoExternal: false,
		},
	}

	err := as.Collect(events[criticalAlert1])
	if err != nil {
		t.Error(err)
	}
	checkTopicState(t, as, topic, true, alert.Critical, 1)
	if err := as.Collect(events[resetAlert1]); err != nil {
		t.Error(err)
	}
	// Still have one event state, because on-disk copy was deleted, but not in-memory
	checkTopicState(t, as, topic, true, alert.OK, 1)

	if err := as.CloseTopic(topic); err != nil {
		t.Error(err)
	}
	// Now have no event states, because restoration from disk had zero
	// because event states are deleted on reset to OK status.
	checkTopicState(t, as, topic, false, alert.OK, 0)

	if err := as.RestoreTopic(topic); err != nil {
		t.Error(err)
	}
	checkTopicState(t, as, topic, true, alert.OK, 0)

	err = as.Collect(events[warningAlert2])
	if err != nil {
		t.Error(err)
	}
	checkTopicState(t, as, topic, true, alert.Warning, 1)

	err = as.Collect(events[criticalAlert1])
	if err != nil {
		t.Error(err)
	}
	checkTopicState(t, as, topic, true, alert.Critical, 2)
	if err := as.CloseTopic(topic); err != nil {
		t.Error(err)
	}
	checkTopicState(t, as, topic, false, alert.OK, 0)

	if err := as.RestoreTopic(topic); err != nil {
		t.Error(err)
	}
	checkTopicState(t, as, topic, true, alert.Critical, 2)
}

func checkTopicState(t *testing.T, as *salert.Service, topic string, okStatus bool, level alert.Level, countStates int) {
	state, ok, err := as.TopicState(topic)
	if err != nil {
		t.Error(err)
	} else if ok != okStatus {
		t.Errorf("expected topic %q existence to be %v, got %v", topic, okStatus, ok)
	} else if state.Level != level {
		t.Errorf("expected topic level to be %s, got %s", level.String(), state.Level.String())
	} else if !ok {
		return
	} else if es, err := as.EventStates(topic, alert.OK); err != nil {
		t.Error(fmt.Errorf("EventStates(%q) failed: %w", topic, err))
	} else if len(es) != countStates {
		t.Errorf("expected %d event states, got %d", countStates, len(es))
	}
}
