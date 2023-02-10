package server_test

import (
	"fmt"
	"testing"

	"github.com/influxdata/kapacitor/services/alert"
	"github.com/influxdata/kapacitor/services/alert/alerttest"
	"github.com/influxdata/kapacitor/services/storage"
)

type testData struct {
	name                string
	topicEventStatesMap map[string]map[string]alert.EventState
}

var tests []testData = []testData{
	{
		name: "one topic",
		topicEventStatesMap: map[string]map[string]alert.EventState{
			"t1": alerttest.MakeEventStates(alerttest.EventStateSpec{N: 300, Mwc: 6, Dwc: 20}),
		},
	},
	{
		name: "three topics",
		topicEventStatesMap: map[string]map[string]alert.EventState{
			"t1": alerttest.MakeEventStates(alerttest.EventStateSpec{N: 100, Mwc: 5, Dwc: 15}),
			"t2": alerttest.MakeEventStates(alerttest.EventStateSpec{N: 130, Mwc: 6, Dwc: 12}),
			"t3": alerttest.MakeEventStates(alerttest.EventStateSpec{N: 50, Mwc: 6, Dwc: 17}),
		},
	},
	{
		name: "two topics",
		topicEventStatesMap: map[string]map[string]alert.EventState{
			"t1": alerttest.MakeEventStates(alerttest.EventStateSpec{N: 100, Mwc: 5, Dwc: 15}),
			"t2": alerttest.MakeEventStates(alerttest.EventStateSpec{N: 130, Mwc: 6, Dwc: 12}),
		},
	},
}

func Test_migrate_topicstore(t *testing.T) {
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create default config
			c := NewConfig(t)
			// Force this for the test.
			c.Alert.PersistTopics = true
			s := OpenServer(c)
			Client(s)
			defer s.Close()
			// Create V1 topic store.
			TopicStatesDAO, err := alert.NewTopicStateKV(s.AlertService.StorageService.Store(alert.AlertNameSpace))
			if err != nil {
				t.Errorf("cannot create version one topic store: %v", err)
			}
			// Put the test data in the V1 topic store
			for topic, es := range tt.topicEventStatesMap {
				if err = TopicStatesDAO.Put(alert.TopicState{Topic: topic, EventStates: es}); err != nil {
					t.Errorf("cannot save version one topic store test data for topic %q: %v", topic, err)
				}
			}
			err = alert.DeleteV2TopicStore(s.AlertService.StorageService.Store(alert.TopicStatesNameSpace))
			if err != nil {
				t.Errorf("cannot delete version two topic store: %v", err)
			}
			err = s.StorageService.Versions().Set(alert.TopicStoreVersionKey, "")
			if err != nil {
				t.Errorf("cannot reset version in topic store: %v", err)
			}
			// Convert the V1 topic Store to a V2 topic store
			err = s.AlertService.MigrateTopicStoreV1V2()
			if err != nil {
				t.Errorf("failure migrating topic store from version one to version two: %v", err)
			}

			// Check that the topic store version was updated
			version, err := s.StorageService.Versions().Get(alert.TopicStoreVersionKey)
			if err != nil {
				t.Errorf("cannot retrieve version from topic store: %v", err)
			}
			if version != alert.TopicStoreVersion2 {
				t.Errorf("topic store version: expected: %q, got: %q", alert.TopicStoreVersion2, version)
			}

			count := 0
			err = alert.WalkTopicBuckets(s.AlertService.StorageService.Store(alert.TopicStatesNameSpace), func(tx storage.ReadOnlyTx, topic string) error {
				esStoredV2, err := alert.LoadTopicBucket(tx, []byte(topic))
				if err != nil {
					return err
				}
				count++
				if esOriginal, ok := tt.topicEventStatesMap[topic]; !ok {
					return fmt.Errorf("topic %q not found in version two store: %w", topic, alert.ErrNoTopicStateExists)
				} else if ok, msg := eventStateMapCompare(esOriginal, esStoredV2); !ok {
					return fmt.Errorf("event states for topic %q differ between original and V2 storage: %s", topic, msg)
				}
				return nil
			})
			if err != nil {
				t.Errorf("migration V1 to V2 error: %v", err)
			} else if count != len(tt.topicEventStatesMap) {
				t.Errorf("wrong number of store topics.  Expected %d, got %d", len(tt.topicEventStatesMap), count)
			}
			err = alert.MigrateTopicStoreV2V1(s.StorageService)
			if err != nil {
				t.Errorf("migration V2 to V1 error: %v", err)
			}
			// Load all the saved topic states (plus one in case of error or duplicates in saving).
			topicStates, err := TopicStatesDAO.List("", 0, len(tt.topicEventStatesMap)+1)
			if err != nil {
				t.Errorf("failed to load saved topic states: %v", err)
			}
			count = 0
			for _, ts := range topicStates {
				if esOriginal, ok := tt.topicEventStatesMap[ts.Topic]; !ok {
					t.Errorf("topic %q not found in version one store: %v", ts.Topic, alert.ErrNoTopicStateExists)
				} else if ok, msg := eventStateMapCompare(esOriginal, ts.EventStates); !ok {
					t.Errorf("event states for topic %q differ between V2 storage and original: %s", ts.Topic, msg)
				} else {
					count++
				}
			}
			if count != len(tt.topicEventStatesMap) {
				t.Errorf("wrong number of store topics. Expected %d, got %d", len(tt.topicEventStatesMap), count)
			}
		})
	}
}

func eventStateMapCompare(em1, em2 map[string]alert.EventState) (bool, string) {
	for id, es1 := range em1 {
		if es2, ok := em2[id]; !ok {
			return false, fmt.Sprintf("second map missing id: %q", id)
		} else if match, msg := eventStateCompare(&es1, &es2); !match {
			return match, msg
		}
	}
	for id := range em2 {
		if _, ok := em1[id]; !ok {
			return false, fmt.Sprintf("first map missing id: %q", id)
		}
	}
	return true, ""
}

func eventStateCompare(es1, es2 *alert.EventState) (bool, string) {
	if es1.Level != es2.Level {
		return false, fmt.Sprintf("EventState.Level differs: %v != %v", es1.Level, es2.Level)
	} else if es1.Message != es2.Message {
		return false, fmt.Sprintf("EventState.Message differs: %q != %q", es1.Message, es2.Message)
	} else if fmt.Sprintf("%v", es1.Time) != fmt.Sprintf("%v", es2.Time) {
		// This is a hack to avoid JSON loss of precision causing test failures
		return false, fmt.Sprintf("EventState.Time differs: %v != %v", es1.Time, es2.Time)
	} else if es1.Duration != es2.Duration {
		return false, fmt.Sprintf("EventState.Duration differs: %v != %v", es1.Duration, es2.Duration)
	} else if es1.Details != es2.Details {
		return false, fmt.Sprintf("EventState.Details differ: %q != %q", es1.Details, es2.Details)
	} else {
		return true, ""
	}
}
