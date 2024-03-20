package server_test

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	alertcore "github.com/influxdata/kapacitor/alert"
	"github.com/influxdata/kapacitor/services/alert"
	"github.com/influxdata/kapacitor/services/alert/alerttest"
	"github.com/influxdata/kapacitor/services/storage"
)

type testData struct {
	name                  string
	topicEventStatesMap   map[string]map[string]alert.EventState
	v2IsTheSame           bool
	topicEventStatesV2Map map[string]map[string]alert.EventState
}

var tests []testData = []testData{
	{
		name: "one topic",
		topicEventStatesMap: map[string]map[string]alert.EventState{
			"t1": alerttest.MakeEventStates(alerttest.EventStateSpec{N: 300, Mwc: 6, Dwc: 20}),
		},
		v2IsTheSame: true,
	},
	{
		name: "three topics",
		topicEventStatesMap: map[string]map[string]alert.EventState{
			"t1": alerttest.MakeEventStates(alerttest.EventStateSpec{N: 100, Mwc: 5, Dwc: 15}),
			"t2": alerttest.MakeEventStates(alerttest.EventStateSpec{N: 130, Mwc: 6, Dwc: 12}),
			"t3": alerttest.MakeEventStates(alerttest.EventStateSpec{N: 50, Mwc: 6, Dwc: 17}),
		},
		v2IsTheSame: true,
	},
	{
		name: "two topics",
		topicEventStatesMap: map[string]map[string]alert.EventState{
			"t1": alerttest.MakeEventStates(alerttest.EventStateSpec{N: 100, Mwc: 5, Dwc: 15}),
			"t2": alerttest.MakeEventStates(alerttest.EventStateSpec{N: 130, Mwc: 6, Dwc: 12}),
		},
		v2IsTheSame: true,
	},
	{
		name: "EAR issue 4984", // fails with "cannot store event \"\" in topic \"main:batch:alert3\": key required"
		topicEventStatesMap: map[string]map[string]alert.EventState{
			"main:batch:alert3": {
				"": {
					Message:  "event state no ID",
					Details:  "event state no ID details",
					Time:     time.Unix(1710838800, 0),
					Duration: time.Duration(15 * time.Second),
					Level:    alertcore.OK,
				},
				"event_state_id_1": {
					Message:  "event state 1",
					Details:  "event state 1 details",
					Time:     time.Unix(1710838801, 0),
					Duration: time.Duration(15 * time.Second),
					Level:    alertcore.Warning,
				},
			},
		},
		v2IsTheSame: false,
		topicEventStatesV2Map: map[string]map[string]alert.EventState{
			"main:batch:alert3": { // event state with empty ID is not migrated
				"event_state_id_1": {
					Message:  "event state 1",
					Details:  "event state 1 details",
					Time:     time.Unix(1710838801, 0),
					Duration: time.Duration(15 * time.Second),
					Level:    alertcore.Warning,
				},
			},
		},
	},
}

func Test_Migrate_TopicStore(t *testing.T) {
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
				t.Fatalf("cannot create version one topic store: %v", err)
			}
			// Put the test data in the V1 topic store
			for topic, es := range tt.topicEventStatesMap {
				if err = TopicStatesDAO.Put(alert.TopicState{Topic: topic, EventStates: es}); err != nil {
					t.Fatalf("cannot save version one topic store test data for topic %q: %v", topic, err)
				}
			}
			err = alert.DeleteV2TopicStore(s.AlertService.StorageService.Store(alert.TopicStatesNameSpace))
			if err != nil {
				t.Fatalf("cannot delete version two topic store: %v", err)
			}
			err = s.StorageService.Versions().Set(alert.TopicStoreVersionKey, "")
			if err != nil {
				t.Fatalf("cannot reset version in topic store: %v", err)
			}
			// Convert the V1 topic Store to a V2 topic store
			err = s.AlertService.MigrateTopicStoreV1V2()
			if err != nil {
				t.Fatalf("failure migrating topic store from version one to version two: %v", err)
			}

			// Check that the topic store version was updated
			version, err := s.StorageService.Versions().Get(alert.TopicStoreVersionKey)
			if err != nil {
				t.Fatalf("cannot retrieve version from topic store: %v", err)
			}
			if version != alert.TopicStoreVersion2 {
				t.Fatalf("topic store version: expected: %q, got: %q", alert.TopicStoreVersion2, version)
			}

			var topicEventStatesMap map[string]map[string]alert.EventState
			if tt.v2IsTheSame {
				topicEventStatesMap = tt.topicEventStatesMap
			} else {
				topicEventStatesMap = tt.topicEventStatesV2Map
			}

			count := 0
			err = alert.WalkTopicBuckets(s.AlertService.StorageService.Store(alert.TopicStatesNameSpace), func(tx storage.ReadOnlyTx, topic string) error {
				esStoredV2, err := alert.LoadTopicBucket(tx, []byte(topic))
				if err != nil {
					return err
				}
				count++
				if esOriginal, ok := topicEventStatesMap[topic]; !ok {
					return fmt.Errorf("topic %q not found in version two store: %w", topic, alert.ErrNoTopicStateExists)
				} else if ok, msg := eventStateMapCompare(esOriginal, esStoredV2); !ok {
					return fmt.Errorf("event states for topic %q differ between original and V2 storage: %s", topic, msg)
				}
				return nil
			})
			if err != nil {
				t.Fatalf("migration V1 to V2 error: %v", err)
			} else if count != len(topicEventStatesMap) {
				t.Fatalf("wrong number of store topics.  Expected %d, got %d", len(topicEventStatesMap), count)
			}
			err = alert.MigrateTopicStoreV2V1(s.StorageService)

			if err != nil {
				t.Fatalf("migration V2 to V1 error: %v", err)
			}
			// Load all the saved topic states (plus one in case of error or duplicates in saving).
			topicStates, err := TopicStatesDAO.List("", 0, len(topicEventStatesMap)+1)
			if err != nil {
				t.Fatalf("failed to load saved topic states: %v", err)
			}
			count = 0
			for _, ts := range topicStates {
				if esOriginal, ok := topicEventStatesMap[ts.Topic]; !ok {
					t.Fatalf("topic %q not found in version one store: %v", ts.Topic, alert.ErrNoTopicStateExists)
				} else if ok, msg := eventStateMapCompare(esOriginal, ts.EventStates); !ok {
					t.Fatalf("event states for topic %q differ between V2 storage and original: %s", ts.Topic, msg)
				} else {
					count++
				}
			}
			if count != len(topicEventStatesMap) {
				t.Fatalf("wrong number of store topics. Expected %d, got %d", len(topicEventStatesMap), count)
			}
		})
	}
}

var errVersionSetFail error = errors.New("version set failure")

type storageTestService struct {
	alert.StorageService
	versions storageTestVersions
}

func (s *storageTestService) Versions() storage.Versions {
	return &s.versions
}

type storageTestVersions struct {
	storage.Versions
	setCount, setFailOn int
}

func (sv *storageTestVersions) Set(id string, version string) error {
	if sv.setFailOn == sv.setCount {
		return fmt.Errorf("on call %d: %w", sv.setFailOn+1, errVersionSetFail)
	} else {
		sv.setCount++
		return sv.Versions.Set(id, version)
	}
}

func newStorageTestService(sService alert.StorageService, setFailCount int) *storageTestService {
	return &storageTestService{
		StorageService: sService,
		versions: storageTestVersions{
			Versions:  sService.Versions(),
			setCount:  0,
			setFailOn: setFailCount,
		},
	}
}

func Test_MigrateTopicStoreFail(t *testing.T) {
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create default config
			c := NewConfig(t)
			// Force this for the test.
			c.Alert.PersistTopics = true
			s := OpenServer(c)
			Client(s)
			defer s.Close()

			// Wrap the StorageService in a testing decorator
			s.AlertService.StorageService = newStorageTestService(s.AlertService.StorageService, 0)
			// Create V1 topic store.
			TopicStatesDAO, err := alert.NewTopicStateKV(s.AlertService.StorageService.Store(alert.AlertNameSpace))
			if err != nil {
				t.Fatalf("cannot create version one topic store: %v", err)
			}
			// Put the test data in the V1 topic store
			for topic, es := range tt.topicEventStatesMap {
				if err = TopicStatesDAO.Put(alert.TopicState{Topic: topic, EventStates: es}); err != nil {
					t.Fatalf("cannot save version one topic store test data for topic %q: %v", topic, err)
				}
			}
			err = alert.DeleteV2TopicStore(s.AlertService.StorageService.Store(alert.TopicStatesNameSpace))
			if err != nil {
				t.Fatalf("cannot delete version two topic store: %v", err)
			}
			err = s.StorageService.Versions().Set(alert.TopicStoreVersionKey, "")
			if err != nil {
				t.Fatalf("cannot reset version in topic store: %v", err)
			}

			hash, err := HashFileSHA256(s.AlertService.StorageService.Path())
			if err != nil {
				t.Fatal(err)
			}
			// Convert the V1 topic Store to a V2 topic store and fail.
			err = s.AlertService.MigrateTopicStoreV1V2()
			if err == nil || !errors.Is(err, errVersionSetFail) {
				t.Fatalf("wrong or missing error. expected %v, got %v", errVersionSetFail, err)
			}

			backup := s.AlertService.StorageService.Path() + alert.TopicStoreBackupSuffix
			_, err = os.Stat(backup)
			if err == nil || !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("backup file %q should be deleted. expected %v, got %v", backup, os.ErrNotExist, err)
			}
			newHash, err := HashFileSHA256(s.AlertService.StorageService.Path())
			if err != nil {
				t.Fatal(err)
			} else if !bytes.Equal(hash, newHash) {
				t.Fatalf("restored BoltDB not the same as original: %q", s.AlertService.StorageService.Path())
			}
		})
	}
}

func HashFileSHA256(name string) ([]byte, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, fmt.Errorf("cannot open %q for hasing: %w", name, err)
	}
	hash := sha256.New()
	if _, err := io.Copy(hash, f); err != nil {
		return nil, fmt.Errorf("failed to compute hash for %q: %w", name, err)
	}
	return hash.Sum(nil), nil
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
