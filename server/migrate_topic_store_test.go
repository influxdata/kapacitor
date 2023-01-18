package server_test

import (
	"errors"
	"github.com/influxdata/kapacitor/services/alert"
	"testing"

	_ "github.com/influxdata/kapacitor/services/alert"
	"github.com/influxdata/kapacitor/services/alert/alerttest"
	"github.com/influxdata/kapacitor/services/storage/storagetest"
	bolt "go.etcd.io/bbolt"
)

func mustDB(db *storagetest.BoltDB, err error) *bolt.DB {
	if err != nil {
		panic(err)
	}
	return db.DB
}

// TODO(DSB): What's the right format for test cases here?  We don't
// really want to keep V1 definitions for DAO just to generate test cases, do we?

type topicData struct {
	topic  string
	events alerttest.EventStateSpec
}

func Test_migrate_topicstore_v1_v2(t *testing.T) {
	tests := []struct {
		name    string
		data    []alert.TopicState
		wantErr error
	}{
		{
			name: "two topics",
			data: []alert.TopicState{
				{
					Topic:       "t1",
					EventStates: alerttest.MakeEventStates(alerttest.EventStateSpec{100, 5, 15}),
				},
				{
					Topic:       "t2",
					EventStates: alerttest.MakeEventStates(alerttest.EventStateSpec{130, 6, 12}),
				},
			},
			wantErr: nil,
		},
		{
			name: "three topics",
			data: []alert.TopicState{
				{
					Topic:       "t1",
					EventStates: alerttest.MakeEventStates(alerttest.EventStateSpec{100, 5, 15}),
				},
				{
					Topic:       "t2",
					EventStates: alerttest.MakeEventStates(alerttest.EventStateSpec{130, 6, 12}),
				},
				{
					Topic:       "t3",
					EventStates: alerttest.MakeEventStates(alerttest.EventStateSpec{50, 6, 17}),
				},
			},
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create default config
			c := NewConfig(t)
			s := OpenServer(c)
			cli := Client(s)
			_ = cli
			defer s.Close()
			// Create V1 topic store.
			TopicStatesDAO, err := alert.NewTopicStateKV(s.AlertService.StorageService.Store(alert.AlertNameSpace))
			if err != nil {
				t.Errorf("cannot create version one topc store: %v", err)
			}
			for _, ts := range tt.data {
				if err = TopicStatesDAO.Put(ts); err != nil {
					t.Errorf("cannot save version one topic store test data: %v", err)
				}
			}
			err = s.AlertService.MigrateTopicStore()
			// TODO (DSB): Check against originals, convert to V1, check against originals, etc.
			if !(tt.wantErr == err || errors.Is(err, tt.wantErr)) {
				t.Errorf("migrate_topicstore_v1_v2() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_migrate_topicstore_v2_v1(t *testing.T) {
	tests := []struct {
		name    string
		db      *bolt.DB
		wantErr bool
	}{{
		name: "test1",
		db:   mustDB(storagetest.NewBolt()),
	},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create default config
			c := NewConfig(t)
			s := OpenServer(c)
			cli := Client(s)
			_ = cli
			defer s.Close()

			if err := alert.MigrateTopicStoreV2V1(s.StorageService); (err != nil) != tt.wantErr {
				t.Errorf("migrate_topicstore_v2_v1() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
