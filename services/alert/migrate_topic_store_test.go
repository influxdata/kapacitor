package alert_test

import (
	"errors"
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
func Test_migrate_topicstore_v1_v2(t *testing.T) {
	tests := []struct {
		name string
		alerttest.EventStateSpec
		wantErr error
	}{
		{
			"basic",
			alerttest.EventStateSpec{N: 25, Mwc: 5, Dwc: 15},
			nil,
		},
		// TODO(DSB): Add test cases.
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create default config
			c := NewConfig(t)
			s := OpenServer(c)
			cli := Client(s)
			_ = cli
			// TODO(DSB) - load test cases into V2 database
			// convert them into V1 format, convert them back into V2 format, check against original examples.
			defer s.Close()
			if err := s.MigrateTopicStore(); (tt.wantErr == err) || errors.Is(err, tt.wantErr) {
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

			if err := s.MigrateTopicStoreV2V1(tt.db); (err != nil) != tt.wantErr {
				t.Errorf("migrate_topicstore_v2_v1() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
