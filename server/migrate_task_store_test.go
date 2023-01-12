package server_test

import (
	"testing"

	"github.com/influxdata/kapacitor/server"
	"github.com/influxdata/kapacitor/services/storage/storagetest"

	"go.etcd.io/bbolt"
	bolt "go.etcd.io/bbolt"
)

func mustDB(db *storagetest.BoltDB, err error) *bolt.DB {
	if err != nil {
		panic(err)
	}
	return db.DB
}
func Test_migrate_topicstore_v1_v2(t *testing.T) {
	tests := []struct {
		name    string
		db      *bbolt.DB
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create default config
			c := NewConfig(t)
			s := OpenServer(c)
			cli := Client(s)
			_ = cli
			defer s.Close()
			if err := server.MigrateTopicStoreV1V2(tt.db); (err != nil) != tt.wantErr {
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

			if err := server.MigrateTopicStoreV2V1(tt.db); (err != nil) != tt.wantErr {
				t.Errorf("migrate_topicstore_v2_v1() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
