package kapacitor

import (
	"errors"
	"time"

	"github.com/influxdata/influxdb/services/meta"
)

type NoopMetaClient struct{}

func (m *NoopMetaClient) WaitForLeader(d time.Duration) error {
	return nil
}
func (m *NoopMetaClient) CreateDatabase(name string) (*meta.DatabaseInfo, error) {
	return nil, nil
}
func (m *NoopMetaClient) CreateDatabaseWithRetentionPolicy(name string, rpi *meta.RetentionPolicySpec) (*meta.DatabaseInfo, error) {
	return nil, nil
}
func (m *NoopMetaClient) CreateRetentionPolicy(database string, rpi *meta.RetentionPolicySpec) (*meta.RetentionPolicyInfo, error) {
	return nil, nil
}
func (m *NoopMetaClient) Database(name string) *meta.DatabaseInfo {
	return &meta.DatabaseInfo{
		Name: name,
	}
}
func (m *NoopMetaClient) RetentionPolicy(database, name string) (*meta.RetentionPolicyInfo, error) {
	return nil, nil
}
func (m *NoopMetaClient) Authenticate(username, password string) (ui *meta.UserInfo, err error) {
	return nil, errors.New("not authenticated")
}
func (m *NoopMetaClient) Users() ([]meta.UserInfo, error) {
	return nil, errors.New("no user")
}
