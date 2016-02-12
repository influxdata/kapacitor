package stats

import (
	"time"

	"github.com/influxdata/enterprise-client/v2"
	"github.com/influxdata/influxdb/toml"
)

const (
	DefaultDatabse         = "_kapacitor"
	DefaultRetentionPolicy = "default"
	DefaultStatsInterval   = toml.Duration(10 * time.Second)
)

type Config struct {
	Enabled         bool           `toml:"enabled"`
	StatsInterval   toml.Duration  `toml:"stats-interval"`
	Database        string         `toml:"database"`
	RetentionPolicy string         `toml:"retention-policy"`
	EnterpriseHosts []*client.Host `toml:"enterprise-hosts"`
	AdminPort       uint16         `toml:"admin-port"`
	AdminHost       string         `toml:"admin-host"`
}

func NewConfig() Config {
	return Config{
		Enabled:         true,
		Database:        DefaultDatabse,
		RetentionPolicy: DefaultRetentionPolicy,
		StatsInterval:   DefaultStatsInterval,
		AdminPort:       9095,
		AdminHost:       "localhost",
	}
}
