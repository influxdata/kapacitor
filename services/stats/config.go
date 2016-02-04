package stats

import (
	"time"

	"github.com/influxdb/influxdb/toml"
)

const (
	DefaultDatabse         = "_kapacitor"
	DefaultRetentionPolicy = "default"
	DefaultStatsInterval   = toml.Duration(10 * time.Second)
)

type Config struct {
	Enabled         bool          `toml:"enabled"`
	StatsInterval   toml.Duration `toml:"stats-interval"`
	Database        string        `toml:"database"`
	RetentionPolicy string        `toml:"retention-policy"`
}

func NewConfig() Config {
	return Config{
		Enabled:         true,
		Database:        DefaultDatabse,
		RetentionPolicy: DefaultRetentionPolicy,
		StatsInterval:   DefaultStatsInterval,
	}
}
