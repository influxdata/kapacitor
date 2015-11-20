package stats

import (
	"time"

	"github.com/influxdb/influxdb/toml"
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
		Database:        "_kapacitor",
		RetentionPolicy: "default",
		StatsInterval:   toml.Duration(10 * time.Second),
	}
}
