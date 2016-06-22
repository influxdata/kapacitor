package stats

import (
	"time"

	"github.com/influxdata/influxdb/toml"
)

const (
	DefaultDatabse                 = "_kapacitor"
	DefaultRetentionPolicy         = "autogen"
	DefaultStatsInterval           = toml.Duration(10 * time.Second)
	DefaultTimingSampleRate        = 0.10
	DefaultTimingMovingAverageSize = 1000
)

type Config struct {
	Enabled                 bool          `toml:"enabled"`
	StatsInterval           toml.Duration `toml:"stats-interval"`
	Database                string        `toml:"database"`
	RetentionPolicy         string        `toml:"retention-policy"`
	TimingSampleRate        float64       `toml:"timing-sample-rate"`
	TimingMovingAverageSize int           `toml:"timing-movavg-size"`
}

func NewConfig() Config {
	return Config{
		Enabled:                 true,
		Database:                DefaultDatabse,
		RetentionPolicy:         DefaultRetentionPolicy,
		StatsInterval:           DefaultStatsInterval,
		TimingSampleRate:        DefaultTimingSampleRate,
		TimingMovingAverageSize: DefaultTimingMovingAverageSize,
	}
}
