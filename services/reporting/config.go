package reporting

import (
	"time"

	"github.com/influxdb/enterprise-client/v1"
	"github.com/influxdb/influxdb/toml"
)

type Config struct {
	Enabled       bool          `toml:"enabled"`
	EnterpriseURL string        `toml:"enterprise-url"`
	StatsInterval toml.Duration `toml:"stats-interval"`
}

func NewConfig() Config {
	return Config{
		Enabled:       true,
		EnterpriseURL: client.URL,
		StatsInterval: toml.Duration(time.Minute),
	}
}
