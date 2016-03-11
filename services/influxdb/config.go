package influxdb

import (
	"net/url"
	"time"

	"github.com/influxdata/influxdb/toml"
	"github.com/influxdata/kapacitor/services/stats"
	"github.com/influxdata/kapacitor/services/udp"
)

const (
	// Maximum time to try and connect to InfluxDB during startup.
	DefaultStartUpTimeout = time.Minute * 5
)

type Config struct {
	Enabled               bool                `toml:"enabled"`
	URLs                  []string            `toml:"urls"`
	Username              string              `toml:"username"`
	Password              string              `toml:"password"`
	Timeout               toml.Duration       `toml:"timeout"`
	Subscriptions         map[string][]string `toml:"subscriptions"`
	ExcludedSubscriptions map[string][]string `toml:"excluded-subscriptions"`
	UDPBuffer             int                 `toml:"udp-buffer"`
	UDPReadBuffer         int                 `toml:"udp-read-buffer"`
	StartUpTimeout        toml.Duration       `toml:"startup-timeout"`
}

func NewConfig() Config {
	return Config{
		Enabled:       true,
		URLs:          []string{"http://localhost:8086"},
		Username:      "",
		Password:      "",
		Subscriptions: make(map[string][]string),
		ExcludedSubscriptions: map[string][]string{
			stats.DefaultDatabse: []string{stats.DefaultRetentionPolicy},
		},
		UDPBuffer:      udp.DefaultBuffer,
		StartUpTimeout: toml.Duration(DefaultStartUpTimeout),
	}
}

func (c Config) Validate() error {
	for _, u := range c.URLs {
		_, err := url.Parse(u)
		if err != nil {
			return err
		}
	}
	return nil
}
