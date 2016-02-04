package influxdb

import (
	"net/url"
	"time"

	"github.com/influxdata/kapacitor/services/stats"
	"github.com/influxdata/kapacitor/services/udp"
)

type Config struct {
	Enabled               bool                `toml:"enabled"`
	URLs                  []string            `toml:"urls"`
	Username              string              `toml:"username"`
	Password              string              `toml:"password"`
	Timeout               time.Duration       `toml:"timeout"`
	Subscriptions         map[string][]string `toml:"subscriptions"`
	ExcludedSubscriptions map[string][]string `toml:"excluded-subscriptions"`
	UDPBuffer             int                 `toml:"udp-buffer"`
	UDPReadBuffer         int                 `toml:"udp-read-buffer"`
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
		UDPBuffer: udp.DefaultBuffer,
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
