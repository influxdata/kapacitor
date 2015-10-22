package influxdb

import (
	"net/url"
	"time"
)

type Config struct {
	Enabled       bool
	URLs          []string            `toml:"urls"`
	Username      string              `toml:"username"`
	Password      string              `toml:"password"`
	Timeout       time.Duration       `toml:"timeout"`
	Subscriptions map[string][]string `toml:"subscriptions"`
}

func NewConfig() Config {
	return Config{
		Enabled:       true,
		URLs:          []string{"http://localhost:8086"},
		Username:      "",
		Password:      "",
		Subscriptions: make(map[string][]string),
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
