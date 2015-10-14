package influxdb

import (
	"net/url"
	"time"
)

type Config struct {
	URLs          []string            `toml:"urls"`
	Username      string              `toml:"username"`
	Password      string              `toml:"password"`
	Timeout       time.Duration       `toml:"timeout"`
	Precision     string              `toml:"precision"`
	Subscriptions map[string][]string `toml:"subscriptions"`
	Dir           string              `toml:"dir"`
}

func NewConfig() Config {
	return Config{
		URLs:          []string{"http://localhost:8086"},
		Username:      "",
		Password:      "",
		Precision:     "s",
		Subscriptions: make(map[string][]string),
		Dir:           "subscriptions",
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
