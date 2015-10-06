package influxdb

import (
	"net/url"
	"time"
)

type Config struct {
	URLs      []string
	Username  string
	Password  string
	Timeout   time.Duration
	Precision string
}

func NewConfig() Config {
	return Config{
		URLs:      []string{"http://localhost:8086"},
		Username:  "",
		Password:  "",
		Precision: "s",
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
