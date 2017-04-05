package scraper

import (
	"errors"
	"net/url"
	"regexp"
	"time"

	"github.com/influxdata/influxdb/toml"
)

const (
	// Maximum time to try and connect to InfluxDB during startup.
	DefaultStartUpTimeout           = 5 * time.Minute
	DefaultSubscriptionSyncInterval = 1 * time.Minute

	DefaultSubscriptionProtocol = "http"
)

type Config struct {
	Enabled                  bool          `toml:"enabled" override:"enabled"`
	Name                     string        `toml:"name" override:"name"`
	URLs                     []string      `toml:"urls" override:"urls"`
	Timeout                  toml.Duration `toml:"timeout" override:"timeout"`
	HTTPPort                 int           `toml:"http-port" override:"http-port"`
	StartUpTimeout           toml.Duration `toml:"startup-timeout" override:"startup-timeout"`
	SubscriptionSyncInterval toml.Duration `toml:"subscriptions-sync-interval" override:"subscriptions-sync-interval"`
}

func NewConfig() Config {
	c := &Config{}
	c.Init()
	c.Enabled = true
	return *c
}

func (c *Config) Init() {
	c.Name = "default"
	c.URLs = []string{"http://localhost:8086"}
	c.StartUpTimeout = toml.Duration(DefaultStartUpTimeout)
	c.SubscriptionSyncInterval = toml.Duration(DefaultSubscriptionSyncInterval)
}

func (c *Config) ApplyConditionalDefaults() {
	if c.StartUpTimeout == 0 {
		c.StartUpTimeout = toml.Duration(DefaultStartUpTimeout)
	}
	if c.SubscriptionSyncInterval == toml.Duration(0) {
		c.SubscriptionSyncInterval = toml.Duration(DefaultSubscriptionSyncInterval)
	}
}

var validNamePattern = regexp.MustCompile(`^[-\._\p{L}0-9]+$`)

func (c Config) Validate() error {
	if c.Name == "" {
		return errors.New("influxdb cluster must be given a name")
	}
	if !validNamePattern.MatchString(c.Name) {
		return errors.New("influxdb cluster name must contain only numbers, letters or '.', '-', and '_' characters")
	}
	if len(c.URLs) == 0 {
		return errors.New("must specify at least one InfluxDB URL")
	}
	for _, u := range c.URLs {
		_, err := url.Parse(u)
		if err != nil {
			return err
		}
	}
	return nil
}
