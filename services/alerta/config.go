package alerta

import (
	"net/url"

	"github.com/pkg/errors"
)

type Config struct {
	// Whether Alerta integration is enabled.
	Enabled bool `toml:"enabled"`
	// The Alerta URL.
	URL string `toml:"url"`
	// The authentication token for this notification, can be overridden per alert.
	Token string `toml:"token" override:",redact"`
	// The environment in which to raise the alert.
	Environment string `toml:"environment"`
	// The origin of the alert.
	Origin string `toml:"origin"`
}

func NewConfig() Config {
	return Config{}
}

func (c Config) Validate() error {
	if c.Enabled && c.URL == "" {
		return errors.New("must specify url")
	}
	if _, err := url.Parse(c.URL); err != nil {
		return errors.Wrapf(err, "invalid url %q", c.URL)
	}
	return nil
}
