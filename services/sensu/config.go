package sensu

import (
	"net/url"
)

const DefaultSource = "Kapacitor"

type Config struct {
	// Whether Sensu integration is enabled.
	Enabled bool `toml:"enabled"`
	// The Sensu URL.
	URL string `toml:"url"`
	// The JIT sensu source name of the alert.
	Source string `toml:"source"`
}

func NewConfig() Config {
	return Config{
		Source: DefaultSource,
	}
}

func (c Config) Validate() error {
	_, err := url.Parse(c.URL)
	if err != nil {
		return err
	}
	return nil
}
