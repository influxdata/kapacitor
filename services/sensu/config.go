package sensu

import "errors"

const DefaultSource = "Kapacitor"

type Config struct {
	// Whether Sensu integration is enabled.
	Enabled bool `toml:"enabled"`
	// The Sensu client host:port address.
	Addr string `toml:"addr"`
	// The JIT sensu source name of the alert.
	Source string `toml:"source"`
}

func NewConfig() Config {
	return Config{
		Source: DefaultSource,
	}
}

func (c Config) Validate() error {
	if c.Enabled && c.Addr == "" {
		return errors.New("must specify client address")
	}
	return nil
}
