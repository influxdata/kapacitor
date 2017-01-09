package snmptrap

import "errors"

type Config struct {
	// Whether Snmptrap is enabled.
	Enabled bool `toml:"enabled" override:"enabled"`
	// The host:port address of the SNMP trap server
	Addr string `toml:"addr" override:"addr"`
	// SNMP Community
	Community string `toml:"community" override:"community,redact"`
	// Retries count for traps
	Retries int `toml:"retries" override:"retries"`
}

func NewConfig() Config {
	return Config{
		Addr:      "localhost:162",
		Community: "kapacitor",
		Retries:   1,
	}
}

func (c Config) Validate() error {
	if c.Enabled && c.Addr == "" {
		return errors.New("must specify addr")
	}
	return nil
}
