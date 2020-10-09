package bigpanda

import (
	"github.com/pkg/errors"
)

type Config struct {
	// Whether BigPanda integration is enabled.
	Enabled bool `toml:"enabled" override:"enabled"`

	// Whether all alerts should automatically post to Teams.
	Global bool `toml:"global" override:"global"`

	//Each integration must have an App Key in BigPanda to identify it as a unique source.
	AppKey string `toml:"app-key" override:"app-key"`

	//Each integration must have an App Key in BigPanda to identify it as a unique source.
	Token string `toml:"token" override:"token"`

	// Whether all alerts should automatically use stateChangesOnly mode.
	// Only applies if global is also set.
	StateChangesOnly bool `toml:"state-changes-only" override:"state-changes-only"`
}

func NewConfig() Config {
	return Config{}
}

func (c Config) Validate() error {
	if c.Enabled && c.AppKey == "" {
		return errors.New("must specify BigPanda AppKey")
	}
	return nil
}
