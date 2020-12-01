package teams

import (
	"net/url"

	"github.com/pkg/errors"
)

type Config struct {
	// Whether Teams integration is enabled.
	Enabled bool `toml:"enabled" override:"enabled"`
	// The incoming (to Teams) channel webhook URL.
	ChannelURL string `toml:"channel-url" override:"channel-url"`
	// Whether all alerts should automatically post to Teams.
	Global bool `toml:"global" override:"global"`
	// Whether all alerts should automatically use stateChangesOnly mode.
	// Only applies if global is also set.
	StateChangesOnly bool `toml:"state-changes-only" override:"state-changes-only"`
}

func NewConfig() Config {
	return Config{}
}

func (c Config) Validate() error {
	if c.Enabled && c.ChannelURL == "" {
		return errors.New("must specify Teams channel webhook URL")
	}
	if _, err := url.Parse(c.ChannelURL); err != nil {
		return errors.Wrapf(err, "invalid url %q", c.ChannelURL)
	}
	return nil
}
