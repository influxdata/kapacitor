package slack

import (
	"net/url"

	"github.com/pkg/errors"
)

type Config struct {
	// Whether Slack integration is enabled.
	Enabled bool `toml:"enabled"`
	// The Slack webhook URL, can be obtained by adding Incoming Webhook integration.
	URL string `toml:"url" override:",redact"`
	// The default channel, can be overridden per alert.
	Channel string `toml:"channel"`
	// Whether all alerts should automatically post to slack
	Global bool `toml:"global"`
	// Whether all alerts should automatically use stateChangesOnly mode.
	// Only applies if global is also set.
	StateChangesOnly bool `toml:"state-changes-only"`
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
