package slack

import (
	"net/url"

	"github.com/influxdata/kapacitor/listmap"
	"github.com/pkg/errors"
)

const DefaultUsername = "kapacitor"

type Config struct {
	// Whether Slack integration is enabled.
	Enabled bool `toml:"enabled" override:"enabled"`
	// Whether this is the default slack config.
	Default bool `toml:"enabled" override:"default"`
	// ID assigned if multiple slack configs are given
	Workspace string `toml:"workspace"  override:"workspace"`
	// The Slack webhook URL, can be obtained by adding Incoming Webhook integration.
	URL string `toml:"url" override:"url,redact"`
	// The default channel, can be overridden per alert.
	Channel string `toml:"channel" override:"channel"`
	// The username of the Slack bot.
	// Default: kapacitor
	Username string `toml:"username" override:"username"`
	// IconEmoji uses an emoji instead of the normal icon for the message.
	// The contents should be the name of an emoji surrounded with ':', i.e. ':chart_with_upwards_trend:'
	IconEmoji string `toml:"icon-emoji" override:"icon-emoji"`
	// Whether all alerts should automatically post to slack
	Global bool `toml:"global" override:"global"`
	// Whether all alerts should automatically use stateChangesOnly mode.
	// Only applies if global is also set.
	StateChangesOnly bool `toml:"state-changes-only" override:"state-changes-only"`

	// Path to CA file
	SSLCA string `toml:"ssl-ca" override:"ssl-ca"`
	// Path to host cert file
	SSLCert string `toml:"ssl-cert" override:"ssl-cert"`
	// Path to cert key file
	SSLKey string `toml:"ssl-key" override:"ssl-key"`
	// Use SSL but skip chain & host verification
	InsecureSkipVerify bool `toml:"insecure-skip-verify" override:"insecure-skip-verify"`
}

func NewConfig() Config {
	return Config{
		Username: DefaultUsername,
	}
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

type Configs []Config

func (cs *Configs) UnmarshalTOML(data interface{}) error {
	return listmap.DoUnmarshalTOML(cs, data)
}

func (cs Configs) Validate() error {
	l := len(cs)
	for _, c := range cs {
		if err := c.Validate(); err != nil {
			return err
		}
		// ID must not be empty when we have more than one.
		if l > 1 && c.Workspace == "" {
			return errors.New("id must not be empty")
		}
	}
	return nil
}
