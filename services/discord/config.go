package discord

import (
	"net/url"

	"github.com/influxdata/kapacitor/listmap"
	"github.com/pkg/errors"
)

// Config object for Discord alert handler
type Config struct {
	// Whether Discord integration is enabled
	Enabled bool `toml:"enabled" override:"enabled"`
	// Whether this is the default discord config.
	Default bool `toml:"default" override:"default"`
	// ID assigned if multiple discord configs are given
	Workspace string `toml:"workspace" override:"workspace"`
	// Discord channel webhook URL
	URL string `toml:"url" override:"url"`
	// Whether the timestamp is included in the embed
	Timestamp bool `toml:"timestamp" override:"timestamp"`
	// Username of webhook
	Username string `toml:"username" override:"username"`
	// Avatar URL
	AvatarURL string `toml:"avatar-url" override:"avatar-url"`
	// Embed Title
	EmbedTitle string `toml:"embed-title" override:"embed-title"`
	// Whether all alerts should automatically post to discord
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

func NewDefaultConfig() Config {
	c := Config{}
	c.Default = true
	return c
}

func NewConfig() Config {
	c := Config{}
	return c
}

func (c Config) Validate() error {
	if c.Enabled && c.URL == "" {
		return errors.New("Must specify the Discord channel webhook URL")
	}
	if _, err := url.Parse(c.URL); err != nil {
		return errors.Wrapf(err, "invalid url %q", c.URL)
	}
	if c.AvatarURL != "" {
		if _, err := url.Parse(c.AvatarURL); err != nil {
			return errors.Wrapf(err, "invalid url %q", c.AvatarURL)
		}
	}
	return nil
}

type Configs []Config

func (cs *Configs) UnmarshalTOML(data interface{}) error {
	return listmap.DoUnmarshalTOML(cs, data)
}

func (cs Configs) Validate() error {
	l := len(cs)
	// if only one config, then it would be the default config
	hasDefault := l == 1
	for _, c := range cs {
		if err := c.Validate(); err != nil {
			return err
		}
		// ID must not be empty when we have more than one.
		if l > 1 && c.Workspace == "" {
			return errors.New("id must not be empty")
		}

		hasDefault = hasDefault || c.Default

		if c.Global && !c.Default {
			return errors.New("only the default config may be assigned as global")
		}

		if c.StateChangesOnly && (!c.Default || !c.Global) {
			return errors.New("stateChangesOnly may only be assigned when the config is both default and global")
		}
	}
	if len(cs) > 0 && !hasDefault {
		return errors.New("at least one Discord config must be set as default")
	}
	return nil
}
