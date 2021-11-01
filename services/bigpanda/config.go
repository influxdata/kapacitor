package bigpanda

import (
	"net/url"

	"github.com/pkg/errors"
)

const (
	defaultBigPandaAlertApi = "https://api.bigpanda.io/data/v2/alerts"
	defaultAutoAttributes   = "tags,fields"
)

type Config struct {
	// Whether BigPanda integration is enabled.
	Enabled bool `toml:"enabled" override:"enabled"`

	// Whether all alerts should automatically post to BigPanda.
	Global bool `toml:"global" override:"global"`

	// Each integration must have an App Key in BigPanda to identify it as a unique source.
	AppKey string `toml:"app-key" override:"app-key"`

	// Each integration must have an App Key in BigPanda to identify it as a unique source.
	Token string `toml:"token" override:"token,redact"`

	// Whether all alerts should automatically use stateChangesOnly mode.
	// Only applies if global is also set.
	StateChangesOnly bool `toml:"state-changes-only" override:"state-changes-only"`

	// Whether to skip the tls verification
	InsecureSkipVerify bool `toml:"insecure-skip-verify" override:"insecure-skip-verify"`

	// BigPanda Alert API URL, if not specified https://api.bigpanda.io/data/v2/alerts is used.
	URL string `toml:"url" override:"url"`

	// Option to control tags and fields serialization into payload (for backward compatibility).
	AutoAttributes string `toml:"auto-attributes" override:"auto-attributes"`
}

func NewConfig() Config {
	return Config{
		URL:            defaultBigPandaAlertApi,
		AutoAttributes: defaultAutoAttributes,
	}
}

func (c Config) Validate() error {
	if c.Enabled && c.URL == "" {
		return errors.New("must specify the BigPanda webhook URL")
	}
	if c.Enabled && c.AppKey == "" {
		return errors.New("must specify BigPanda app-key")
	}
	if _, err := url.Parse(c.URL); err != nil {
		return errors.Wrapf(err, "invalid url %q", c.URL)
	}
	if c.Enabled && c.Token == "" {
		return errors.New("must specify BigPanda token")
	}

	return nil
}
