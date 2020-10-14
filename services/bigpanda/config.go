package bigpanda

import (
	"github.com/pkg/errors"
	"net/url"
)

const (
	defaultBigPandaAlertApi = "https://api.bigpanda.io/data/v2/alerts"
)

type Config struct {
	// Whether BigPanda integration is enabled.
	Enabled bool `toml:"enabled" override:"enabled"`

	// Whether all alerts should automatically post to Teams.
	Global bool `toml:"global" override:"global"`

	//Each integration must have an App Key in BigPanda to identify it as a unique source.
	AppKey string `toml:"app-key" override:"app-key"`

	//Each integration must have an App Key in BigPanda to identify it as a unique source.
	Token string `toml:"token" override:"token,redact"`

	// Whether all alerts should automatically use stateChangesOnly mode.
	// Only applies if global is also set.
	StateChangesOnly bool `toml:"state-changes-only" override:"state-changes-only"`

	// Whether to skip the tls verification of the alerta host
	InsecureSkipVerify bool `toml:"insecure-skip-verify" override:"insecure-skip-verify"`

	//Optional alert api URL, if not specified https://api.bigpanda.io/data/v2/alerts is used
	URL string `toml:"url" override:"url"`
}

func NewConfig() Config {
	return Config{
		URL: defaultBigPandaAlertApi,
	}
}

func (c Config) Validate() error {
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
