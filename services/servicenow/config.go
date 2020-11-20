package servicenow

import (
	"net/url"

	"github.com/pkg/errors"
)

type Config struct {
	// Whether ServiceNow integration is enabled.
	Enabled bool `toml:"enabled" override:"enabled"`
	// ServiceNow events API URL.
	URL string `toml:"url" override:"url"`
	// Event source.
	Source string `toml:"source" override:"source"`
	// ServiceNow authentication username.
	Username string `toml:"username" override:"username"`
	// ServiceNow authentication password.
	Password string `toml:"password" override:"password,redact"`
	// Whether all alerts should automatically post to ServiceNow.
	Global bool `toml:"global" override:"global"`
	// Whether all alerts should automatically use stateChangesOnly mode.
	// Only applies if global is also set.
	StateChangesOnly bool `toml:"state-changes-only" override:"state-changes-only"`
}

func NewConfig() Config {
	return Config{
		URL: "https://instance.service-now.com/api/global/em/jsonv2", // dummy default
	}
}

func (c Config) Validate() error {
	if c.Enabled && c.URL == "" {
		return errors.New("must specify events URL")
	}
	if _, err := url.Parse(c.URL); err != nil {
		return errors.Wrapf(err, "invalid url %q", c.URL)
	}
	return nil
}
