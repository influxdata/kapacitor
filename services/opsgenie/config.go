package opsgenie

import (
	"net/url"

	"github.com/pkg/errors"
)

const DefaultOpsGenieAPIURL = "https://api.opsgenie.com/v2/alerts"

type Config struct {
	// Whether to enable OpsGenie integration.
	Enabled bool `toml:"enabled" override:"enabled"`
	// The OpsGenie API key.
	APIKey string `toml:"api-key" override:"api-key,redact"`
	// The default Teams, can be overridden per alert.
	Teams []string `toml:"teams" override:"teams"`
	// The default Teams, can be overridden per alert.
	Recipients []string `toml:"recipients" override:"recipients"`
	// The default Entity, used to specify which domain alert is related to.
	Entity string `toml:"entity" override:"entity"`
	// The OpsGenie API URL, should not need to be changed.
	URL string `toml:"url" override:"url"`
	// Whether every alert should automatically go to OpsGenie.
	Global bool `toml:"global" override:"global"`
}

func NewConfig() Config {
	return Config{
		URL: DefaultOpsGenieAPIURL,
	}
}

func (c Config) Validate() error {
	if c.URL == "" {
		return errors.New("url cannot be empty")
	}
	if _, err := url.Parse(c.URL); err != nil {
		return errors.Wrapf(err, "invalid URL %q", c.URL)
	}
	if c.Enabled && c.APIKey == "" {
		return errors.New("api-key cannot be empty")
	}
	return nil
}
