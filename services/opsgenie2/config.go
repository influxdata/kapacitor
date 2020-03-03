package opsgenie2

import (
	"net/url"

	"github.com/pkg/errors"
)

const DefaultOpsGenieAPIURL = "https://api.opsgenie.com/v2/alerts"
const DefaultOpsGenieRecoveryAction = "notes"

type Config struct {
	// Whether to enable OpsGenie integration.
	Enabled bool `toml:"enabled" override:"enabled"`
	// The OpsGenie API key.
	APIKey string `toml:"api-key" override:"api-key,redact"`
	// The default Teams, can be overridden per alert.
	Teams []string `toml:"teams" override:"teams"`
	// The default Teams, can be overridden per alert.
	Recipients []string `toml:"recipients" override:"recipients"`
	// The OpsGenie API URL, should not need to be changed.
	URL string `toml:"url" override:"url"`
	// The OpsGenie Recovery action, may be one of:
	//    * notes -- add note on recovery
	//    * close -- close alert on recovery
	RecoveryAction string `toml:"recovery_action" override:"recovery_action"`
	// Whether every alert should automatically go to OpsGenie.
	Global bool `toml:"global" override:"global"`
}

func NewConfig() Config {
	return Config{
		URL:            DefaultOpsGenieAPIURL,
		RecoveryAction: DefaultOpsGenieRecoveryAction,
	}
}

func (c Config) Validate() error {
	if c.URL == "" {
		return errors.New("url cannot be empty")
	}
	if c.RecoveryAction == "" {
		return errors.New("recovery_action cannot be empty")
	}
	if _, err := url.Parse(c.URL); err != nil {
		return errors.Wrapf(err, "invalid URL %q", c.URL)
	}

	if c.Enabled && c.APIKey == "" {
		return errors.New("api-key cannot be empty")
	}
	return nil
}
