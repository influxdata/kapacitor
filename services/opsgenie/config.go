package opsgenie

import (
	"net/url"

	"github.com/pkg/errors"
)

const DefaultOpsGenieAPIURL = "https://api.opsgenie.com/v1/json/alert"
const DefaultOpsGenieRecoveryURL = "https://api.opsgenie.com/v1/json/alert/note"

type Config struct {
	// Whether to enable OpsGenie integration.
	Enabled bool `toml:"enabled"`
	// The OpsGenie API key.
	APIKey string `toml:"api-key" override:",redact"`
	// The default Teams, can be overridden per alert.
	Teams []string `toml:"teams"`
	// The default Teams, can be overridden per alert.
	Recipients []string `toml:"recipients"`
	// The OpsGenie API URL, should not need to be changed.
	URL string `toml:"url"`
	// The OpsGenie Recovery URL, you can change this based on which behavior you want a recovery to trigger (Add Notes, Close Alert, etc.)
	RecoveryURL string `toml:"recovery_url"`
	// Whether every alert should automatically go to OpsGenie.
	Global bool `toml:"global"`
}

func NewConfig() Config {
	return Config{
		URL:         DefaultOpsGenieAPIURL,
		RecoveryURL: DefaultOpsGenieRecoveryURL,
	}
}

func (c Config) Validate() error {
	if c.URL == "" {
		return errors.New("url cannot be empty")
	}
	if c.RecoveryURL == "" {
		return errors.New("recovery_url cannot be empty")
	}
	if _, err := url.Parse(c.URL); err != nil {
		return errors.Wrapf(err, "invalid URL %q", c.URL)
	}
	if _, err := url.Parse(c.RecoveryURL); err != nil {
		return errors.Wrapf(err, "invalid recovery_url %q", c.URL)
	}
	if c.Enabled && c.APIKey == "" {
		return errors.New("api-key cannot be empty")
	}
	return nil
}
