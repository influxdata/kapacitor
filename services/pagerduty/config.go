package pagerduty

import (
	"net/url"

	"github.com/pkg/errors"
)

const DefaultPagerDutyAPIURL = "https://events.pagerduty.com/generic/2010-04-15/create_event.json"

type Config struct {
	// Whether PagerDuty integration is enabled.
	Enabled bool `toml:"enabled" override:"enabled"`
	// The PagerDuty API URL, should not need to be changed.
	URL string `toml:"url" override:"url"`
	// The PagerDuty service key.
	ServiceKey string `toml:"service-key" override:"service-key,redact"`
	// Whether every alert should automatically go to PagerDuty
	Global bool `toml:"global" override:"global"`
}

func NewConfig() Config {
	return Config{
		URL: DefaultPagerDutyAPIURL,
	}
}

func (c Config) Validate() error {
	if c.URL == "" {
		return errors.New("url cannot be empty")
	}
	if _, err := url.Parse(c.URL); err != nil {
		return errors.Wrapf(err, "invalid URL %q", c.URL)
	}
	return nil
}
