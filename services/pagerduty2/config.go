package pagerduty2

import (
	"net/url"

	"github.com/pkg/errors"
)

// DefaultPagerDuty2APIURL is the default URL for the v2 API
const DefaultPagerDuty2APIURL = "https://events.pagerduty.com/v2/enqueue"

// Config is the default struct for the PagerDuty v2 plugin
type Config struct {
	// Whether PagerDuty integration is enabled.
	Enabled bool `toml:"enabled" override:"enabled"`
	// The PagerDuty API URL, should not need to be changed.
	URL string `toml:"url" override:"url"`
	// The PagerDuty routing key, this is associated with an Event v2 API integration service.
	RoutingKey string `toml:"routing-key" override:"routing-key,redact"`
	// Whether every alert should automatically go to PagerDuty
	Global bool `toml:"global" override:"global"`
}

// NewConfig returns a new instance of the primary config struct for PagerDuty
func NewConfig() Config {
	return Config{
		URL: DefaultPagerDuty2APIURL,
	}
}

// Validate is a bound method that checks/confirms whether the attached configuration is valid
func (c Config) Validate() error {
	if c.URL == "" {
		return errors.New("url cannot be empty")
	}
	if _, err := url.Parse(c.URL); err != nil {
		return errors.Wrapf(err, "invalid URL %q", c.URL)
	}
	return nil
}
