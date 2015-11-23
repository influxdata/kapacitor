package pagerduty

const DefaultPagerDutyAPIURL = "https://events.pagerduty.com/generic/2010-04-15/create_event.json"

type Config struct {
	// Whether PagerDuty integration is enabled.
	Enabled bool `toml:"enabled"`
	// The PagerDuty API URL, should not need to be changed.
	URL string `toml:"url"`
	// The PagerDuty service key.
	ServiceKey string `toml:"service-key"`
	// Whether every alert should automatically go to PagerDuty
	Global bool `toml:"global"`
}

func NewConfig() Config {
	return Config{
		URL: DefaultPagerDutyAPIURL,
	}
}
