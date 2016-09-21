package victorops

import (
	"net/url"

	"github.com/pkg/errors"
)

const DefaultVictorOpsAPIURL = "https://alert.victorops.com/integrations/generic/20131114/alert"

type Config struct {
	// Whether to enable Victor Ops integration.
	Enabled bool `toml:"enabled"`
	// The Victor Ops API key.
	APIKey string `toml:"api-key" override:",redact"`
	// The default Routing Key, can be overridden per alert.
	RoutingKey string `toml:"routing-key"`
	// The Victor Ops API URL, should not need to be changed.
	URL string `toml:"url"`
	// Whether every alert should automatically go to VictorOps.
	Global bool `toml:"global"`
}

func NewConfig() Config {
	return Config{
		URL: DefaultVictorOpsAPIURL,
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
