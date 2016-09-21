package talk

import (
	"net/url"

	"github.com/pkg/errors"
)

type Config struct {
	// Whether Talk integration is enabled.
	Enabled bool `toml:"enabled"`
	// The Talk webhook URL, can be obtained by adding Incoming Webhook integration.
	URL string `toml:"url" override:",redact"`
	// The default authorName, can be overridden per alert.
	AuthorName string `toml:"author_name"`
}

func NewConfig() Config {
	return Config{}
}

func (c Config) Validate() error {
	if c.Enabled && c.URL == "" {
		return errors.New("must specify url")
	}
	if _, err := url.Parse(c.URL); err != nil {
		return errors.Wrapf(err, "invalid url %q", c.URL)
	}
	return nil
}
