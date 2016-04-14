package talk

import (
	"net/url"
)

type Config struct {
	// Whether Talk integration is enabled.
	Enabled bool `toml:"enabled"`
	// The Talk webhook URL, can be obtained by adding Incoming Webhook integration.
	URL string `toml:"url"`
	// The default authorName, can be overridden per alert.
	AuthorName string `toml:"author_name"`
}

func NewConfig() Config {
	return Config{}
}

func (c Config) Validate() error {
	_, err := url.Parse(c.URL)
	if err != nil {
		return err
	}
	return nil
}
