package foo

import "errors"

// Config declares the needed configuration options for the service Foo.
type Config struct {
	// Enabled indicates whether the service should be enabled.
	Enabled bool `toml:"enabled" override:"enabled"`
	// URL of the Foo server.
	URL string `toml:"url" override:"url"`
	// Room specifies the default room to use for all handlers.
	Room string `toml:"room" override:"room"`
}

func NewConfig() Config {
	return Config{}
}

func (c Config) Validate() error {
	if c.Enabled && c.URL == "" {
		return errors.New("must specify the Foo server URL")
	}
	return nil
}
