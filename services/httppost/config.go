package httppost

import (
	"net/url"

	"github.com/pkg/errors"
)

type BasicAuth struct {
	Username string `toml:"username" json:"username"`
	Password string `toml:"password" json:"password"`
}

func (b BasicAuth) valid() bool {
	return b.Username != "" && b.Password != ""
}

func (b BasicAuth) validate() error {
	if !b.valid() {
		return errors.New("basic-auth must set both \"username\" and \"password\" parameters")
	}

	return nil
}

// Config is the configuration for a single [[httppost]] section of the kapacitor
// configuration file.
type Config struct {
	Endpoint  string            `toml:"endpoint" override:"endpoint"`
	URL       string            `toml:"url" override:"url"`
	Headers   map[string]string `toml:"headers" override:"headers"`
	BasicAuth BasicAuth         `toml:"basic-auth" override:"basic-auth,redact"`
}

// Validate ensures that all configurations options are valid. The Endpoint,
// and URL parameters must be set to be considered valid.
func (c Config) Validate() error {
	if c.Endpoint == "" {
		return errors.New("must specify endpoint name")
	}

	if c.URL == "" {
		return errors.New("must specify url")
	}

	if _, err := url.Parse(c.URL); err != nil {
		return errors.Wrapf(err, "invalid URL %q", c.URL)
	}

	return nil
}

// Configs is the configuration for all [[alertpost]] sections of the kapacitor
// configuration file.
type Configs []Config

// Validate calls config.Validate for each element in Configs
func (cs Configs) Validate() error {
	for _, c := range cs {
		err := c.Validate()
		if err != nil {
			return err
		}
	}
	return nil
}

// index generates a map from config.Endpoint to config
func (cs Configs) index() map[string]*Endpoint {
	m := map[string]*Endpoint{}

	for _, c := range cs {
		m[c.Endpoint] = NewEndpoint(c.URL, c.Headers, c.BasicAuth)
	}

	return m
}
