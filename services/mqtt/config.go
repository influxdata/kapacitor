package mqtt

import (
	"errors"
)

type Config struct {
	// Enabled indicates whether the service should be enabled
	Enabled bool   `toml:"enabled" override:"enabled"`
	Name    string `toml:"name" override:"name"`
	Default bool   `toml:"default" override:"default"`
	// URL of the MQTT Broker.
	// Valid URLs include tcp://host:port, ws://host:port or ssl://host:port.
	// If using ssl://host:port, one must also specify the SSL configuration options.
	URL string `toml:"url" override:"url"`

	// Path to CA file
	SSLCA string `toml:"ssl-ca" override:"ssl-ca"`
	// Path to host cert file
	SSLCert string `toml:"ssl-cert" override:"ssl-cert"`
	// Path to cert key file
	SSLKey string `toml:"ssl-key" override:"ssl-key"`
	// Use SSL but skip chain & host verification
	InsecureSkipVerify bool `toml:"insecure-skip-verify" override:"insecure-skip-verify"`

	// ClientID is the unique ID advertised to the MQTT broker from this client.
	// Defaults to Name if empty.
	ClientID string `toml:"client-id" override:"client-id"`
	Username string `toml:"username" override:"username"`
	Password string `toml:"password" override:"password,redact"`

	// newClientF is a function that returns a client for a given config.
	// It is used exclusively for testing.
	newClientF func(c Config) (Client, error) `override:"-"`
}

// SetNewClientF sets the newClientF on a Config.
// It is used exclusively for testing.
func (c *Config) SetNewClientF(fn func(c Config) (Client, error)) {
	c.newClientF = fn
}

func NewConfig() Config {
	return Config{
		Enabled: false,
		Name:    "default",
	}
}

func (c Config) Validate() error {
	if c.Name == "" {
		return errors.New("must specify a name for the mqtt broker")
	}
	if c.Enabled {
		if c.URL == "" {
			return errors.New("must specify a url for mqtt service")
		}
	}
	return nil
}

// NewClient creates a new client based off this configuration.
func (c Config) NewClient() (Client, error) {
	newC := newClient
	if c.newClientF != nil {
		newC = c.newClientF
	}
	return newC(c)
}

func (c Config) Equal(o Config) bool {
	if c.Enabled != o.Enabled {
		return false
	}
	if c.Name != o.Name {
		return false
	}
	if c.Default != o.Default {
		return false
	}
	if c.URL != o.URL {
		return false
	}

	if c.SSLCA != o.SSLCA {
		return false
	}
	if c.SSLCert != o.SSLCert {
		return false
	}
	if c.SSLKey != o.SSLKey {
		return false
	}
	if c.InsecureSkipVerify != o.InsecureSkipVerify {
		return false
	}

	if c.ClientID != o.ClientID {
		return false
	}
	if c.Username != o.Username {
		return false
	}
	if c.Password != o.Password {
		return false
	}
	return true
}

type Configs []Config

// Validate calls config.Validate for each element in Configs
func (cs Configs) Validate() error {
	defaultCount := 0
	for _, c := range cs {
		if c.Default {
			defaultCount++
		}
		err := c.Validate()
		if err != nil {
			return err
		}
	}
	if defaultCount > 1 {
		return errors.New("more than one configuration is marked as the default")
	}
	if defaultCount == 0 && len(cs) > 1 {
		return errors.New("no configuration is marked as the default")
	}
	return nil
}

// index generates a map of configs by name
func (cs Configs) index() map[string]Config {
	m := make(map[string]Config, len(cs))

	for _, c := range cs {
		m[c.Name] = c
	}

	return m
}
