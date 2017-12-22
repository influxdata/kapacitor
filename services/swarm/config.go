package swarm

import (
	"github.com/influxdata/kapacitor/services/swarm/client"
	"github.com/influxdata/kapacitor/tlsconfig"
	"github.com/pkg/errors"
)

type Config struct {
	Enabled bool     `toml:"enabled" override:"enabled"`
	ID      string   `toml:"id" override:"id"`
	Servers []string `toml:"servers" override:"servers"`
	// Path to CA file
	SSLCA string `toml:"ssl-ca" override:"ssl-ca"`
	// Path to host cert file
	SSLCert string `toml:"ssl-cert" override:"ssl-cert"`
	// Path to cert key file
	SSLKey string `toml:"ssl-key" override:"ssl-key"`
	// Use SSL but skip chain & host verification
	InsecureSkipVerify bool `toml:"insecure-skip-verify" override:"insecure-skip-verify"`
}

func NewConfig() Config {
	return Config{}
}

func (c Config) Validate() error {
	if !c.Enabled {
		return nil
	}

	if len(c.Servers) == 0 {
		return errors.New("no api-servers specified, must provide at least one server URL")
	}

	return nil
}

func (c Config) ClientConfig() (client.Config, error) {
	t, err := tlsconfig.Create(c.SSLCA, c.SSLCert, c.SSLKey, c.InsecureSkipVerify)
	if err != nil {
		return client.Config{}, err
	}
	return client.Config{
		URLs:      c.Servers,
		TLSConfig: t,
	}, nil
}

type Configs []Config

func (cs Configs) Validate() error {
	for _, c := range cs {
		if err := c.Validate(); err != nil {
			return err
		}
	}
	return nil
}
