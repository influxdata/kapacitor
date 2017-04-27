package marathon

import (
	"fmt"
	"time"

	"github.com/influxdata/influxdb/toml"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
)

// Config is Marathon service discovery configuration
type Config struct {
	Enabled         bool          `toml:"enabled" override:"enabled"`
	Name            string        `toml:"name" override:"name"`
	Servers         []string      `toml:"servers" override:"servers"`
	Timeout         toml.Duration `toml:"timeout" override:"timeout"`
	RefreshInterval toml.Duration `toml:"refresh-interval" override:"refresh-interval"`
	BearerToken     string        `toml:"bearer-token" override:"bearer-token"`
	// Path to CA file
	SSLCA string `toml:"ssl-ca" override:"ssl-ca"`
	// Path to host cert file
	SSLCert string `toml:"ssl-cert" override:"ssl-cert"`
	// Path to cert key file
	SSLKey string `toml:"ssl-key" override:"ssl-key"`
	// SSLServerName is used to verify the hostname for the targets.
	SSLServerName string `toml:"ssl-server-name" override:"ssl-server-name"`
	// Use SSL but skip chain & host verification
	InsecureSkipVerify bool `toml:"insecure-skip-verify" override:"insecure-skip-verify"`
}

// NewConfig creates a Marathon discovery configuration with default values
func NewConfig() Config {
	return Config{
		Enabled:         false,
		Name:            "marathon",
		Servers:         []string{},
		Timeout:         toml.Duration(30 * time.Second),
		RefreshInterval: toml.Duration(30 * time.Second),
	}
}

// ApplyConditionalDefaults adds default values to existing Marathon configuration
func (m *Config) ApplyConditionalDefaults() {
	if m.Timeout == 0 {
		m.Timeout = toml.Duration(30 * time.Second)
	}
	if m.RefreshInterval == 0 {
		m.RefreshInterval = toml.Duration(30 * time.Second)
	}
}

// Validate validates Marathon configuration values
func (m Config) Validate() error {
	if m.Name == "" {
		return fmt.Errorf("marathon discovery must be given a name")
	}
	return nil
}

// Prom creates prometheus configuration for Marathon
func (m Config) Prom(c *config.ScrapeConfig) {
	c.ServiceDiscoveryConfig.MarathonSDConfigs = []*config.MarathonSDConfig{
		&config.MarathonSDConfig{
			Servers:         m.Servers,
			Timeout:         model.Duration(m.Timeout),
			RefreshInterval: model.Duration(m.RefreshInterval),
			BearerToken:     m.BearerToken,
			TLSConfig: config.TLSConfig{
				CAFile:             m.SSLCA,
				CertFile:           m.SSLCert,
				KeyFile:            m.SSLKey,
				ServerName:         m.SSLServerName,
				InsecureSkipVerify: m.InsecureSkipVerify,
			},
		},
	}
}

// Service return discoverer type
func (m Config) Service() string {
	return "marathon"
}

// ID returns the discoverers name
func (m Config) ID() string {
	return m.Name
}
