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
	ID              string        `toml:"id" override:"id"`
	Servers         []string      `toml:"servers" override:"servers"`
	Timeout         toml.Duration `toml:"timeout" override:"timeout"`
	RefreshInterval toml.Duration `toml:"refresh-interval" override:"refresh-interval"`
	BearerToken     string        `toml:"bearer-token" override:"bearer-token,redact"`
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

// Init adds default values to existing Marathon configuration
func (m *Config) Init() {
	m.Timeout = toml.Duration(30 * time.Second)
	m.RefreshInterval = toml.Duration(30 * time.Second)
}

// Validate validates Marathon configuration values
func (m Config) Validate() error {
	if m.ID == "" {
		return fmt.Errorf("marathon discovery must be given an ID")
	}
	return nil
}

// Prom writes the prometheus configuration for discoverer into ScrapeConfig
func (m Config) Prom(c *config.ScrapeConfig) {
	c.ServiceDiscoveryConfig.MarathonSDConfigs = []*config.MarathonSDConfig{
		m.PromConfig(),
	}
}

// PromConfig returns the prometheus configuration for this discoverer
func (m Config) PromConfig() *config.MarathonSDConfig {
	return &config.MarathonSDConfig{
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
	}
}

// Service return discoverer type
func (m Config) Service() string {
	return "marathon"
}

// ServiceID returns the discoverers name
func (m Config) ServiceID() string {
	return m.ID
}
