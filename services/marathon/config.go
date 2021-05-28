package marathon

import (
	"fmt"
	"time"

	"github.com/influxdata/influxdb/toml"
	config2 "github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/discovery/marathon"
)

// Config is Marathon service discovery configuration
type Config struct {
	Enabled         bool          `toml:"enabled" override:"enabled"`
	ID              string        `toml:"id" override:"id"`
	Servers         []string      `toml:"servers" override:"servers"`
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
	// This previously supported Timeout but it was removed, see https://github.com/prometheus/common/pull/123
}

// Init adds default values to existing Marathon configuration
func (m *Config) Init() {
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
	c.ServiceDiscoveryConfigs = append(c.ServiceDiscoveryConfigs, m.PromConfig())
}

// PromConfig returns the prometheus configuration for this discoverer
func (m Config) PromConfig() *marathon.SDConfig {
	return &marathon.SDConfig{
		Servers:         m.Servers,
		RefreshInterval: model.Duration(m.RefreshInterval),
		HTTPClientConfig: config2.HTTPClientConfig{
			TLSConfig: config2.TLSConfig{
				CAFile:             m.SSLCA,
				CertFile:           m.SSLCert,
				KeyFile:            m.SSLKey,
				ServerName:         m.SSLServerName,
				InsecureSkipVerify: m.InsecureSkipVerify,
			},
			BearerToken: config2.Secret(m.BearerToken),
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
