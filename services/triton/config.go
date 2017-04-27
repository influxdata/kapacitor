package triton

import (
	"fmt"
	"time"

	"github.com/influxdata/influxdb/toml"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
)

// Config is a Triton service discovery configuration
type Config struct {
	Enabled         bool          `toml:"enabled" override:"enabled"`
	Name            string        `toml:"name" override:"name"`
	Account         string        `toml:"account" override:"account"`
	DNSSuffix       string        `toml:"dns-suffix" override:"dns-suffix"`
	Endpoint        string        `toml:"endpoint" override:"endpoint"`
	Port            int           `toml:"port" override:"port"`
	RefreshInterval toml.Duration `toml:"refresh-interval" override:"refresh-interval"`
	Version         int           `toml:"version" override:"version"`
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

// NewConfig creates Triton discovery configuration with default values
func NewConfig() Config {
	return Config{
		Name:            "triton",
		Enabled:         false,
		Port:            9163,
		RefreshInterval: toml.Duration(60 * time.Second),
		Version:         1,
	}
}

// ApplyConditionalDefaults adds default values to Triton configuration
func (t *Config) ApplyConditionalDefaults() {
	if t.Port == 0 {
		t.Port = 9163
	}
	if t.RefreshInterval == 0 {
		t.RefreshInterval = toml.Duration(60 * time.Second)
	}
	if t.Version == 0 {
		t.Version = 1
	}
}

// Validate validates Triton configuration values
func (t Config) Validate() error {
	if t.Name == "" {
		return fmt.Errorf("triton discovery must be given a name")
	}
	return nil
}

// Prom creates a prometheus configuration for Triton
func (t Config) Prom(c *config.ScrapeConfig) {
	c.ServiceDiscoveryConfig.TritonSDConfigs = []*config.TritonSDConfig{
		&config.TritonSDConfig{
			Account:         t.Account,
			DNSSuffix:       t.DNSSuffix,
			Endpoint:        t.Endpoint,
			Port:            t.Port,
			RefreshInterval: model.Duration(t.RefreshInterval),
			Version:         t.Version,
			TLSConfig: config.TLSConfig{
				CAFile:             t.SSLCA,
				CertFile:           t.SSLCert,
				KeyFile:            t.SSLKey,
				ServerName:         t.SSLServerName,
				InsecureSkipVerify: t.InsecureSkipVerify,
			},
		},
	}
}

// Service return discoverer type
func (t Config) Service() string {
	return "triton"
}

// ID returns the discoverers name
func (t Config) ID() string {
	return t.Name
}
