package azure

import (
	"fmt"
	"time"

	"github.com/influxdata/influxdb/toml"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
)

// Config is a Azure service discovery configuration
type Config struct {
	Enabled         bool          `toml:"enabled" override:"enabled"`
	Name            string        `toml:"name" override:"name"`
	Port            int           `toml:"port" override:"port"`
	SubscriptionID  string        `toml:"subscription-id" override:"subscription-id"`
	TenantID        string        `toml:"tenant-id" override:"tenant-id"`
	ClientID        string        `toml:"client-id" override:"client-id"`
	ClientSecret    string        `toml:"client-secret" override:"client-secret,redact"`
	RefreshInterval toml.Duration `toml:"refresh-interval" override:"refresh-interval"`
}

// NewConfig creates a new Azure configuration with default values.
func NewConfig() Config {
	return Config{
		Name:            "azure",
		Enabled:         false,
		Port:            80,
		RefreshInterval: toml.Duration(5 * time.Minute),
	}
}

// ApplyConditionalDefaults adds the default values for uninitialized configurations
func (a *Config) ApplyConditionalDefaults() {
	if a.Port == 0 {
		a.Port = 80
	}
	if a.RefreshInterval == 0 {
		a.RefreshInterval = toml.Duration(5 * time.Minute)
	}
}

// Validate checks the azure configuration
func (a Config) Validate() error {
	if a.Name == "" {
		return fmt.Errorf("azure discovery must be given a name")
	}
	return nil
}

// Prom returns the prometheus configuration for this discoverer
func (a Config) Prom(c *config.ScrapeConfig) {
	c.ServiceDiscoveryConfig.AzureSDConfigs = []*config.AzureSDConfig{
		&config.AzureSDConfig{
			Port:            a.Port,
			SubscriptionID:  a.SubscriptionID,
			TenantID:        a.TenantID,
			ClientID:        a.ClientID,
			ClientSecret:    a.ClientSecret,
			RefreshInterval: model.Duration(a.RefreshInterval),
		},
	}
}

// Service return discoverer type
func (a Config) Service() string {
	return "azure"
}

// ID returns the discoverers name
func (a Config) ID() string {
	return a.Name
}
