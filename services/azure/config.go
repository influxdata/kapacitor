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
	ID              string        `toml:"id" override:"id"`
	Port            int           `toml:"port" override:"port"`
	SubscriptionID  string        `toml:"subscription-id" override:"subscription-id"`
	TenantID        string        `toml:"tenant-id" override:"tenant-id"`
	ClientID        string        `toml:"client-id" override:"client-id"`
	ClientSecret    string        `toml:"client-secret" override:"client-secret,redact"`
	RefreshInterval toml.Duration `toml:"refresh-interval" override:"refresh-interval"`
}

// Init adds the default values for uninitialized configurations
func (a *Config) Init() {
	a.Port = 80
	a.RefreshInterval = toml.Duration(5 * time.Minute)
}

// Validate checks the azure configuration
func (a Config) Validate() error {
	if a.ID == "" {
		return fmt.Errorf("azure discovery must be given a ID")
	}
	return nil
}

// Prom writes the prometheus configuration for discoverer into ScrapeConfig
func (a Config) Prom(c *config.ScrapeConfig) {
	c.ServiceDiscoveryConfig.AzureSDConfigs = []*config.AzureSDConfig{
		a.PromConfig(),
	}
}

// PromConfig returns the prometheus configuration for this discoverer
func (a Config) PromConfig() *config.AzureSDConfig {
	return &config.AzureSDConfig{
		Port:            a.Port,
		SubscriptionID:  a.SubscriptionID,
		TenantID:        a.TenantID,
		ClientID:        a.ClientID,
		ClientSecret:    a.ClientSecret,
		RefreshInterval: model.Duration(a.RefreshInterval),
	}
}

// Service return discoverer type
func (a Config) Service() string {
	return "azure"
}

// ServiceID returns the discoverers name
func (a Config) ServiceID() string {
	return a.ID
}
