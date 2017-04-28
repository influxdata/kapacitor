package dns

import (
	"fmt"
	"strings"
	"time"

	"github.com/influxdata/influxdb/toml"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
)

// Config is a DNS service discovery configuration
type Config struct {
	Enabled         bool          `toml:"enabled" override:"enabled"`
	ID              string        `toml:"id" override:"id"`
	RecordNames     []string      `toml:"record-names" override:"record-names"`
	RefreshInterval toml.Duration `toml:"refresh-interval" override:"refresh-interval"`
	Type            string        `toml:"type" override:"type"`
	Port            int           `toml:"port" override:"port"` // Ignored for SRV records
}

// Init adds default values to DNS configuration
func (d *Config) Init() {
	d.Type = "SRV"
	d.RefreshInterval = toml.Duration(30 * time.Second)
}

// Validate validates DNS configuration's values
func (d Config) Validate() error {
	if d.ID == "" {
		return fmt.Errorf("dns discovery must be given a ID")
	}
	switch strings.ToUpper(d.Type) {
	case "SRV":
	case "A", "AAAA":
		if d.Port == 0 {
			return fmt.Errorf("Port required for dns discovery type %s", d.Type)
		}
	default:
		return fmt.Errorf("invalid dns discovery records type %s", d.Type)
	}
	return nil
}

// Prom writes the prometheus configuration for discoverer into ScrapeConfig
func (d Config) Prom(c *config.ScrapeConfig) {
	c.ServiceDiscoveryConfig.DNSSDConfigs = []*config.DNSSDConfig{
		d.PromConfig(),
	}
}

// PromConfig returns the prometheus configuration for this discoverer
func (d Config) PromConfig() *config.DNSSDConfig {
	return &config.DNSSDConfig{
		Names:           d.RecordNames,
		RefreshInterval: model.Duration(d.RefreshInterval),
		Type:            d.Type,
		Port:            d.Port,
	}
}

// Service return discoverer type
func (d Config) Service() string {
	return "dns"
}

// ServiceID returns the discoverers name
func (d Config) ServiceID() string {
	return d.ID
}
