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
	Name            string        `toml:"name" override:"name"`
	RecordNames     []string      `toml:"record-names" override:"record-names"`
	RefreshInterval toml.Duration `toml:"refresh-interval" override:"refresh-interval"`
	Type            string        `toml:"type" override:"type"`
	Port            int           `toml:"port" override:"port"` // Ignored for SRV records
}

// NewDNS creates a new DNS configuration with default values
func NewConfig() Config {
	return Config{
		Name:            "dns",
		Enabled:         false,
		RefreshInterval: toml.Duration(30 * time.Second),
		RecordNames:     []string{},
		Type:            "SRV",
	}
}

// ApplyConditionalDefaults adds default values to DNS configuration
func (d *Config) ApplyConditionalDefaults() {
	if d.Type == "" {
		d.Type = "SRV"
	}
	if d.RefreshInterval == 0 {
		d.RefreshInterval = toml.Duration(30 * time.Second)
	}
}

// Validate validates DNS configuration's values
func (d Config) Validate() error {
	if d.Name == "" {
		return fmt.Errorf("dns discovery must be given a name")
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

// Prom creates the prometheus configuration using the DNS configuration
func (d Config) Prom(c *config.ScrapeConfig) {
	c.ServiceDiscoveryConfig.DNSSDConfigs = []*config.DNSSDConfig{
		&config.DNSSDConfig{
			Names:           d.RecordNames,
			RefreshInterval: model.Duration(d.RefreshInterval),
			Type:            d.Type,
			Port:            d.Port,
		},
	}
}

// Service return discoverer type
func (d Config) Service() string {
	return "dns"
}

// ID returns the discoverers name
func (d Config) ID() string {
	return d.Name
}
