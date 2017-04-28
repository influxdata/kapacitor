package nerve

import (
	"fmt"
	"strings"
	"time"

	"github.com/influxdata/influxdb/toml"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
)

// Config is a Nerve service discovery configuration
type Config struct {
	Enabled bool          `toml:"enabled" override:"enabled"`
	ID      string        `toml:"id" override:"id"`
	Servers []string      `toml:"servers" override:"servers"`
	Paths   []string      `toml:"paths" override:"paths"`
	Timeout toml.Duration `toml:"timeout" override:"timeout"`
}

// Init adds default values to Nerve configuration
func (n *Config) Init() {
	n.Timeout = toml.Duration(10 * time.Second)
}

// Validate validates the Nerve configuration values
func (n Config) Validate() error {
	if n.ID == "" {
		return fmt.Errorf("nerve discovery must be given a ID")
	}
	for _, path := range n.Paths {
		if !strings.HasPrefix(path, "/") {
			return fmt.Errorf("nerve discovery paths must begin with '/': %s", path)
		}
	}
	return nil
}

// Prom writes the prometheus configuration for discoverer into ScrapeConfig
func (n Config) Prom(c *config.ScrapeConfig) {
	c.ServiceDiscoveryConfig.NerveSDConfigs = []*config.NerveSDConfig{
		n.PromConfig(),
	}
}

// PromConfig returns the prometheus configuration for this discoverer
func (n Config) PromConfig() *config.NerveSDConfig {
	return &config.NerveSDConfig{
		Servers: n.Servers,
		Paths:   n.Paths,
		Timeout: model.Duration(n.Timeout),
	}
}

// Service return discoverer type
func (n Config) Service() string {
	return "nerve"
}

// ServiceID returns the discoverers name
func (n Config) ServiceID() string {
	return n.ID
}
