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
	Name    string        `toml:"name" override:"name"`
	Servers []string      `toml:"servers" override:"servers"`
	Paths   []string      `toml:"paths" override:"paths"`
	Timeout toml.Duration `toml:"timeout" override:"timeout"`
}

// NewConfig creates Nerve discovery configuration with default values
func NewConfig() Config {
	return Config{
		Name:    "nerve",
		Enabled: false,
		Servers: []string{},
		Paths:   []string{},
		Timeout: toml.Duration(10 * time.Second),
	}
}

// ApplyConditionalDefaults adds default values to Nerve configuration
func (n *Config) ApplyConditionalDefaults() {
	if n.Timeout == 0 {
		n.Timeout = toml.Duration(10 * time.Second)
	}
}

// Validate validates the Nerve configuration values
func (n Config) Validate() error {
	if n.Name == "" {
		return fmt.Errorf("nerve discovery must be given a name")
	}
	for _, path := range n.Paths {
		if !strings.HasPrefix(path, "/") {
			return fmt.Errorf("nerve discovery paths must begin with '/': %s", path)
		}
	}
	return nil
}

// Prom creates a prometheus configuration for Nerve
func (n Config) Prom(c *config.ScrapeConfig) {
	c.ServiceDiscoveryConfig.NerveSDConfigs = []*config.NerveSDConfig{
		&config.NerveSDConfig{
			Servers: n.Servers,
			Paths:   n.Paths,
			Timeout: model.Duration(n.Timeout),
		},
	}
}

// Service return discoverer type
func (n Config) Service() string {
	return "nerve"
}

// ID returns the discoverers name
func (n Config) ID() string {
	return n.Name
}
