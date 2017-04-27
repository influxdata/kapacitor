package serverset

import (
	"fmt"
	"strings"
	"time"

	"github.com/influxdata/influxdb/toml"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
)

// Config is a Serverset service discovery configuration
type Config struct {
	Enabled bool          `toml:"enabled" override:"enabled"`
	Name    string        `toml:"name" override:"name"`
	Servers []string      `toml:"servers" override:"servers"`
	Paths   []string      `toml:"paths" override:"paths"`
	Timeout toml.Duration `toml:"timeout" override:"timeout"`
}

// NewConfig creates Serverset with default values
func NewConfig() Config {
	return Config{
		Name:    "serverset",
		Enabled: false,
		Servers: []string{},
		Paths:   []string{},
		Timeout: toml.Duration(10 * time.Second),
	}
}

// ApplyConditionalDefaults adds default values to Serverset configuration
func (s *Config) ApplyConditionalDefaults() {
	if s.Timeout == 0 {
		s.Timeout = toml.Duration(10 * time.Second)
	}
}

// Validate validates Serverset configuration
func (s Config) Validate() error {
	if s.Name == "" {
		return fmt.Errorf("server set discovery must be given a name")
	}
	for _, path := range s.Paths {
		if !strings.HasPrefix(path, "/") {
			return fmt.Errorf("serverset discovery config paths must begin with '/': %s", path)
		}
	}
	return nil
}

// Prom creates prometheus configuration from Serverset
func (s Config) Prom(c *config.ScrapeConfig) {
	c.ServiceDiscoveryConfig.ServersetSDConfigs = []*config.ServersetSDConfig{
		&config.ServersetSDConfig{
			Servers: s.Servers,
			Paths:   s.Paths,
			Timeout: model.Duration(s.Timeout),
		},
	}
}

// Service return discoverer type
func (s Config) Service() string {
	return "serverset"
}

// ID returns the discoverers name
func (s Config) ID() string {
	return s.Name
}
