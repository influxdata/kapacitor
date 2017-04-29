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
	ID      string        `toml:"id" override:"id"`
	Servers []string      `toml:"servers" override:"servers"`
	Paths   []string      `toml:"paths" override:"paths"`
	Timeout toml.Duration `toml:"timeout" override:"timeout"`
}

// Init adds default values to Serverset configuration
func (s *Config) Init() {
	s.Timeout = toml.Duration(10 * time.Second)
}

// Validate validates Serverset configuration
func (s Config) Validate() error {
	if s.ID == "" {
		return fmt.Errorf("server set discovery must be given a ID")
	}
	for _, path := range s.Paths {
		if !strings.HasPrefix(path, "/") {
			return fmt.Errorf("serverset discovery config paths must begin with '/': %s", path)
		}
	}
	return nil
}

// Prom writes the prometheus configuration for discoverer into ScrapeConfig
func (s Config) Prom(c *config.ScrapeConfig) {
	c.ServiceDiscoveryConfig.ServersetSDConfigs = []*config.ServersetSDConfig{
		s.PromConfig(),
	}
}

// PromConfig returns the prometheus configuration for this discoverer
func (s Config) PromConfig() *config.ServersetSDConfig {
	return &config.ServersetSDConfig{
		Servers: s.Servers,
		Paths:   s.Paths,
		Timeout: model.Duration(s.Timeout),
	}
}

// Service return discoverer type
func (s Config) Service() string {
	return "serverset"
}

// ServiceID returns the discoverers name
func (s Config) ServiceID() string {
	return s.ID
}
