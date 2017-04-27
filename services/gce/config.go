package gce

import (
	"fmt"
	"time"

	"github.com/influxdata/influxdb/toml"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
)

// Config is GCE service discovery configuration
type Config struct {
	Enabled         bool          `toml:"enabled" override:"enabled"`
	Name            string        `toml:"name" override:"name"`
	Project         string        `toml:"project" override:"project"`
	Zone            string        `toml:"zone" override:"zone"`
	Filter          string        `toml:"filter" override:"filter"`
	RefreshInterval toml.Duration `toml:"refresh-interval" override:"refresh-interval"`
	Port            int           `toml:"port" override:"port"`
	TagSeparator    string        `toml:"tag-separator" override:"tag-separator"`
}

// NewConfig creates a google compute engine discovery configuration with default values
func NewConfig() Config {
	return Config{
		Enabled:         false,
		Name:            "gce",
		Port:            80,
		TagSeparator:    ",",
		RefreshInterval: toml.Duration(60 * time.Second),
	}
}

// ApplyConditionalDefaults adds defaults to existing GCE configuration
func (g *Config) ApplyConditionalDefaults() {
	if g.Port == 0 {
		g.Port = 80
	}
	if g.TagSeparator == "" {
		g.TagSeparator = ","
	}
	if g.RefreshInterval == 0 {
		g.RefreshInterval = toml.Duration(60 * time.Second)
	}
}

// Validate validates the GCE configuration values
func (g Config) Validate() error {
	if g.Name == "" {
		return fmt.Errorf("gce discovery must be given a name")
	}
	return nil
}

// Prom creates a prometheus configuration for GCE
func (g Config) Prom(c *config.ScrapeConfig) {
	c.ServiceDiscoveryConfig.GCESDConfigs = []*config.GCESDConfig{
		&config.GCESDConfig{
			Project:         g.Project,
			Zone:            g.Zone,
			Filter:          g.Filter,
			RefreshInterval: model.Duration(g.RefreshInterval),
			Port:            g.Port,
			TagSeparator:    g.TagSeparator,
		},
	}
}

// Service return discoverer type
func (g Config) Service() string {
	return "gce"
}

// ID returns the discoverers name
func (g Config) ID() string {
	return g.Name
}
