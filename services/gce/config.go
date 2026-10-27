package gce

import (
	"fmt"
	"time"

	"github.com/influxdata/influxdb/toml"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/discovery/gce"
)

// Config is GCE service discovery configuration
type Config struct {
	Enabled         bool          `toml:"enabled" override:"enabled"`
	ID              string        `toml:"id" override:"id"`
	Project         string        `toml:"project" override:"project"`
	Zone            string        `toml:"zone" override:"zone"`
	Filter          string        `toml:"filter" override:"filter"`
	RefreshInterval toml.Duration `toml:"refresh-interval" override:"refresh-interval"`
	Port            int           `toml:"port" override:"port"`
	TagSeparator    string        `toml:"tag-separator" override:"tag-separator"`
}

// Init adds defaults to existing GCE configuration
func (g *Config) Init() {
	g.Port = 80
	g.TagSeparator = ","
	g.RefreshInterval = toml.Duration(60 * time.Second)
}

// Validate validates the GCE configuration values
func (g Config) Validate() error {
	if g.ID == "" {
		return fmt.Errorf("gce discovery must be given a ID")
	}
	return nil
}

// Prom writes the prometheus configuration for discoverer into ScrapeConfig
func (g Config) Prom(c *config.ScrapeConfig) {
	c.ServiceDiscoveryConfigs = append(c.ServiceDiscoveryConfigs, g.PromConfig())
}

// PromConfig returns the prometheus configuration for this discoverer
func (g Config) PromConfig() *gce.SDConfig {
	sd := gce.DefaultSDConfig
	sd.Project = g.Project
	sd.Zone = g.Zone
	sd.Filter = g.Filter
	sd.RefreshInterval = model.Duration(g.RefreshInterval)
	sd.Port = g.Port
	sd.TagSeparator = g.TagSeparator
	return &sd
}

// Service return discoverer type
func (g Config) Service() string {
	return "gce"
}

// ServiceID returns the discoverers name
func (g Config) ServiceID() string {
	return g.ID
}
