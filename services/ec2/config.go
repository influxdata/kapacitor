package ec2

import (
	"fmt"
	"time"

	"github.com/influxdata/influxdb/toml"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
)

// Config is EC2 service discovery configuration
type Config struct {
	Enabled         bool          `toml:"enabled" override:"enabled"`
	Name            string        `toml:"name" override:"name"`
	Region          string        `toml:"region" override:"region"`
	AccessKey       string        `toml:"access-key" override:"access-key"`
	SecretKey       string        `toml:"secret-key" override:"secret-key,redact"`
	Profile         string        `toml:"profile" override:"profile"`
	RefreshInterval toml.Duration `toml:"refresh-interval" override:"refresh-interval"`
	Port            int           `toml:"port" override:"port"`
}

// NewConfig creates a new EC2 discovery configuration with default values
func NewConfig() Config {
	return Config{
		Enabled:         false,
		Region:          "us-east-1",
		Name:            "ec2",
		Port:            80,
		RefreshInterval: toml.Duration(60 * time.Second),
	}
}

// ApplyConditionalDefaults adds default values to EC2 configuration
func (e *Config) ApplyConditionalDefaults() {
	if e.Port == 0 {
		e.Port = 80
	}
	if e.RefreshInterval == 0 {
		e.RefreshInterval = toml.Duration(60 * time.Second)
	}
}

// Validate validates the EC2 configuration values
func (e Config) Validate() error {
	if e.Name == "" {
		return fmt.Errorf("ec2 discovery must be given a name")
	}
	if e.Region == "" {
		return fmt.Errorf("ec2 discovery, %s, requires a region", e.Name)
	}
	return nil
}

// Prom creates a prometheus configuration for EC2
func (e Config) Prom(c *config.ScrapeConfig) {
	c.ServiceDiscoveryConfig.EC2SDConfigs = []*config.EC2SDConfig{
		&config.EC2SDConfig{
			Region:          e.Region,
			AccessKey:       e.AccessKey,
			SecretKey:       e.SecretKey,
			Profile:         e.Profile,
			RefreshInterval: model.Duration(e.RefreshInterval),
			Port:            e.Port,
		},
	}
}

// Service return discoverer type
func (e Config) Service() string {
	return "ec2"
}

// ID returns the discoverers name
func (e Config) ID() string {
	return e.Name
}
