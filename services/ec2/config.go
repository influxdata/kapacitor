package ec2

import (
	"fmt"
	"time"

	"github.com/influxdata/influxdb/toml"
	"github.com/influxdata/kapacitor/services/ec2/client"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
)

// Config is EC2 service discovery configuration
type Config struct {
	Enabled         bool          `toml:"enabled" override:"enabled"`
	ID              string        `toml:"id" override:"id"`
	Region          string        `toml:"region" override:"region"`
	AccessKey       string        `toml:"access-key" override:"access-key"`
	SecretKey       string        `toml:"secret-key" override:"secret-key,redact"`
	Profile         string        `toml:"profile" override:"profile"`
	RefreshInterval toml.Duration `toml:"refresh-interval" override:"refresh-interval"`
	Port            int           `toml:"port" override:"port"`
}

// Init adds default values to EC2 configuration
func (e *Config) Init() {
	e.Port = 80
	e.RefreshInterval = toml.Duration(60 * time.Second)
}

// Validate validates the EC2 configuration values
func (e Config) Validate() error {
	if e.ID == "" {
		return fmt.Errorf("ec2 discovery must be given a ID")
	}
	if e.Region == "" {
		return fmt.Errorf("ec2 discovery, %s, requires a region", e.ID)
	}
	return nil
}

// Prom writes the prometheus configuration for discoverer into ScrapeConfig
func (e Config) Prom(c *config.ScrapeConfig) {
	c.ServiceDiscoveryConfig.EC2SDConfigs = []*config.EC2SDConfig{
		e.PromConfig(),
	}
}

func (c Config) ClientConfig() (client.Config, error) {
	return client.Config{
		AccessKey: c.AccessKey,
		SecretKey: c.SecretKey,
		Region:    c.Region,
	}, nil
}

// PromConfig returns the prometheus configuration for this discoverer
func (e Config) PromConfig() *config.EC2SDConfig {
	return &config.EC2SDConfig{
		Region:          e.Region,
		AccessKey:       e.AccessKey,
		SecretKey:       e.SecretKey,
		Profile:         e.Profile,
		RefreshInterval: model.Duration(e.RefreshInterval),
		Port:            e.Port,
	}
}

// Service return discoverer type
func (e Config) Service() string {
	return "ec2"
}

// ServiceID returns the discoverers name
func (e Config) ServiceID() string {
	return e.ID
}
