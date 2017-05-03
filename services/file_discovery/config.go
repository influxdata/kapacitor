package file_discovery

import (
	"fmt"
	"regexp"
	"time"

	"github.com/influxdata/influxdb/toml"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
)

// Config is a file service discovery configuration
type Config struct {
	Enabled         bool          `toml:"enabled" override:"enabled"`
	ID              string        `toml:"id" override:"id"`
	Files           []string      `toml:"files" override:"files"`
	RefreshInterval toml.Duration `toml:"refresh-interval" override:"refresh-interval"`
}

// Init adds defaults to an existing File configuration
func (f *Config) Init() {
	f.RefreshInterval = toml.Duration(5 * time.Minute)

}

var fileRegex = regexp.MustCompile(`^[^*]*(\*[^/]*)?\.(json|yml|yaml|JSON|YML|YAML)$`)

// Validate validates the File configuration
func (f Config) Validate() error {
	if f.ID == "" {
		return fmt.Errorf("file discovery must be given a ID")
	}
	for _, name := range f.Files {
		if !fileRegex.MatchString(name) {
			return fmt.Errorf("path name %q is not valid for file discovery", name)
		}
	}
	return nil
}

// Prom writes the prometheus configuration for discoverer into ScrapeConfig
func (f Config) Prom(c *config.ScrapeConfig) {
	c.ServiceDiscoveryConfig.FileSDConfigs = []*config.FileSDConfig{
		f.PromConfig(),
	}
}

// PromConfig returns the prometheus configuration for this discoverer
func (f Config) PromConfig() *config.FileSDConfig {
	return &config.FileSDConfig{
		Files:           f.Files,
		RefreshInterval: model.Duration(f.RefreshInterval),
	}
}

// Service return discoverer type
func (f Config) Service() string {
	return "file-discovery"
}

// ServiceID returns the discoverers name
func (f Config) ServiceID() string {
	return f.ID
}
