package file

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
	Name            string        `toml:"name" override:"name"`
	Files           []string      `toml:"files" override:"files"`
	RefreshInterval toml.Duration `toml:"refresh-interval" override:"refresh-interval"`
}

// NewConfig creates a File discovery configuration with default values
func NewConfig() Config {
	return Config{
		Enabled:         false,
		Name:            "file",
		Files:           []string{},
		RefreshInterval: toml.Duration(5 * time.Minute),
	}
}

// ApplyConditionalDefaults adds defaults to an existing File configuration
func (f *Config) ApplyConditionalDefaults() {
	if f.RefreshInterval == 0 {
		f.RefreshInterval = toml.Duration(5 * time.Minute)
	}
}

var fileRegex = regexp.MustCompile(`^[^*]*(\*[^/]*)?\.(json|yml|yaml|JSON|YML|YAML)$`)

// Validate validates the File configuration
func (f Config) Validate() error {
	if f.Name == "" {
		return fmt.Errorf("file discovery must be given a name")
	}
	for _, name := range f.Files {
		if !fileRegex.MatchString(name) {
			return fmt.Errorf("path name %q is not valid for file discovery", name)
		}
	}
	return nil
}

// Prom creates a prometheus configuration for the File discovery
func (f Config) Prom(c *config.ScrapeConfig) {
	c.ServiceDiscoveryConfig.FileSDConfigs = []*config.FileSDConfig{
		&config.FileSDConfig{
			Files:           f.Files,
			RefreshInterval: model.Duration(f.RefreshInterval),
		},
	}
}

// Service return discoverer type
func (f Config) Service() string {
	return "file"
}

// ID returns the discoverers name
func (f Config) ID() string {
	return f.Name
}
