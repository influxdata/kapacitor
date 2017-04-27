package static

import (
	"fmt"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
)

// Config is a static list of  of labeled target groups
type Config struct {
	Enabled bool   `toml:"enabled" override:"enabled"`
	ID      string `toml:"id" override:"id"`
	// Targets is a list of targets identified by a label set. Each target is
	// uniquely identifiable in the group by its address label.
	Targets []map[string]string `toml:"targets" override:"targets"`
	// Labels is a set of labels that is common across all targets in the group.
	Labels map[string]string `toml:"labels" override:"labels"`
}

// Init the static configuration to an empty set of structures
func (s *Config) Init() {
	s.Targets = []map[string]string{}
	s.Labels = map[string]string{}
}

// Validate validates Static configuration values
func (s Config) Validate() error {
	if s.ID == "" {
		return fmt.Errorf("azure discovery must be given a ID")
	}
	return nil
}

// Prom creates a prometheus configuration from Static
func (s Config) Prom(c *config.ScrapeConfig) {
	set := func(l map[string]string) model.LabelSet {
		res := make(model.LabelSet)
		for k, v := range l {
			res[model.LabelName(k)] = model.LabelValue(v)
		}
		return res
	}
	target := func(t []map[string]string) []model.LabelSet {
		res := make([]model.LabelSet, len(t))
		for i, l := range t {
			res[i] = set(l)
		}
		return res
	}
	c.ServiceDiscoveryConfig.StaticConfigs = []*config.TargetGroup{
		&config.TargetGroup{
			Targets: target(s.Targets),
			Labels:  set(s.Labels),
			Source:  s.ID,
		},
	}
}

// Service return discoverer type
func (s Config) Service() string {
	return "static"
}

// ServiceID returns the discoverers name
func (s Config) ServiceID() string {
	return s.ID
}
