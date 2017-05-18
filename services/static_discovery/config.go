package static_discovery

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
	Targets []string `toml:"targets" override:"targets"`
	// Labels is a set of labels that is common across all targets in the group.
	Labels map[string]string `toml:"labels" override:"labels"`
}

// Init the static configuration to an empty set of structures
func (s *Config) Init() {
	s.Targets = []string{}
	s.Labels = map[string]string{}
}

// Validate validates Static configuration values
func (s Config) Validate() error {
	if s.ID == "" {
		return fmt.Errorf("static discovery must be given a ID")
	}
	return nil
}

// Prom writes the prometheus configuration for discoverer into ScrapeConfig
func (s Config) Prom(c *config.ScrapeConfig) {
	c.ServiceDiscoveryConfig.StaticConfigs = s.PromConfig()
}

// PromConfig returns the prometheus configuration for this discoverer
func (s Config) PromConfig() []*config.TargetGroup {
	set := func(l map[string]string) model.LabelSet {
		res := make(model.LabelSet)
		for k, v := range l {
			res[model.LabelName(k)] = model.LabelValue(v)
		}
		return res
	}
	target := func(t []string) []model.LabelSet {
		res := make([]model.LabelSet, len(t))
		for i, l := range t {
			res[i] = model.LabelSet{
				model.LabelName(model.AddressLabel): model.LabelValue(l),
			}
		}
		return res
	}
	return []*config.TargetGroup{
		&config.TargetGroup{
			Targets: target(s.Targets),
			Labels:  set(s.Labels),
			Source:  s.ID,
		},
	}
}

// Service return discoverer type
func (s Config) Service() string {
	return "static-discovery"
}

// ServiceID returns the discoverers name
func (s Config) ServiceID() string {
	return s.ID
}
