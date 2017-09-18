package k8s

import (
	"fmt"
	"sync"

	"github.com/influxdata/kapacitor/services/k8s/client"
	"github.com/influxdata/kapacitor/services/scraper"
)

// Doesn't actually get used, but its good to have a template here already
type Diagnostic interface {
	WithClusterContext(cluster string) Diagnostic
}

// Service is the kubernetes discovery and autoscale service
type Service struct {
	mu       sync.Mutex
	configs  []Config
	clusters map[string]*Cluster
	registry scraper.Registry
	diag     Diagnostic
}

// NewService creates a new unopened k8s service
func NewService(c []Config, r scraper.Registry, d Diagnostic) (*Service, error) {
	clusters := make(map[string]*Cluster, len(c))
	for i := range c {
		cluster, err := NewCluster(c[i], d.WithClusterContext(c[i].ID))
		if err != nil {
			return nil, err
		}
		clusters[c[i].ID] = cluster
	}

	return &Service{
		clusters: clusters,
		configs:  c,
		diag:     d,
		registry: r,
	}, nil
}

// Open starts the kubernetes service
func (s *Service) Open() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.clusters {
		if err := c.Open(); err != nil {
			return err
		}
	}
	s.register()
	return s.registry.Commit()
}

func (s *Service) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.clusters {
		c.Close()
	}
	s.deregister()
	return s.registry.Commit()
}

func (s *Service) deregister() {
	// Remove all the configurations in the registry
	for _, d := range s.configs {
		s.registry.RemoveDiscoverer(&d)
	}
}

func (s *Service) register() {
	// Add all configurations to registry
	for _, d := range s.configs {
		if d.Enabled {
			s.registry.AddDiscoverer(&d)
		}
	}
}

func (s *Service) Update(newConfigs []interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	configs := make([]Config, len(newConfigs))
	existingClusters := make(map[string]bool, len(newConfigs))
	for i := range newConfigs {
		c, ok := newConfigs[i].(Config)
		if !ok {
			return fmt.Errorf("expected config object to be of type %T, got %T", c, newConfigs[i])
		}
		configs[i] = c
		cluster, ok := s.clusters[c.ID]
		if !ok {
			var err error
			cluster, err = NewCluster(c, s.diag.WithClusterContext(c.ID))
			if err != nil {
				return err
			}
			if err := cluster.Open(); err != nil {
				return err
			}
			s.clusters[c.ID] = cluster
		} else {
			if err := cluster.Update(c); err != nil {
				return err
			}
		}
		existingClusters[c.ID] = true
	}

	// Close and delete any removed clusters
	for id := range s.clusters {
		if !existingClusters[id] {
			s.clusters[id].Close()
			delete(s.clusters, id)
		}
	}
	s.deregister()
	s.configs = configs
	s.register()
	return nil
}

type testOptions struct {
	ID string `json:"id"`
}

func (s *Service) TestOptions() interface{} {
	return new(testOptions)
}

func (s *Service) Test(options interface{}) error {
	o, ok := options.(*testOptions)
	if !ok {
		return fmt.Errorf("unexpected options type %T", options)
	}
	s.mu.Lock()
	cluster, ok := s.clusters[o.ID]
	s.mu.Unlock()
	if !ok {
		return fmt.Errorf("unknown kubernetes cluster %q", o.ID)
	}
	return cluster.Test()
}

func (s *Service) Client(id string) (client.Client, error) {
	s.mu.Lock()
	cluster, ok := s.clusters[id]
	s.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("unknown kubernetes cluster %q, cannot get client", id)
	}
	return cluster.Client()
}
