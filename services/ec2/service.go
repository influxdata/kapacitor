package ec2

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/influxdata/kapacitor/services/ec2/client"
	"github.com/influxdata/kapacitor/services/scraper"
	"github.com/prometheus/prometheus/config"
	pec2 "github.com/prometheus/prometheus/discovery/ec2"
)

type Diagnostic interface {
	scraper.Diagnostic
	WithClusterContext(region string) Diagnostic
}

// Service is the ec2 discovery and autoscale service
type Service struct {
	Configs  []Config
	mu       sync.Mutex
	clusters map[string]*Cluster
	registry scraper.Registry

	diag Diagnostic
	open bool
}

// NewService creates a new unopened service
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
		Configs:  c,
		registry: r,
		diag:     d,
	}, nil
}

// Open starts the service
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

// Close stops the service
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
	for _, d := range s.Configs {
		s.registry.RemoveDiscoverer(&d)
	}
}

func (s *Service) register() {
	// Add all configurations to registry
	for _, d := range s.Configs {
		if d.Enabled {
			s.registry.AddDiscoverer(&d)
		}
	}
}

// Update updates configuration while running
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
	s.Configs = configs
	s.register()
	return nil
}

type testOptions struct {
	ID string `json:"id"`
}

// TestOptions returns an object that is in turn passed to Test.
func (s *Service) TestOptions() interface{} {
	return &testOptions{}
}

// Test a service with the provided options.
func (s *Service) Test(options interface{}) error {
	o, ok := options.(*testOptions)
	if !ok {
		return fmt.Errorf("unexpected options type %T", options)
	}

	found := -1
	for i := range s.Configs {
		if s.Configs[i].ID == o.ID && s.Configs[i].Enabled {
			found = i
		}
	}
	if found < 0 {
		return fmt.Errorf("discoverer %q is not enabled or does not exist", o.ID)
	}

	sd := s.Configs[found].PromConfig()
	discoverer := pec2.NewDiscovery(sd, s.diag)

	ctx, cancel := context.WithCancel(context.Background())
	updates := make(chan []*config.TargetGroup)
	go discoverer.Run(ctx, updates)

	var err error
	select {
	case _, ok := <-updates:
		// Handle the case that a target provider exits and closes the channel
		// before the context is done.
		if !ok {
			err = fmt.Errorf("discoverer %q exited ", o.ID)
		}
		break
	case <-time.After(30 * time.Second):
		err = fmt.Errorf("timeout waiting for discoverer %q to return", o.ID)
		break
	}
	cancel()

	return err
}

func (s *Service) Client(id string) (client.Client, error) {
	s.mu.Lock()
	cluster, ok := s.clusters[id]
	s.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("unknown ec2 region %q, cannot get client", id)
	}
	return cluster.Client()
}
