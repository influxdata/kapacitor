package swarm

import (
	"fmt"
	"sync"

	"github.com/influxdata/kapacitor/services/swarm/client"
	"github.com/pkg/errors"
)

type Diagnostic interface {
	WithClusterContext(cluster string) Diagnostic
}

type Service struct {
	mu       sync.Mutex
	clusters map[string]*Cluster
	diag     Diagnostic
}

func NewService(cs Configs, d Diagnostic) (*Service, error) {
	clusters := make(map[string]*Cluster, len(cs))
	for _, c := range cs {
		cluster, err := NewCluster(c, d.WithClusterContext(c.ID))
		if err != nil {
			return nil, errors.Wrapf(err, "failed to create cluster for %q", c.ID)
		}
		clusters[c.ID] = cluster
	}
	return &Service{
		clusters: clusters,
		diag:     d,
	}, nil
}

func (s *Service) Open() error {
	return nil
}
func (s *Service) Close() error {
	return nil
}

func (s *Service) Update(newConfigs []interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	existingClusters := make(map[string]bool, len(newConfigs))
	for i := range newConfigs {
		c, ok := newConfigs[i].(Config)
		if !ok {
			return fmt.Errorf("expected config object to be of type %T, got %T", c, newConfigs[i])
		}
		cluster, ok := s.clusters[c.ID]
		if !ok {
			var err error
			cluster, err = NewCluster(c, s.diag.WithClusterContext(c.ID))
			if err != nil {
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

	// Delete any removed clusters
	for id := range s.clusters {
		if !existingClusters[id] {
			delete(s.clusters, id)
		}
	}
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
		return fmt.Errorf("unknown swarm cluster %q", o.ID)
	}
	return cluster.Test()
}

func (s *Service) Client(id string) (client.Client, error) {
	s.mu.Lock()
	cluster, ok := s.clusters[id]
	s.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("unknown swarm cluster %q, cannot get client", id)
	}
	return cluster.Client()
}
