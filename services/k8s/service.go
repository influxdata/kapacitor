package k8s

import (
	"fmt"
	"log"
	"sync"

	"github.com/influxdata/kapacitor/services/k8s/client"
	"github.com/pkg/errors"
)

type Service struct {
	mu       sync.Mutex
	clusters map[string]*Cluster
	logger   *log.Logger
}

func NewService(c []Config, l *log.Logger) (*Service, error) {
	l.Println("D! k8s configs", c)

	clusters := make(map[string]*Cluster, len(c))
	for i := range c {
		cluster, err := NewCluster(c[i], l)
		if err != nil {
			return nil, err
		}
		clusters[c[i].ID] = cluster
	}

	return &Service{
		clusters: clusters,
		logger:   l,
	}, nil
}

func (s *Service) Open() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.clusters {
		if err := c.Open(); err != nil {
			return err
		}
	}
	return nil
}
func (s *Service) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.clusters {
		c.Close()
	}
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
			cluster, err = NewCluster(c, s.logger)
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
	return nil
}

type testOptions struct {
	ID string `json:"id"`
}

func (s *Service) TestOptions() interface{} {
	return new(testOptions)
}

func (s *Service) Test(options interface{}) error {
	o, ok := options.(testOptions)
	if !ok {
		return errors.New("unexpected test options")
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
