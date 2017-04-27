package marathon

import (
	"fmt"
	"log"
	"sync"

	"github.com/influxdata/kapacitor/services/scraper"
)

// Service is the marathon discovery service
type Service struct {
	Configs []Config
	mu      sync.Mutex

	registry scraper.Registry

	logger *log.Logger
	open   bool
}

// NewService creates a new unopened service
func NewService(c []Config, r scraper.Registry, l *log.Logger) *Service {
	return &Service{
		Configs:  c,
		registry: r,
		logger:   l,
	}
}

// Open starts the service
func (s *Service) Open() error {
	if s.open {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.open = true
	s.register()

	s.logger.Println("I! opened service")
	return s.registry.Commit()
}

// Close stops the service
func (s *Service) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.open {
		return nil
	}

	s.open = false
	s.deregister()

	s.logger.Println("I! closed service")
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
	for i, c := range newConfigs {
		if config, ok := c.(Config); ok {
			configs[i] = config
		} else {
			return fmt.Errorf("unexpected config object type, got %T exp %T", c, config)
		}
	}

	s.deregister()
	s.Configs = configs
	s.register()

	return s.registry.Commit()
}

// TestOptions returns an object that is in turn passed to Test.
func (s *Service) TestOptions() interface{} {
	return nil
}

// Test a service with the provided options.
func (s *Service) Test(options interface{}) error {
	return nil
}
