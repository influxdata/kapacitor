package scraper

import (
	"fmt"
	"log"
	"math"
	"sync"

	"github.com/influxdata/influxdb/models"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/retrieval"
	"github.com/prometheus/prometheus/storage"
)

var (
	_ Registry               = &Service{}
	_ storage.SampleAppender = &Service{}
)

// Service represents the scraper manager
type Service struct {
	PointsWriter interface {
		WritePoints(database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, points []models.Point) error
	}
	mu sync.Mutex
	wg sync.WaitGroup

	open     bool
	running  bool
	closing  chan struct{}
	updating chan *config.Config

	configs []Config

	logger *log.Logger

	discoverers []Discoverer

	// TargetManager represents a scraping/discovery manager
	mgr interface {
		ApplyConfig(cfg *config.Config) error
		Stop()
		Start()
		Wait()
	}
}

// NewService creates a new scraper service
func NewService(c []Config, l *log.Logger) *Service {
	s := &Service{
		configs: c,
		logger:  l,
	}
	s.mgr = retrieval.NewTargetManager(s, NewLogger(l))
	return s
}

// Open starts the scraper service
func (s *Service) Open() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.open {
		return nil
	}

	s.open = true
	s.updating = make(chan *config.Config)
	s.closing = make(chan struct{})

	go s.scrape()
	s.logger.Println("I! opened service")

	s.open = true
	return nil
}

// Close stops the scraper service
func (s *Service) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.open {
		return nil
	}

	s.open = false
	close(s.closing)

	s.wg.Wait()

	s.logger.Println("I! closed service")
	return nil
}

func (s *Service) scrape() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.mu.Lock()
		pairs := s.pairs()
		conf := s.prom(pairs)
		s.mu.Unlock()

		s.mgr.ApplyConfig(conf)

		s.mu.Lock()
		// Need to check open if service was stopped just before this lock
		if s.open {
			s.mgr.Start()
			s.running = true
		}
		s.mu.Unlock()
		// Wait will block until mgr.Stop has been called.
		s.mgr.Wait()
	}()

	for {
		select {
		case <-s.closing:
			s.mu.Lock()
			// Need to check if the targetmanager is even running before Stopping
			if s.running {
				s.mgr.Stop()
				s.running = false
			}
			s.mu.Unlock()
			return
		case conf := <-s.updating:
			s.logger.Println("I! updating scraper service")
			s.mgr.ApplyConfig(conf)
		}
	}
}

// Append tranforms prometheus samples and inserts data into the tasks pipeline
func (s *Service) Append(sample *model.Sample) error {
	value := float64(sample.Value)
	// Remove all NaN values
	// TODO: Add counter stat for this variable
	if math.IsNaN(value) {
		return nil
	}

	var err error
	db := ""
	rp := ""
	job := ""

	tags := map[string]string{}
	for name, value := range sample.Metric {
		if name == "job" {
			db, rp, job, err = decodeJobName(string(value))
			continue
		}
		tags[string(name)] = string(value)
	}

	blacklist := func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		// If instance is blacklisted then do not send to PointsWriter
		if instance, ok := tags["instance"]; ok {
			for i := range s.configs {
				if s.configs[i].Name == job {
					for _, listed := range s.configs[i].Blacklist {
						if instance == listed {
							return true
						}
					}
				}
			}
		}
		return false
	}
	if blacklist() {
		return nil
	}

	fields := models.Fields{
		"value": value,
	}

	pt, err := models.NewPoint(job, models.NewTags(tags), fields, sample.Timestamp.Time())
	if err != nil {
		return err
	}

	return s.PointsWriter.WritePoints(db, rp, models.ConsistencyLevelAny, []models.Point{pt})
}

// NeedsThrottling conforms to SampleAppender and never returns true currently.
func (s *Service) NeedsThrottling() bool {
	return false
}

type testOptions struct {
	Name string `json:"name"`
}

// TestOptions returns options that are allowed for the Test
func (s *Service) TestOptions() interface{} {
	return &testOptions{}
}

// Test tests the options for the scrapers
func (s *Service) Test(options interface{}) error {
	return nil
}

// Update will replace all scraper configurations and apply the configuration
// to the target manager
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

	s.configs = configs
	if s.open {
		pairs := s.pairs()
		conf := s.prom(pairs)
		select {
		case <-s.closing:
			return fmt.Errorf("error writing configuration to closed scraper")
		case s.updating <- conf:
		}
	}
	return nil
}

// prom assumes service is locked
func (s *Service) prom(pairs []Pair) *config.Config {
	conf := &config.Config{
		ScrapeConfigs: make([]*config.ScrapeConfig, len(pairs)),
	}

	for i, pair := range pairs {
		sc := pair.Scraper.Prom()
		pair.Discoverer.Prom(sc)
		conf.ScrapeConfigs[i] = sc
	}
	return conf
}

// Commit applies the configuration to the scraper
func (s *Service) Commit() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.open {
		return nil
	}

	pairs := s.pairs()
	conf := s.prom(pairs)
	select {
	case <-s.closing:
		return fmt.Errorf("error writing configuration to closed registry")
	case s.updating <- conf:
	}

	return nil
}

// AddDiscoverer adds discoverer to the registry
func (s *Service) AddDiscoverer(discoverer Discoverer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.open {
		return
	}

	s.discoverers = append(s.discoverers, discoverer)
}

// RemoveDiscoverer removes discoverer from the registry
func (s *Service) RemoveDiscoverer(rm Discoverer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.open {
		return
	}

	filtered := s.discoverers[0:0]
	for _, d := range s.discoverers {
		if d.ServiceID() != rm.ServiceID() || d.Service() != rm.Service() {
			filtered = append(filtered, d)
		}
	}
	s.discoverers = filtered
}

// AddScrapers adds scrapers to the registry
func (s *Service) AddScrapers(scrapers []Config) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.open {
		return
	}

	s.configs = append(s.configs, scrapers...)
}

// RemoveScrapers removes scrapers from the registry
func (s *Service) RemoveScrapers(scrapers []Config) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.open {
		return
	}

	for _, rm := range scrapers {
		for i, c := range s.configs {
			if c.Name == rm.Name {
				s.configs = append(s.configs[:i], s.configs[i+1:]...)
			}
		}
	}
}

// pairs returns all named pairs of scrapers and discoverers from registry must be locked
func (s *Service) pairs() []Pair {
	pairs := []Pair{}
	for _, scr := range s.configs {
		if !scr.Enabled {
			continue
		}
		for _, d := range s.discoverers {
			if scr.DiscoverID == d.ServiceID() && scr.DiscoverService == d.Service() {
				pairs = append(pairs, Pair{
					Scraper:    scr,
					Discoverer: d,
				})
			}
		}
	}
	return pairs
}

// Pairs returns all named pairs of scrapers and discoverers from registry must be locked
func (s *Service) Pairs() []Pair {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.pairs()
}
