package scraper

/*
TODO:
- validate (at least one discovery)
- update running configuration (change from interface) 1 hour
- Add db/rp (specify in coniguration) 1 hour
- Fixup logging (vendoring?) 2 hour delay)
- test scraping 2 hours+
- test discover 2 hours+
- blacklisting? 2 hour+
redact 30 mins
*/
import (
	"fmt"
	"log"
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

// TargetManager represents a scraping/discovery manager
type TargetManager interface {
	ApplyConfig(cfg *config.Config) error
	Stop()
	Run()
}

// Service represents the scraper manager
type Service struct {
	PointsWriter interface {
		WritePoints(database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, points []models.Point) error
	}
	mu      sync.Mutex
	wg      sync.WaitGroup
	open    bool
	closing chan struct{}
	configs []Config

	logger *log.Logger

	discoverers []Discoverer
	mgr         TargetManager
}

// NewServicecreates a new scraper service
func NewService(c []Config, l *log.Logger) *Service {
	s := &Service{
		configs: c,
		logger:  l,
	}
	s.mgr = retrieval.NewTargetManager(s)
	return s
}

// Open starts the scraper service
func (s *Service) Open() error {
	if s.open {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	s.open = true
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
		pairs := s.Pairs()

		s.mu.Lock()
		err := s.apply(pairs)
		s.mu.Unlock()

		if err != nil {
			panic(err)
		}

		s.mgr.Run()
	}()
	select {
	case <-s.closing:
		s.mgr.Stop()
	}
	return
}

// Append tranforms prometheus samples and inserts data into the tasks pipeline
func (s *Service) Append(sample *model.Sample) error {
	p := float64(sample.Value)
	tags := map[string]string{}
	for name, value := range sample.Metric {
		tags[string(name)] = string(value)
	}
	fields := models.Fields{}
	fields["value"] = p
	pt, err := models.NewPoint("meas", models.NewTags(tags), fields, sample.Timestamp.Time())
	if err != nil {
		return err
	}
	// TODO: figure out how to get db/rp
	s.PointsWriter.WritePoints("cpg", "myrp", models.ConsistencyLevelAny, []models.Point{pt})
	return nil
}

// NeedsThrottling conforms to SampleAppender and never returns true currently.
func (s *Service) NeedsThrottling() bool {
	return false
}

type testOptions struct {
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
	pairs := s.Pairs()
	return s.apply(pairs)
}

// apply assumes service is locked
func (s *Service) apply(pairs []Pair) error {
	conf := &config.Config{
		ScrapeConfigs: make([]*config.ScrapeConfig, len(pairs)),
	}
	for i, pair := range pairs {
		sc := pair.Scraper.Prom()
		pair.Discoverer.Prom(sc)
		conf.ScrapeConfigs[i] = sc
	}
	return s.mgr.ApplyConfig(conf)
}

// Commit applies the configuration to the scraper
func (s *Service) Commit() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	pairs := s.Pairs()
	return s.apply(pairs)
}

// AddDiscoverer adds discoverer to the registry
func (s *Service) AddDiscoverer(discoverer Discoverer) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.discoverers = append(s.discoverers, discoverer)
}

// RemoveDiscoverer removes discoverer from the registry
func (s *Service) RemoveDiscoverer(rm Discoverer) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i, d := range s.discoverers {
		if d.ID() == rm.ID() && d.Service() == rm.Service() {
			s.discoverers = append(s.discoverers[:i], s.discoverers[i+1:]...)
		}
	}
}

// AddScrapers adds scrapers to the registry
func (s *Service) AddScrapers(scrapers []Config) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.configs = append(s.configs, scrapers...)
}

// RemoveScrapers removes scrapers from the registry
func (s *Service) RemoveScrapers(scrapers []Config) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, rm := range scrapers {
		for i, c := range s.configs {
			if c.Name == rm.Name {
				s.configs = append(s.configs[:i], s.configs[i+1:]...)
			}
		}
	}
}

// Pairs returns all named pairs of scrapers and discoverers from registry
func (s *Service) Pairs() []Pair {
	pairs := []Pair{}
	for _, scr := range s.configs {
		if !scr.Enabled {
			continue
		}
		for _, d := range s.discoverers {
			if scr.DiscoverName == d.ID() && scr.DiscoverService == d.Service() {
				pairs = append(pairs, Pair{
					Scraper:    scr,
					Discoverer: d,
				})
			}
		}
	}
	return pairs
}
