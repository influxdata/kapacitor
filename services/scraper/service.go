package scraper

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/kapacitor/edge"
	"github.com/influxdata/kapacitor/models"
	plog "github.com/prometheus/common/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/discovery"
	"github.com/prometheus/prometheus/pkg/exemplar"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/scrape"
	"github.com/prometheus/prometheus/storage"
)

var (
	_ Registry         = &Service{}
	_ storage.Appender = &ServiceAppenderAdapter{}
)

// Prometheus logger
type Diagnostic interface {
	plog.Logger
	Log(...interface{}) error
}

// Service represents the scraper manager
type Service struct {
	PointsWriter interface {
		WriteKapacitorPoint(edge.PointMessage) error
	}
	mu sync.Mutex
	wg sync.WaitGroup

	open     bool
	closing  chan struct{}
	updating chan *config.Config

	configs atomic.Value // []Config

	diag Diagnostic

	discoverers []Discoverer

	cancelScrape     context.CancelFunc
	scrapeManager    *scrape.Manager
	discoveryManager *discovery.Manager
}

// ServiceAppenderAdapter exposes the service's Append method, but hides the
// Commit method used by the registry to commit config
type ServiceAppenderAdapter struct {
	Wrapped *Service
}

type appendable struct {
	svc *ServiceAppenderAdapter
}

func (a *appendable) Appender(ctx context.Context) storage.Appender {
	return a.svc
}

// NewService creates a new scraper service
func NewService(c []Config, d Diagnostic) *Service {
	s := &Service{
		diag: d,
	}
	s.storeConfigs(c)
	var ctxScrape context.Context
	ctxScrape, s.cancelScrape = context.WithCancel(context.Background())
	s.discoveryManager = discovery.NewManager(ctxScrape, d, discovery.Name("discoveryScrapeManager"))
	s.scrapeManager = scrape.NewManager(d, &appendable{&ServiceAppenderAdapter{s}})

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
	// Roughly match the number of scraper services, so each config change doesn't have to wait.
	s.updating = make(chan *config.Config, 100)
	s.closing = make(chan struct{})

	pairs := s.pairs()
	conf := s.prom(pairs)
	err := s.scrapeManager.ApplyConfig(conf)
	if err != nil {
		return err
	}
	c := make(map[string]discovery.Configs)
	for _, v := range conf.ScrapeConfigs {
		c[v.JobName] = v.ServiceDiscoveryConfigs
	}
	err = s.discoveryManager.ApplyConfig(c)
	if err != nil {
		return err
	}
	s.wg.Add(2)
	go func() {
		defer s.wg.Done()
		s.discoveryManager.Run()
	}()
	go func() {
		defer s.wg.Done()
		s.scrapeManager.Run(s.discoveryManager.SyncCh())
	}()
	go s.scrape()

	s.open = true
	return nil
}

// Close stops the scraper service
func (s *Service) Close() error {
	s.mu.Lock()
	if s.open {
		s.open = false
		s.mu.Unlock()
	} else {
		s.mu.Unlock()
		return nil
	}

	s.cancelScrape()
	s.scrapeManager.Stop()

	close(s.closing)

	s.wg.Wait()

	return nil
}

func (s *Service) loadConfigs() []Config {
	return s.configs.Load().([]Config)
}

func (s *Service) storeConfigs(c []Config) {
	s.configs.Store(c)
}

func (s *Service) scrape() {
	for {
		select {
		case <-s.closing:
			return
		case conf := <-s.updating:
			// only apply the most recent conf
			for {
				select {
				case conf = <-s.updating:
					continue
				default:
				}
				break
			}
			s.scrapeManager.ApplyConfig(conf)
			c := make(map[string]discovery.Configs)
			for _, v := range conf.ScrapeConfigs {
				c[v.JobName] = v.ServiceDiscoveryConfigs
			}
			s.discoveryManager.ApplyConfig(c)
		}
	}
}

// Append tranforms prometheus samples and inserts data into the tasks pipeline
func (s *ServiceAppenderAdapter) Append(ref uint64, labels labels.Labels, timestamp int64, value float64) (uint64, error) {
	return s.Wrapped.Append(ref, labels, timestamp, value)
}

// Append tranforms prometheus samples and inserts data into the tasks pipeline
func (s *Service) Append(_ uint64, labels labels.Labels, timestamp int64, value float64) (uint64, error) {
	// Remove all NaN values
	// TODO: Add counter stat for this variable
	if math.IsNaN(value) {
		return 0, nil
	}

	var err error
	db := ""
	rp := ""
	job := ""

	tags := make(models.Tags)
	for _, kv := range labels {
		if kv.Name == "job" {
			db, rp, job, err = decodeJobName(string(kv.Value))
			if err != nil {
				return 0, err
			}
			continue
		}
		tags[string(kv.Name)] = string(kv.Value)
	}

	// If instance is blacklisted then do not send to PointsWriter
	if instance, ok := tags["instance"]; ok {
		for _, c := range s.loadConfigs() {
			if c.Name == job {
				for _, listed := range c.Blacklist {
					if instance == listed {
						return 0, nil
					}
				}
			}
		}
	}

	fields := models.Fields{
		"value": value,
	}

	return 0, s.PointsWriter.WriteKapacitorPoint(edge.NewPointMessage(
		tags[model.MetricNameLabel],
		db,
		rp,
		models.Dimensions{},
		fields,
		tags,
		time.Unix(timestamp/1000, (timestamp%1000)*1000000), // TODO: confirm timestamp is milliseconds
	))
}

// conforms to Appender interface
func (s *ServiceAppenderAdapter) AppendExemplar(uint64, labels.Labels, exemplar.Exemplar) (uint64, error) {
	// We don't support exemplars
	return 0, nil
}

// conforms to Appender interface, but we don't support transactions
func (s *ServiceAppenderAdapter) Rollback() error {
	return nil
}

// conforms to Appender interface, but we don't support transactions
func (s *ServiceAppenderAdapter) Commit() error {
	return nil
}

// NeedsThrottling conforms to Appender and never returns true currently.
func (s *ServiceAppenderAdapter) NeedsThrottling() bool {
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

	s.storeConfigs(configs)
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

	configs := s.loadConfigs()
	configs = append(configs, scrapers...)
	s.storeConfigs(configs)
}

// RemoveScrapers removes scrapers from the registry
func (s *Service) RemoveScrapers(scrapers []Config) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.open {
		return
	}

	configs := s.loadConfigs()[0:0]
	for _, rm := range scrapers {
		for _, c := range configs {
			if c.Name != rm.Name {
				configs = append(configs, c)
			}
		}
	}
	s.storeConfigs(configs)
}

// pairs returns all named pairs of scrapers and discoverers from registry must be locked
func (s *Service) pairs() []Pair {
	pairs := []Pair{}
	for _, scr := range s.loadConfigs() {
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
