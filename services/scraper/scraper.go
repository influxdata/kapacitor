package scraper

import (
	"log"
	"sync"

	"github.com/influxdata/influxdb/models"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/retrieval"
	"github.com/prometheus/prometheus/storage"
)

var _ storage.SampleAppender = &Service{}

// MockAppender prints samples
type MockAppender struct {
}

func (m *MockAppender) Append(sample *model.Sample) error {
	log.Printf("%#v", sample)
	return nil
}

func (m *MockAppender) NeedsThrottling() bool {
	return false
}

type Service struct {
	PointsWriter interface {
		WritePoints(database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, points []models.Point) error
	}
	mu      sync.Mutex
	wg      sync.WaitGroup
	open    bool
	closing chan struct{}
	config  Config
	logger  *log.Logger
}

func NewService(c Config, l *log.Logger) *Service {
	return &Service{
		config: c,
		logger: l,
	}
}

func (s *Service) Open() (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.open = true
	s.closing = make(chan struct{})
	s.wg.Add(1)
	go s.scrape()
	s.logger.Println("I! opened service")
	return
}

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
	conf, err := config.LoadFile("prom.yaml")
	if err != nil {
		log.Printf("couldn't load configuration %v", err)
		return
	}

	sampleAppender := s
	targetManager := retrieval.NewTargetManager(sampleAppender)
	targetManager.ApplyConfig(conf)
	targetManager.Run()
	return
}

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
	s.PointsWriter.WritePoints("cpg", "myrp", models.ConsistencyLevelAny, []models.Point{pt})
	return nil
}

func (s *Service) NeedsThrottling() bool {
	return false
}

type testOptions struct {
}

func (s *Service) TestOptions() interface{} {
	return &testOptions{}
}

func (s *Service) Test(options interface{}) error {
	return nil
}

func (s *Service) Update(newConfigs []interface{}) error {
	return nil
}
