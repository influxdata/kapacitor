package scraper

import (
	"math"
	"testing"
	"time"

	"github.com/influxdata/influxdb/toml"
	"github.com/influxdata/kapacitor/edge"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/discovery/file"
	"github.com/prometheus/prometheus/model/labels"
)

type fileDiscoverer struct{}

func (fileDiscoverer) Service() string   { return "file-discovery" }
func (fileDiscoverer) ServiceID() string { return "id" }
func (fileDiscoverer) Prom(c *config.ScrapeConfig) {
	sd := file.DefaultSDConfig
	sd.Files = []string{"/nonexistent/*.json"}
	c.ServiceDiscoveryConfigs = append(c.ServiceDiscoveryConfigs, &sd)
}

type pointsWriter struct {
	points []edge.PointMessage
}

func (w *pointsWriter) WriteKapacitorPoint(p edge.PointMessage) error {
	w.points = append(w.points, p)
	return nil
}

func newConfig(name string) Config {
	c := Config{}
	c.Init()
	c.Enabled = true
	c.Name = name
	c.Database = "db"
	c.RetentionPolicy = "rp"
	return c
}

func TestService_PromIsValid(t *testing.T) {
	s := &Service{}
	conf := s.prom([]Pair{{Discoverer: fileDiscoverer{}, Scraper: newConfig("a")}})

	// The scrape manager rejects the whole config if this fails.
	scfgs, err := conf.GetScrapeConfigs()
	if err != nil {
		t.Fatal(err)
	}
	if len(scfgs) != 1 {
		t.Fatalf("unexpected scrape configs: got %d exp 1", len(scfgs))
	}
	sc := scfgs[0]
	if len(sc.ScrapeProtocols) == 0 {
		t.Error("expected default scrape protocols to be set")
	}
	if !sc.EnableCompression {
		t.Error("expected compression to be enabled")
	}
	if got, exp := sc.ScrapeTimeout, model.Duration(10*time.Second); got != exp {
		t.Errorf("unexpected scrape timeout: got %v exp %v", got, exp)
	}
}

func TestService_NewService(t *testing.T) {
	// Each service uses its own metrics registry, so creating more than one
	// must not fail with duplicate metric registrations.
	for range 2 {
		s, err := NewService(nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		s.cancelScrape()
	}
}

func TestService_Append(t *testing.T) {
	w := &pointsWriter{}
	s := &Service{PointsWriter: w}
	c := newConfig("job")
	c.Blacklist = []string{"blacklisted:9100"}
	s.storeConfigs([]Config{c})
	job := encodeJobName("db", "rp", "job")

	if _, err := s.Append(0, labels.FromStrings(
		model.MetricNameLabel, "up",
		model.JobLabel, job,
		model.InstanceLabel, "host:9100",
	), 1500, 1); err != nil {
		t.Fatal(err)
	}
	if len(w.points) != 1 {
		t.Fatalf("unexpected points: got %d exp 1", len(w.points))
	}
	p := w.points[0]
	if p.Name() != "up" || p.Database() != "db" || p.RetentionPolicy() != "rp" {
		t.Errorf("unexpected point: name %q db %q rp %q", p.Name(), p.Database(), p.RetentionPolicy())
	}
	if _, ok := p.Tags()[model.JobLabel]; ok {
		t.Error("job label should not be a tag")
	}
	if got, exp := p.Tags()[model.InstanceLabel], "host:9100"; got != exp {
		t.Errorf("unexpected instance tag: got %q exp %q", got, exp)
	}
	if got, exp := p.Time(), time.Unix(1, 500*int64(time.Millisecond)); !got.Equal(exp) {
		t.Errorf("unexpected time: got %v exp %v", got, exp)
	}

	// Blacklisted instances and NaN values are dropped.
	if _, err := s.Append(0, labels.FromStrings(model.JobLabel, job, model.InstanceLabel, "blacklisted:9100"), 0, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Append(0, labels.FromStrings(model.JobLabel, job), 0, math.NaN()); err != nil {
		t.Fatal(err)
	}
	if len(w.points) != 1 {
		t.Errorf("unexpected points: got %d exp 1", len(w.points))
	}

	if _, err := s.Append(0, labels.FromStrings(model.JobLabel, "invalid"), 0, 1); err == nil {
		t.Error("expected error for invalid job name")
	}
}

func TestConfig_ValidateTimeout(t *testing.T) {
	c := newConfig("a")
	c.ScrapeTimeout = toml.Duration(2 * time.Minute)
	if err := c.Validate(); err == nil {
		t.Error("expected error for timeout greater than interval")
	}
}

func TestNewDiscoverer(t *testing.T) {
	sd := file.DefaultSDConfig
	sd.Files = []string{"/nonexistent/*.json"}
	// Discoverers use their own metrics registry, so creating one for the
	// same config twice must not fail.
	for range 2 {
		d, unregister, err := NewDiscoverer(&sd, nil)
		if err != nil {
			t.Fatal(err)
		}
		if d == nil {
			t.Fatal("expected discoverer")
		}
		unregister()
	}
}
