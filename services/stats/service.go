// The stats service collects the exported stats and submits them to
// the Kapacitor stream under the configured database and retetion policy.
//
// If you want to persist the data to InfluxDB just add a task like so:
//
// Example:
//    stream
//        .from()
//        .influxDBOut()
//
// Assuming using default database and retetion policy run:
// `kapacitor define -name _stats -type stream -tick path/to/above/script.tick -dbrp _kapacitor.default`
//
// If you do create a task to send the data to InfluxDB make sure not to subscribe to that data in InfluxDB.
//
// Example:
//
// [influxdb]
//     ...
//     [influxdb.excluded-subscriptions]
//         _kapacitor = [ "default" ]
//
package stats

import (
	"log"
	"sync"
	"time"

	"github.com/influxdata/kapacitor"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/server/vars"
	"github.com/influxdata/kapacitor/timer"
)

// Sends internal stats back into the Kapacitor stream.
// Internal stats come from running tasks and other
// services running within Kapacitor.
type Service struct {
	TaskMaster interface {
		Stream(name string) (kapacitor.StreamCollector, error)
	}

	stream kapacitor.StreamCollector

	interval time.Duration
	db       string
	rp       string

	timingSampleRate    float64
	timingMovingAvgSize int

	open    bool
	closing chan struct{}
	mu      sync.Mutex
	wg      sync.WaitGroup

	logger *log.Logger
}

func NewService(c Config, l *log.Logger) *Service {
	return &Service{
		interval:            time.Duration(c.StatsInterval),
		db:                  c.Database,
		rp:                  c.RetentionPolicy,
		timingSampleRate:    c.TimingSampleRate,
		timingMovingAvgSize: c.TimingMovingAverageSize,
		logger:              l,
	}
}

func (s *Service) Open() (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stream, err = s.TaskMaster.Stream("stats")
	if err != nil {
		return
	}
	s.open = true
	s.closing = make(chan struct{})
	s.wg.Add(1)
	go s.sendStats()
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
	s.stream.Close()
	return nil
}

func (s *Service) sendStats() {
	defer s.wg.Done()
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-s.closing:
			return
		case <-ticker.C:
			s.reportStats()
		}
	}
}

func (s *Service) reportStats() {
	now := time.Now().UTC()
	data, err := vars.GetStatsData()
	if err != nil {
		s.logger.Println("E! error getting stats data:", err)
		return
	}
	for _, stat := range data {
		p := models.Point{
			Database:        s.db,
			RetentionPolicy: s.rp,
			Name:            stat.Name,
			Group:           models.NilGroup,
			Tags:            models.Tags(stat.Tags),
			Time:            now,
			Fields:          models.Fields(stat.Values),
		}
		s.stream.CollectPoint(p)
	}
}

func (s *Service) NewTimer(avgVar timer.Setter) timer.Timer {
	return timer.New(s.timingSampleRate, s.timingMovingAvgSize, avgVar)
}
