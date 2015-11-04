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

	"github.com/influxdb/kapacitor"
	"github.com/influxdb/kapacitor/models"
)

// Sends internal stats back into the Kapacitor stream.
// Internal stats come from running tasks and other
// services running within Kapacitor.
type Service struct {
	StreamCollector interface {
		CollectPoint(models.Point) error
	}

	interval time.Duration
	db       string
	rp       string

	closing chan struct{}
	wg      sync.WaitGroup

	logger *log.Logger
}

func NewService(c Config, l *log.Logger) *Service {
	return &Service{
		interval: time.Duration(c.StatsInterval),
		db:       c.Database,
		rp:       c.RetentionPolicy,
		logger:   l,
	}
}

func (s *Service) Open() error {
	s.closing = make(chan struct{})
	s.wg.Add(1)
	go s.sendStats()
	s.logger.Println("I! opened service")
	return nil
}

func (s *Service) Close() error {
	close(s.closing)
	s.wg.Wait()
	s.logger.Println("I! closed service")
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
	data, err := kapacitor.GetStatsData()
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
		s.StreamCollector.CollectPoint(p)
	}
}
