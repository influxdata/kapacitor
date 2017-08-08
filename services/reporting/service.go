// Sends anonymous reports to InfluxData
package reporting

import (
	"log"
	"runtime"
	"sync"
	"time"

	"github.com/influxdata/kapacitor/server/vars"
	client "github.com/influxdata/usage-client/v1"
)

const reportingInterval = time.Hour * 12

// Sends anonymous usage information every 12 hours.
type Service struct {
	tags client.Tags

	client *client.Client

	info vars.Infoer

	statsTicker *time.Ticker
	usageTicker *time.Ticker
	closing     chan struct{}
	logger      *log.Logger
	wg          sync.WaitGroup
}

func NewService(c Config, info vars.Infoer, l *log.Logger) *Service {
	client := client.New("")
	client.URL = c.URL
	return &Service{
		client: client,
		info:   info,
		logger: l,
	}
}

func (s *Service) Open() error {
	if s.closing == nil {
		s.closing = make(chan struct{})
	}

	// Populate anonymous tags
	s.tags = make(client.Tags)
	s.tags["version"] = s.info.Version()
	s.tags["arch"] = runtime.GOARCH
	s.tags["os"] = runtime.GOOS

	// Send report on startup
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		err := s.sendUsageReport()
		if err != nil {
			s.logger.Println("E! error while sending usage report on startup:", err)
		}
	}()

	// Send periodic anonymous usage stats
	s.usageTicker = time.NewTicker(reportingInterval)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.usage()
	}()
	return nil
}

func (s *Service) Close() error {
	if s.usageTicker != nil {
		s.usageTicker.Stop()
	}
	if s.statsTicker != nil {
		s.statsTicker.Stop()
	}
	if s.closing != nil {
		close(s.closing)
	}
	s.wg.Wait()
	return nil
}

func (s *Service) usage() {
	for {
		select {
		case <-s.closing:
			return
		case <-s.usageTicker.C:
			err := s.sendUsageReport()
			if err != nil {
				s.logger.Println("E! error while sending usage report:", err)
			}
		}
	}
}

// Send anonymous usage report.
func (s *Service) sendUsageReport() error {
	data := client.UsageData{
		Tags:   s.tags,
		Values: make(client.Values),
	}
	// Add values
	data.Values[vars.ClusterIDVarName] = s.info.ClusterID().String()
	data.Values[vars.ServerIDVarName] = s.info.ServerID().String()
	data.Values[vars.NumTasksVarName] = s.info.NumTasks()
	data.Values[vars.NumEnabledTasksVarName] = s.info.NumEnabledTasks()
	data.Values[vars.NumSubscriptionsVarName] = s.info.NumSubscriptions()
	data.Values[vars.UptimeVarName] = s.info.Uptime().Seconds()

	usage := client.Usage{
		Product: s.info.Product(),
		Data:    []client.UsageData{data},
	}

	resp, err := s.client.Save(usage)
	if resp != nil {
		resp.Body.Close()
	}
	return err
}
