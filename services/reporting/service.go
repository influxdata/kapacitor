// Sends anonymous reports to InfluxData
package reporting

import (
	"log"
	"runtime"
	"sync"
	"time"

	"github.com/influxdata/kapacitor/vars"
	client "github.com/influxdata/usage-client/v1"
)

const reportingInterval = time.Hour * 12

// Sends anonymous usage information every 12 hours.
type Service struct {
	tags client.Tags

	client *client.Client

	clusterID string
	serverID  string
	hostname  string
	version   string
	product   string

	statsTicker *time.Ticker
	usageTicker *time.Ticker
	closing     chan struct{}
	logger      *log.Logger
	wg          sync.WaitGroup
}

func NewService(c Config, l *log.Logger) *Service {
	client := client.New("")
	client.URL = c.URL
	return &Service{
		client: client,
		logger: l,
	}
}

func (s *Service) Open() error {
	if s.closing == nil {
		s.closing = make(chan struct{})
	}

	// Populate published vars
	s.clusterID = vars.ClusterIDVar.StringValue()
	s.serverID = vars.ServerIDVar.StringValue()
	s.hostname = vars.HostVar.StringValue()
	s.version = vars.VersionVar.StringValue()
	s.product = vars.Product

	// Populate anonymous tags
	s.tags = make(client.Tags)
	s.tags["version"] = s.version
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
	data.Values[vars.ClusterIDVarName] = s.clusterID
	data.Values[vars.ServerIDVarName] = s.serverID
	data.Values[vars.NumTasksVarName] = vars.NumTasksVar.IntValue()
	data.Values[vars.NumEnabledTasksVarName] = vars.NumEnabledTasksVar.IntValue()
	data.Values[vars.NumSubscriptionsVarName] = vars.NumSubscriptionsVar.IntValue()
	data.Values[vars.UptimeVarName] = vars.Uptime().Seconds()

	usage := client.Usage{
		Product: vars.Product,
		Data:    []client.UsageData{data},
	}

	resp, err := s.client.Save(usage)
	if resp != nil {
		resp.Body.Close()
	}
	return err
}
