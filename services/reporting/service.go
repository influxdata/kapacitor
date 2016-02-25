// Sends anonymous reports to InfluxData
package reporting

import (
	"log"
	"runtime"
	"sync"
	"time"

	"github.com/influxdata/kapacitor"
	"github.com/influxdb/usage-client/v1"
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
	s.clusterID = kapacitor.ClusterIDVar.StringValue()
	s.serverID = kapacitor.ServerIDVar.StringValue()
	s.hostname = kapacitor.HostVar.StringValue()
	s.version = kapacitor.VersionVar.StringValue()
	s.product = kapacitor.Product

	// Populate anonymous tags
	s.tags = make(client.Tags)
	s.tags["version"] = s.version
	s.tags["arch"] = runtime.GOARCH
	s.tags["os"] = runtime.GOOS

	// Send periodic anonymous usage stats
	s.usageTicker = time.NewTicker(reportingInterval)
	s.wg.Add(1)
	go s.usage()
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
	defer s.wg.Done()
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
	data.Values[kapacitor.ClusterIDVarName] = s.clusterID
	data.Values[kapacitor.ServerIDVarName] = s.serverID
	data.Values[kapacitor.NumTasksVarName] = kapacitor.NumTasksVar.IntValue()
	data.Values[kapacitor.NumEnabledTasksVarName] = kapacitor.NumEnabledTasksVar.IntValue()
	data.Values[kapacitor.NumSubscriptionsVarName] = kapacitor.NumSubscriptionsVar.IntValue()
	data.Values[kapacitor.UptimeVarName] = kapacitor.Uptime().Seconds()

	usage := client.Usage{
		Product: kapacitor.Product,
		Data:    []client.UsageData{data},
	}

	resp, err := s.client.Save(usage)
	if resp != nil {
		resp.Body.Close()
	}
	return err
}
