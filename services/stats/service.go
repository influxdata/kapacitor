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
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/influxdata/enterprise-client/v2"
	"github.com/influxdata/enterprise-client/v2/admin"
	"github.com/influxdata/kapacitor"
	"github.com/influxdata/kapacitor/models"
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

	open    bool
	closing chan struct{}
	mu      sync.Mutex
	wg      sync.WaitGroup

	logger *log.Logger

	enterpriseHosts   []*client.Host
	adminPort         string
	adminHost         string
	token             string
	secret            string
	productRegistered chan struct{}

	clusterID string
	productID string
	hostname  string
	version   string
	product   string
}

func NewService(c Config, l *log.Logger) *Service {
	return &Service{
		interval:        time.Duration(c.StatsInterval),
		db:              c.Database,
		rp:              c.RetentionPolicy,
		logger:          l,
		enterpriseHosts: c.EnterpriseHosts,
		adminPort:       fmt.Sprintf(":%d", c.AdminPort),
		adminHost:       c.AdminHost,
	}
}

func (s *Service) Open() (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Populate published vars
	s.clusterID = kapacitor.GetStringVar(kapacitor.ClusterIDVarName)
	s.productID = kapacitor.GetStringVar(kapacitor.ServerIDVarName)
	s.hostname = kapacitor.GetStringVar(kapacitor.HostVarName)
	s.version = kapacitor.GetStringVar(kapacitor.VersionVarName)
	s.product = kapacitor.Product

	s.stream, err = s.TaskMaster.Stream("stats")
	if err != nil {
		return
	}
	s.open = true
	s.closing = make(chan struct{})
	s.productRegistered = make(chan struct{})

	if err := s.registerServer(); err != nil {
		s.logger.Println("E! Unable to register with Enterprise")
	}
	s.wg.Add(2)
	go s.sendStats()
	go s.launchAdminInterface()
	s.logger.Println("I! opened service")
	return
}

func (s *Service) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.open {
		return errors.New("error closing stats service: service not open")
	}
	s.open = false
	close(s.closing)
	s.wg.Wait()
	s.stream.Close()
	s.logger.Println("I! closed service")
	return nil
}

func (s *Service) registerServer() error {
	if len(s.enterpriseHosts) == 0 {
		return nil
	}

	cl, err := client.New(s.enterpriseHosts)
	if err != nil {
		s.logger.Printf("E! Unable to contact one or more Enterprise hosts: %s\n", err.Error())
		return err
	}

	adminURL, err := url.Parse("http://" + s.adminHost + s.adminPort)
	if err != nil {
		s.logger.Printf("E! Unable to parse admin URL: %s\n", err.Error())
		return err
	}

	product := client.Product{
		ClusterID: s.clusterID,
		ProductID: s.productID,
		Host:      s.hostname,
		Name:      s.product,
		Version:   s.version,
		AdminURL:  adminURL.String(),
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		_, err := cl.Register(&product)

		if err != nil {
			s.logger.Printf("E! failed to register Kapacitor, err: %s", err)
			return
		} else {
			s.logger.Println("I! Registered product")
		}

		s.enterpriseHosts = cl.Hosts
		for _, host := range s.enterpriseHosts {
			if host.Primary {
				s.token = host.Token
				s.secret = host.SecretKey
			}
		}
		close(s.productRegistered)
	}()
	return nil
}

func (s *Service) launchAdminInterface() error {
	defer s.wg.Done()

	<-s.productRegistered

	srv := &http.Server{
		Addr:         s.adminPort,
		Handler:      admin.App(s.token, []byte(s.secret)),
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	l, err := net.Listen("tcp", s.adminPort)
	if err != nil {
		s.logger.Printf("E! Unable to bind enterprise admin interface to port %s. err: %s\n", s.adminPort, err)
		return err
	}
	go srv.Serve(l)
	select {
	case <-s.closing:
		s.logger.Println("Shutting down enterprise admin interface")
		l.Close()
	}
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

	if len(s.enterpriseHosts) != 0 {
		eData := make([]client.StatsData, len(data))

		for i, d := range data {
			eData[i] = client.StatsData{
				Name:   d.Name,
				Tags:   d.Tags,
				Values: client.Values(d.Values),
			}
		}

		eStats := client.Stats{
			ProductName: s.product,
			ClusterID:   s.clusterID,
			ProductID:   s.productID,
			Data:        eData,
		}

		cl, err := client.New(s.enterpriseHosts)
		if err != nil {
			s.logger.Printf("E! Unable to contact one or or more Enterprise hosts: %s\n", err.Error())
			return
		}

		resp, err := cl.Save(eStats)
		if err != nil {
			s.logger.Printf("E! Error saving stats to Enterprise: response code: %d: error: %s", resp.StatusCode, err)
		}
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
