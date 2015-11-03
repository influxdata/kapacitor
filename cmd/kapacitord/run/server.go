package run

import (
	"errors"
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"time"

	"github.com/influxdb/kapacitor"
	"github.com/influxdb/kapacitor/services/httpd"
	"github.com/influxdb/kapacitor/services/influxdb"
	"github.com/influxdb/kapacitor/services/logging"
	"github.com/influxdb/kapacitor/services/replay"
	"github.com/influxdb/kapacitor/services/smtp"
	"github.com/influxdb/kapacitor/services/streamer"
	"github.com/influxdb/kapacitor/services/task_store"
	"github.com/influxdb/kapacitor/services/udp"
	"github.com/influxdb/kapacitor/wlog"

	"github.com/influxdb/influxdb/influxql"
	"github.com/influxdb/influxdb/meta"
	"github.com/influxdb/influxdb/services/collectd"
	"github.com/influxdb/influxdb/services/graphite"
	"github.com/influxdb/influxdb/services/opentsdb"
)

// BuildInfo represents the build details for the server code.
type BuildInfo struct {
	Version string
	Commit  string
	Branch  string
}

// Server represents a container for the metadata and storage data and services.
// It is built using a Config and it manages the startup and shutdown of all
// services in the proper order.
type Server struct {
	buildInfo BuildInfo

	err     chan error
	closing chan struct{}

	TaskMaster *kapacitor.TaskMaster

	LogService    *logging.Service
	HTTPDService  *httpd.Service
	Streamer      *streamer.Service
	TaskStore     *task_store.Service
	ReplayService *replay.Service
	InfluxDB      *influxdb.Service
	SMTPService   *smtp.Service

	MetaStore     *metastore
	QueryExecutor *queryexecutor

	Services []Service

	// Server reporting
	reportingDisabled bool

	// Profiling
	CPUProfile string
	MemProfile string

	Logger *log.Logger
}

// NewServer returns a new instance of Server built from a config.
func NewServer(c *Config, buildInfo *BuildInfo, l *log.Logger, logService *logging.Service) (*Server, error) {

	s := &Server{
		buildInfo:     *buildInfo,
		err:           make(chan error),
		closing:       make(chan struct{}),
		LogService:    logService,
		MetaStore:     &metastore{},
		QueryExecutor: &queryexecutor{},

		reportingDisabled: c.ReportingDisabled,
		Logger:            l,
	}
	s.Logger.Println("I! Kapacitor hostname:", c.Hostname)

	// Start Task Master
	s.TaskMaster = kapacitor.NewTaskMaster(logService)
	if err := s.TaskMaster.Open(); err != nil {
		return nil, err
	}

	// Append Kapacitor services.
	s.appendStreamerService()
	s.appendSMTPService(c.SMTP)
	s.appendHTTPDService(c.HTTP)
	s.appendInfluxDBService(c.InfluxDB, c.Hostname)
	s.appendTaskStoreService(c.Task)
	s.appendReplayStoreService(c.Replay)

	// Append InfluxDB services
	s.appendCollectdService(c.Collectd)
	if err := s.appendOpenTSDBService(c.OpenTSDB); err != nil {
		return nil, err
	}
	for _, g := range c.UDPs {
		s.appendUDPService(g)
	}
	for _, g := range c.Graphites {
		if err := s.appendGraphiteService(g); err != nil {
			return nil, err
		}
	}

	return s, nil
}

func (s *Server) appendStreamerService() {
	l := s.LogService.NewLogger("[streamer] ", log.LstdFlags)
	srv := streamer.NewService(l)
	srv.StreamCollector = s.TaskMaster.Stream

	s.Streamer = srv
	s.Services = append(s.Services, srv)
}

func (s *Server) appendSMTPService(c smtp.Config) {
	if c.Enabled {
		l := s.LogService.NewLogger("[smtp] ", log.LstdFlags)
		srv := smtp.NewService(c, l)

		s.SMTPService = srv
		s.TaskMaster.SMTPService = srv
		s.Services = append(s.Services, srv)
	}
}

func (s *Server) appendInfluxDBService(c influxdb.Config, hostname string) {
	l := s.LogService.NewLogger("[influxdb] ", log.LstdFlags)
	srv := influxdb.NewService(c, hostname, l)
	srv.PointsWriter = s.Streamer
	srv.LogService = s.LogService

	s.InfluxDB = srv
	s.TaskMaster.InfluxDBService = srv
	s.Services = append(s.Services, srv)
}

func (s *Server) appendHTTPDService(c httpd.Config) {
	l := s.LogService.NewLogger("[httpd] ", log.LstdFlags)
	srv := httpd.NewService(c, l)

	srv.Handler.MetaStore = s.MetaStore
	srv.Handler.PointsWriter = s.Streamer
	srv.Handler.Version = s.buildInfo.Version

	s.HTTPDService = srv
	s.TaskMaster.HTTPDService = srv
	s.Services = append(s.Services, srv)
}

func (s *Server) appendTaskStoreService(c task_store.Config) {
	l := s.LogService.NewLogger("[task] ", log.LstdFlags)
	srv := task_store.NewService(c, l)
	srv.HTTPDService = s.HTTPDService
	srv.TaskMaster = s.TaskMaster

	s.TaskStore = srv
	s.Services = append(s.Services, srv)
}

func (s *Server) appendReplayStoreService(c replay.Config) {
	l := s.LogService.NewLogger("[replay] ", log.LstdFlags)
	srv := replay.NewService(c, l)
	srv.TaskStore = s.TaskStore
	srv.HTTPDService = s.HTTPDService
	srv.InfluxDBService = s.InfluxDB
	srv.TaskMaster = s.TaskMaster

	s.ReplayService = srv
	s.Services = append(s.Services, srv)
}

func (s *Server) appendCollectdService(c collectd.Config) {
	if !c.Enabled {
		return
	}
	l := s.LogService.NewStaticLevelLogger("[collectd] ", log.LstdFlags, wlog.INFO)
	srv := collectd.NewService(c)
	srv.SetLogger(l)
	srv.MetaStore = s.MetaStore
	srv.PointsWriter = s.Streamer
	s.Services = append(s.Services, srv)
}

func (s *Server) appendOpenTSDBService(c opentsdb.Config) error {
	if !c.Enabled {
		return nil
	}
	l := s.LogService.NewStaticLevelLogger("[opentsdb] ", log.LstdFlags, wlog.INFO)
	srv, err := opentsdb.NewService(c)
	if err != nil {
		return err
	}
	srv.SetLogger(l)
	srv.PointsWriter = s.Streamer
	srv.MetaStore = s.MetaStore
	s.Services = append(s.Services, srv)
	return nil
}

func (s *Server) appendGraphiteService(c graphite.Config) error {
	if !c.Enabled {
		return nil
	}
	l := s.LogService.NewStaticLevelLogger("[graphite] ", log.LstdFlags, wlog.INFO)
	srv, err := graphite.NewService(c)
	if err != nil {
		return err
	}
	srv.SetLogger(l)

	srv.PointsWriter = s.Streamer
	srv.MetaStore = s.MetaStore
	s.Services = append(s.Services, srv)
	return nil
}

func (s *Server) appendUDPService(c udp.Config) {
	if !c.Enabled {
		return
	}
	l := s.LogService.NewLogger("[udp] ", log.LstdFlags)
	srv := udp.NewService(c, l)
	srv.PointsWriter = s.Streamer
	s.Services = append(s.Services, srv)
}

// Err returns an error channel that multiplexes all out of band errors received from all services.
func (s *Server) Err() <-chan error { return s.err }

// Open opens all the services.
func (s *Server) Open() error {
	if err := func() error {
		// Start profiling, if set.
		s.startProfile(s.CPUProfile, s.MemProfile)

		for _, service := range s.Services {
			if err := service.Open(); err != nil {
				return fmt.Errorf("open service: %s", err)
			}
		}

		// Start the reporting service, if not disabled.
		if !s.reportingDisabled {
			//TODO (nathanielc) setup kapacitor reporting
			//go s.startServerReporting()
		}

		return nil

	}(); err != nil {
		s.Close()
		return err
	}

	go func() {
		// Watch if something dies
		var err error
		select {
		case err = <-s.HTTPDService.Err():
		}
		s.err <- err
	}()

	return nil
}

// Close shuts down the meta and data stores and all services.
func (s *Server) Close() error {
	s.stopProfile()

	// Close services to allow any inflight requests to complete
	// and prevent new requests from being accepted.
	for _, service := range s.Services {
		service.Close()
	}

	close(s.closing)
	return nil
}

// startServerReporting starts periodic server reporting.
func (s *Server) startServerReporting() {
	for {
		select {
		case <-s.closing:
			return
		default:
		}
		if err := s.MetaStore.WaitForLeader(30 * time.Second); err != nil {
			s.Logger.Printf("E! no leader available for reporting: %s", err.Error())
			time.Sleep(time.Second)
			continue
		}
		s.reportServer()
		<-time.After(24 * time.Hour)
	}
}

// reportServer reports anonymous statistics about the system.
func (s *Server) reportServer() {

}

// Service represents a service attached to the server.
type Service interface {
	Open() error
	Close() error
}

// prof stores the file locations of active profiles.
var prof struct {
	cpu *os.File
	mem *os.File
}

// StartProfile initializes the cpu and memory profile, if specified.
func (s *Server) startProfile(cpuprofile, memprofile string) {
	if cpuprofile != "" {
		f, err := os.Create(cpuprofile)
		if err != nil {
			s.Logger.Fatalf("E! cpuprofile: %v", err)
		}
		s.Logger.Printf("I! writing CPU profile to: %s\n", cpuprofile)
		prof.cpu = f
		pprof.StartCPUProfile(prof.cpu)
	}

	if memprofile != "" {
		f, err := os.Create(memprofile)
		if err != nil {
			s.Logger.Fatalf("E! memprofile: %v", err)
		}
		s.Logger.Printf("I! writing mem profile to: %s\n", memprofile)
		prof.mem = f
		runtime.MemProfileRate = 4096
	}

}

// StopProfile closes the cpu and memory profiles if they are running.
func (s *Server) stopProfile() {
	if prof.cpu != nil {
		pprof.StopCPUProfile()
		prof.cpu.Close()
		s.Logger.Println("I! CPU profile stopped")
	}
	if prof.mem != nil {
		pprof.Lookup("heap").WriteTo(prof.mem, 0)
		prof.mem.Close()
		s.Logger.Println("I! mem profile stopped")
	}
}

type tcpaddr struct{ host string }

func (a *tcpaddr) Network() string { return "tcp" }
func (a *tcpaddr) String() string  { return a.host }

type metastore struct{}

func (m *metastore) WaitForLeader(d time.Duration) error {
	return nil
}
func (m *metastore) CreateDatabaseIfNotExists(name string) (*meta.DatabaseInfo, error) {
	return nil, nil
}
func (m *metastore) Database(name string) (*meta.DatabaseInfo, error) {
	return &meta.DatabaseInfo{
		Name: name,
	}, nil
}
func (m *metastore) Authenticate(username, password string) (ui *meta.UserInfo, err error) {
	return nil, errors.New("not authenticated")
}
func (m *metastore) Users() ([]meta.UserInfo, error) {
	return nil, errors.New("no user")
}

type queryexecutor struct{}

func (qe *queryexecutor) Authorize(u *meta.UserInfo, q *influxql.Query, db string) error {
	return nil
}
func (qe *queryexecutor) ExecuteQuery(q *influxql.Query, db string, chunkSize int) (<-chan *influxql.Result, error) {
	return nil, errors.New("cannot execute queries against Kapacitor")
}
