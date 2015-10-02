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
	"github.com/influxdb/kapacitor/services/replay"
	"github.com/influxdb/kapacitor/services/streamer"
	"github.com/influxdb/kapacitor/services/task_store"

	"github.com/influxdb/influxdb/influxql"
	"github.com/influxdb/influxdb/meta"
	"github.com/influxdb/influxdb/services/collectd"
	"github.com/influxdb/influxdb/services/graphite"
	"github.com/influxdb/influxdb/services/opentsdb"
	"github.com/influxdb/influxdb/services/udp"
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

	HTTPDService  *httpd.Service
	Streamer      *streamer.Service
	TaskStore     *task_store.Service
	ReplayService *replay.Service

	MetaStore     *metastore
	QueryExecutor *queryexecutor

	Services []Service

	// Server reporting
	reportingDisabled bool

	// Profiling
	CPUProfile string
	MemProfile string
}

// NewServer returns a new instance of Server built from a config.
func NewServer(c *Config, buildInfo *BuildInfo) (*Server, error) {

	s := &Server{
		buildInfo:     *buildInfo,
		err:           make(chan error),
		closing:       make(chan struct{}),
		TaskMaster:    kapacitor.NewTaskMaster(),
		MetaStore:     &metastore{},
		QueryExecutor: &queryexecutor{},

		reportingDisabled: c.ReportingDisabled,
	}

	// Start Task Master
	if err := s.TaskMaster.Open(); err != nil {
		return nil, err
	}

	// Append Kapacitor services.
	s.appendStreamerService()
	s.appendHTTPDService(c.HTTP)
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
	srv := streamer.NewService()
	srv.StreamCollector = s.TaskMaster.Stream

	s.Streamer = srv
	s.Services = append(s.Services, srv)
}

func (s *Server) appendHTTPDService(c httpd.Config) {
	srv := httpd.NewService(c)

	srv.Handler.MetaStore = s.MetaStore
	srv.Handler.PointsWriter = s.Streamer
	srv.Handler.Version = s.buildInfo.Version

	s.HTTPDService = srv
	s.TaskMaster.HTTPDService = srv
	s.Services = append(s.Services, srv)
}

func (s *Server) appendTaskStoreService(c task_store.Config) {
	srv := task_store.NewService(c)
	srv.HTTPDService = s.HTTPDService
	srv.TaskMaster = s.TaskMaster

	s.TaskStore = srv
	s.Services = append(s.Services, srv)
}

func (s *Server) appendReplayStoreService(c replay.Config) {
	srv := replay.NewService(c)
	srv.TaskStore = s.TaskStore
	srv.HTTPDService = s.HTTPDService
	srv.TaskMaster = s.TaskMaster

	s.ReplayService = srv
	s.Services = append(s.Services, srv)
}

func (s *Server) appendCollectdService(c collectd.Config) {
	if !c.Enabled {
		return
	}
	srv := collectd.NewService(c)
	srv.MetaStore = s.MetaStore
	srv.PointsWriter = s.Streamer
	s.Services = append(s.Services, srv)
}

func (s *Server) appendOpenTSDBService(c opentsdb.Config) error {
	if !c.Enabled {
		return nil
	}
	srv, err := opentsdb.NewService(c)
	if err != nil {
		return err
	}
	srv.PointsWriter = s.Streamer
	srv.MetaStore = s.MetaStore
	s.Services = append(s.Services, srv)
	return nil
}

func (s *Server) appendGraphiteService(c graphite.Config) error {
	if !c.Enabled {
		return nil
	}
	srv, err := graphite.NewService(c)
	if err != nil {
		return err
	}

	srv.PointsWriter = s.Streamer
	srv.MetaStore = s.MetaStore
	s.Services = append(s.Services, srv)
	return nil
}

func (s *Server) appendUDPService(c udp.Config) {
	if !c.Enabled {
		return
	}
	srv := udp.NewService(c)
	srv.PointsWriter = s.Streamer
	s.Services = append(s.Services, srv)
}

// Err returns an error channel that multiplexes all out of band errors received from all services.
func (s *Server) Err() <-chan error { return s.err }

// Open opens all the services.
func (s *Server) Open() error {
	if err := func() error {
		// Start profiling, if set.
		startProfile(s.CPUProfile, s.MemProfile)

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
	stopProfile()

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
			log.Printf("no leader available for reporting: %s", err.Error())
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
func startProfile(cpuprofile, memprofile string) {
	if cpuprofile != "" {
		f, err := os.Create(cpuprofile)
		if err != nil {
			log.Fatalf("cpuprofile: %v", err)
		}
		log.Printf("writing CPU profile to: %s\n", cpuprofile)
		prof.cpu = f
		pprof.StartCPUProfile(prof.cpu)
	}

	if memprofile != "" {
		f, err := os.Create(memprofile)
		if err != nil {
			log.Fatalf("memprofile: %v", err)
		}
		log.Printf("writing mem profile to: %s\n", memprofile)
		prof.mem = f
		runtime.MemProfileRate = 4096
	}

}

// StopProfile closes the cpu and memory profiles if they are running.
func stopProfile() {
	if prof.cpu != nil {
		pprof.StopCPUProfile()
		prof.cpu.Close()
		log.Println("CPU profile stopped")
	}
	if prof.mem != nil {
		pprof.Lookup("heap").WriteTo(prof.mem, 0)
		prof.mem.Close()
		log.Println("mem profile stopped")
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
