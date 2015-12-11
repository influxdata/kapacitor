package run

import (
	"errors"
	"expvar"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/influxdb/influxdb/influxql"
	"github.com/influxdb/influxdb/meta"
	"github.com/influxdb/influxdb/services/collectd"
	"github.com/influxdb/influxdb/services/graphite"
	"github.com/influxdb/influxdb/services/opentsdb"
	"github.com/influxdb/kapacitor"
	"github.com/influxdb/kapacitor/services/httpd"
	"github.com/influxdb/kapacitor/services/influxdb"
	"github.com/influxdb/kapacitor/services/logging"
	"github.com/influxdb/kapacitor/services/pagerduty"
	"github.com/influxdb/kapacitor/services/replay"
	"github.com/influxdb/kapacitor/services/reporting"
	"github.com/influxdb/kapacitor/services/slack"
	"github.com/influxdb/kapacitor/services/smtp"
	"github.com/influxdb/kapacitor/services/stats"
	"github.com/influxdb/kapacitor/services/task_store"
	"github.com/influxdb/kapacitor/services/udp"
	"github.com/influxdb/kapacitor/services/victorops"
	"github.com/influxdb/kapacitor/wlog"
	"github.com/twinj/uuid"
)

const clusterIDFilename = "cluster.id"
const serverIDFilename = "server.id"

var (
	//Published vars
	cidVar = &expvar.String{}

	sidVar = &expvar.String{}

	hostVar = &expvar.String{}

	productVar = &expvar.String{}

	versionVar = &expvar.String{}
)

func init() {
	expvar.Publish(kapacitor.ClusterIDVarName, cidVar)
	expvar.Publish(kapacitor.ServerIDVarName, sidVar)
	expvar.Publish(kapacitor.HostVarName, hostVar)
	expvar.Publish(kapacitor.ProductVarName, productVar)
	expvar.Publish(kapacitor.VersionVarName, versionVar)
}

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
	dataDir   string
	hostname  string

	err chan error

	TaskMaster *kapacitor.TaskMaster

	LogService      logging.Interface
	HTTPDService    *httpd.Service
	TaskStore       *task_store.Service
	ReplayService   *replay.Service
	InfluxDBService *influxdb.Service

	MetaStore     *metastore
	QueryExecutor *queryexecutor

	Services []Service

	ClusterID string
	ServerID  string

	// Profiling
	CPUProfile string
	MemProfile string

	Logger *log.Logger
}

// NewServer returns a new instance of Server built from a config.
func NewServer(c *Config, buildInfo *BuildInfo, logService logging.Interface) (*Server, error) {
	l := logService.NewLogger("[srv] ", log.LstdFlags)
	s := &Server{
		buildInfo:     *buildInfo,
		dataDir:       c.DataDir,
		hostname:      c.Hostname,
		err:           make(chan error),
		LogService:    logService,
		MetaStore:     &metastore{},
		QueryExecutor: &queryexecutor{},
		Logger:        l,
	}
	s.Logger.Println("I! Kapacitor hostname:", s.hostname)

	// Start Task Master
	s.TaskMaster = kapacitor.NewTaskMaster(logService)
	if err := s.TaskMaster.Open(); err != nil {
		return nil, err
	}

	// Append Kapacitor services.
	s.appendSMTPService(c.SMTP)
	s.appendHTTPDService(c.HTTP)
	s.appendInfluxDBService(c.InfluxDB, c.Hostname)
	s.appendTaskStoreService(c.Task)
	s.appendReplayStoreService(c.Replay)
	s.appendVictorOpsService(c.VictorOps)
	s.appendPagerDutyService(c.PagerDuty)
	s.appendSlackService(c.Slack)

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

	// append StatsService and ReportingService last so all stats are ready
	// to be reported
	s.appendStatsService(c.Stats)
	s.appendReportingService(c.Reporting, c.Token)

	return s, nil
}

func (s *Server) appendSMTPService(c smtp.Config) {
	if c.Enabled {
		l := s.LogService.NewLogger("[smtp] ", log.LstdFlags)
		srv := smtp.NewService(c, l)

		s.TaskMaster.SMTPService = srv
		s.Services = append(s.Services, srv)
	}
}

func (s *Server) appendInfluxDBService(c influxdb.Config, hostname string) {
	if c.Enabled {
		l := s.LogService.NewLogger("[influxdb] ", log.LstdFlags)
		srv := influxdb.NewService(c, hostname, l)
		srv.PointsWriter = s.TaskMaster
		srv.LogService = s.LogService

		s.InfluxDBService = srv
		s.TaskMaster.InfluxDBService = srv
		s.Services = append(s.Services, srv)
	}
}

func (s *Server) appendHTTPDService(c httpd.Config) {
	l := s.LogService.NewLogger("[httpd] ", log.LstdFlags)
	srv := httpd.NewService(c, l)

	srv.Handler.MetaStore = s.MetaStore
	srv.Handler.PointsWriter = s.TaskMaster
	srv.Handler.Version = s.buildInfo.Version

	s.HTTPDService = srv
	s.TaskMaster.HTTPDService = srv
	s.Services = append(s.Services, srv)
}

func (s *Server) appendTaskStoreService(c task_store.Config) {
	l := s.LogService.NewLogger("[task_store] ", log.LstdFlags)
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
	srv.InfluxDBService = s.InfluxDBService
	srv.TaskMaster = s.TaskMaster

	s.ReplayService = srv
	s.Services = append(s.Services, srv)
}

func (s *Server) appendVictorOpsService(c victorops.Config) {
	if c.Enabled {
		l := s.LogService.NewLogger("[victorops] ", log.LstdFlags)
		srv := victorops.NewService(c, l)
		s.TaskMaster.VictorOpsService = srv

		s.Services = append(s.Services, srv)
	}
}

func (s *Server) appendPagerDutyService(c pagerduty.Config) {
	if c.Enabled {
		l := s.LogService.NewLogger("[pagerduty] ", log.LstdFlags)
		srv := pagerduty.NewService(c, l)
		srv.HTTPDService = s.HTTPDService
		s.TaskMaster.PagerDutyService = srv

		s.Services = append(s.Services, srv)
	}
}

func (s *Server) appendSlackService(c slack.Config) {
	if c.Enabled {
		l := s.LogService.NewLogger("[slack] ", log.LstdFlags)
		srv := slack.NewService(c, l)
		s.TaskMaster.SlackService = srv

		s.Services = append(s.Services, srv)
	}
}

func (s *Server) appendCollectdService(c collectd.Config) {
	if !c.Enabled {
		return
	}
	l := s.LogService.NewStaticLevelLogger("[collectd] ", log.LstdFlags, wlog.INFO)
	srv := collectd.NewService(c)
	srv.SetLogger(l)
	srv.MetaStore = s.MetaStore
	srv.PointsWriter = s.TaskMaster
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
	srv.PointsWriter = s.TaskMaster
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

	srv.PointsWriter = s.TaskMaster
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
	srv.PointsWriter = s.TaskMaster
	s.Services = append(s.Services, srv)
}

func (s *Server) appendStatsService(c stats.Config) {
	if c.Enabled {
		l := s.LogService.NewLogger("[stats] ", log.LstdFlags)
		srv := stats.NewService(c, l)
		srv.TaskMaster = s.TaskMaster

		s.Services = append(s.Services, srv)
	}
}

func (s *Server) appendReportingService(c reporting.Config, token string) {
	if c.Enabled {
		l := s.LogService.NewLogger("[reporting] ", log.LstdFlags)
		srv := reporting.NewService(c, token, l)

		s.Services = append(s.Services, srv)
	}
}

// Err returns an error channel that multiplexes all out of band errors received from all services.
func (s *Server) Err() <-chan error { return s.err }

// Open opens all the services.
func (s *Server) Open() error {
	if err := func() error {
		// Setup IDs
		err := s.setupIDs()
		if err != nil {
			return err
		}

		// Set published vars
		cidVar.Set(s.ClusterID)
		sidVar.Set(s.ServerID)
		hostVar.Set(s.hostname)
		productVar.Set(kapacitor.Product)
		versionVar.Set(s.buildInfo.Version)
		s.Logger.Printf("I! ClusterID: %s ServerID: %s", s.ClusterID, s.ServerID)

		// Start profiling, if set.
		s.startProfile(s.CPUProfile, s.MemProfile)

		for _, service := range s.Services {
			s.Logger.Printf("D! opening service: %T", service)
			if err := service.Open(); err != nil {
				return fmt.Errorf("open service %T: %s", service, err)
			}
			s.Logger.Printf("D! opened service: %T", service)
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
		s.Logger.Printf("D! closing service: %T", service)
		service.Close()
		s.Logger.Printf("D! closed service: %T", service)
	}

	return s.TaskMaster.Close()
}

func (s *Server) setupIDs() error {
	clusterIDPath := filepath.Join(s.dataDir, clusterIDFilename)
	clusterID, err := s.readID(clusterIDPath)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if clusterID == "" {
		clusterID = uuid.NewV4().String()
		s.writeID(clusterIDPath, clusterID)
	}
	s.ClusterID = clusterID

	serverIDPath := filepath.Join(s.dataDir, serverIDFilename)
	serverID, err := s.readID(serverIDPath)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if serverID == "" {
		serverID = uuid.NewV4().String()
		s.writeID(serverIDPath, serverID)
	}
	s.ServerID = serverID

	return nil
}

func (s *Server) readID(file string) (string, error) {
	f, err := os.Open(file)
	if err != nil {
		return "", err
	}
	defer f.Close()
	b, err := ioutil.ReadAll(f)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(b)), nil
}

func (s *Server) writeID(file, id string) error {
	f, err := os.Create(file)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write([]byte(id))
	if err != nil {
		return err
	}
	return nil
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
