// Provides a server type for starting and configuring a Kapacitor server.
package server

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/services/collectd"
	"github.com/influxdata/influxdb/services/graphite"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/services/opentsdb"
	"github.com/influxdata/kapacitor"
	"github.com/influxdata/kapacitor/auth"
	"github.com/influxdata/kapacitor/services/alerta"
	"github.com/influxdata/kapacitor/services/deadman"
	"github.com/influxdata/kapacitor/services/hipchat"
	"github.com/influxdata/kapacitor/services/httpd"
	"github.com/influxdata/kapacitor/services/influxdb"
	"github.com/influxdata/kapacitor/services/logging"
	"github.com/influxdata/kapacitor/services/noauth"
	"github.com/influxdata/kapacitor/services/opsgenie"
	"github.com/influxdata/kapacitor/services/pagerduty"
	"github.com/influxdata/kapacitor/services/replay"
	"github.com/influxdata/kapacitor/services/reporting"
	"github.com/influxdata/kapacitor/services/sensu"
	"github.com/influxdata/kapacitor/services/slack"
	"github.com/influxdata/kapacitor/services/smtp"
	"github.com/influxdata/kapacitor/services/stats"
	"github.com/influxdata/kapacitor/services/storage"
	"github.com/influxdata/kapacitor/services/talk"
	"github.com/influxdata/kapacitor/services/task_store"
	"github.com/influxdata/kapacitor/services/telegram"
	"github.com/influxdata/kapacitor/services/udf"
	"github.com/influxdata/kapacitor/services/udp"
	"github.com/influxdata/kapacitor/services/victorops"
	"github.com/pkg/errors"
	"github.com/twinj/uuid"
)

const clusterIDFilename = "cluster.id"
const serverIDFilename = "server.id"

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
	dataDir  string
	hostname string

	config *Config

	err chan error

	TaskMaster       *kapacitor.TaskMaster
	TaskMasterLookup *kapacitor.TaskMasterLookup

	AuthService     auth.Interface
	HTTPDService    *httpd.Service
	StorageService  *storage.Service
	TaskStore       *task_store.Service
	ReplayService   *replay.Service
	InfluxDBService *influxdb.Service

	MetaClient    *kapacitor.NoopMetaClient
	QueryExecutor *Queryexecutor

	Services []Service

	BuildInfo BuildInfo
	ClusterID string
	ServerID  string

	// Profiling
	CPUProfile string
	MemProfile string

	LogService logging.Interface
	Logger     *log.Logger
}

// New returns a new instance of Server built from a config.
func New(c *Config, buildInfo BuildInfo, logService logging.Interface) (*Server, error) {
	err := c.Validate()
	if err != nil {
		return nil, fmt.Errorf("%s. To generate a valid configuration file run `kapacitord config > kapacitor.generated.conf`.", err)
	}
	l := logService.NewLogger("[srv] ", log.LstdFlags)
	s := &Server{
		config:        c,
		BuildInfo:     buildInfo,
		dataDir:       c.DataDir,
		hostname:      c.Hostname,
		err:           make(chan error),
		LogService:    logService,
		MetaClient:    &kapacitor.NoopMetaClient{},
		QueryExecutor: &Queryexecutor{},
		Logger:        l,
	}
	s.Logger.Println("I! Kapacitor hostname:", s.hostname)

	// Setup IDs
	err = s.setupIDs()
	if err != nil {
		return nil, err
	}
	// Set published vars
	kapacitor.ClusterIDVar.Set(s.ClusterID)
	kapacitor.ServerIDVar.Set(s.ServerID)
	kapacitor.HostVar.Set(s.hostname)
	kapacitor.ProductVar.Set(kapacitor.Product)
	kapacitor.VersionVar.Set(s.BuildInfo.Version)
	s.Logger.Printf("I! ClusterID: %s ServerID: %s", s.ClusterID, s.ServerID)

	// Start Task Master
	s.TaskMasterLookup = kapacitor.NewTaskMasterLookup()
	s.TaskMaster = kapacitor.NewTaskMaster(kapacitor.MainTaskMaster, logService)
	s.TaskMasterLookup.Set(s.TaskMaster)
	if err := s.TaskMaster.Open(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Server) AppendStorageService() {
	l := s.LogService.NewLogger("[storage] ", log.LstdFlags)
	srv := storage.NewService(s.config.Storage, l)

	s.StorageService = srv
	s.Services = append(s.Services, srv)
}

func (s *Server) AppendSMTPService() {
	c := s.config.SMTP
	if c.Enabled {
		l := s.LogService.NewLogger("[smtp] ", log.LstdFlags)
		srv := smtp.NewService(c, l)

		s.TaskMaster.SMTPService = srv
		s.Services = append(s.Services, srv)
	}
}

func (s *Server) AppendInfluxDBService() error {
	c := s.config.InfluxDB
	if len(c) > 0 {
		l := s.LogService.NewLogger("[influxdb] ", log.LstdFlags)
		httpPort, err := s.config.HTTP.Port()
		if err != nil {
			return errors.Wrap(err, "failed to get http port")
		}
		srv := influxdb.NewService(c, s.config.defaultInfluxDB, httpPort, s.config.Hostname, l)
		srv.PointsWriter = s.TaskMaster
		srv.LogService = s.LogService

		s.InfluxDBService = srv
		s.TaskMaster.InfluxDBService = srv
		s.Services = append(s.Services, srv)
	}
	return nil
}

func (s *Server) InitHTTPDService() {
	l := s.LogService.NewLogger("[httpd] ", log.LstdFlags)
	srv := httpd.NewService(s.config.HTTP, s.hostname, l, s.LogService)

	srv.Handler.PointsWriter = s.TaskMaster
	srv.Handler.Version = s.BuildInfo.Version

	s.HTTPDService = srv
	s.TaskMaster.HTTPDService = srv
}

func (s *Server) AppendHTTPDService() {
	s.Services = append(s.Services, s.HTTPDService)
}

func (s *Server) AppendTaskStoreService() {
	l := s.LogService.NewLogger("[task_store] ", log.LstdFlags)
	srv := task_store.NewService(s.config.Task, l)
	srv.StorageService = s.StorageService
	srv.HTTPDService = s.HTTPDService
	srv.TaskMasterLookup = s.TaskMasterLookup

	s.TaskStore = srv
	s.TaskMaster.TaskStore = srv
	s.Services = append(s.Services, srv)
}

func (s *Server) AppendReplayService() {
	l := s.LogService.NewLogger("[replay] ", log.LstdFlags)
	srv := replay.NewService(s.config.Replay, l)
	srv.StorageService = s.StorageService
	srv.TaskStore = s.TaskStore
	srv.HTTPDService = s.HTTPDService
	srv.InfluxDBService = s.InfluxDBService
	srv.TaskMaster = s.TaskMaster
	srv.TaskMasterLookup = s.TaskMasterLookup

	s.ReplayService = srv
	s.Services = append(s.Services, srv)
}

func (s *Server) AppendDeadmanService() {
	l := s.LogService.NewLogger("[deadman] ", log.LstdFlags)
	srv := deadman.NewService(s.config.Deadman, l)

	s.TaskMaster.DeadmanService = srv
	s.Services = append(s.Services, srv)
}

func (s *Server) AppendUDFService() {
	l := s.LogService.NewLogger("[udf] ", log.LstdFlags)
	srv := udf.NewService(s.config.UDF, l)

	s.TaskMaster.UDFService = srv
	s.Services = append(s.Services, srv)
}

func (s *Server) AppendAuthService() {
	l := s.LogService.NewLogger("[noauth] ", log.LstdFlags)
	srv := noauth.NewService(l)

	s.AuthService = srv
	s.HTTPDService.Handler.AuthService = srv
	s.Services = append(s.Services, srv)
}

func (s *Server) AppendOpsGenieService() {
	c := s.config.OpsGenie
	if c.Enabled {
		l := s.LogService.NewLogger("[opsgenie] ", log.LstdFlags)
		srv := opsgenie.NewService(c, l)
		s.TaskMaster.OpsGenieService = srv

		s.Services = append(s.Services, srv)
	}
}

func (s *Server) AppendVictorOpsService() {
	c := s.config.VictorOps
	if c.Enabled {
		l := s.LogService.NewLogger("[victorops] ", log.LstdFlags)
		srv := victorops.NewService(c, l)
		s.TaskMaster.VictorOpsService = srv

		s.Services = append(s.Services, srv)
	}
}

func (s *Server) AppendPagerDutyService() {
	c := s.config.PagerDuty
	if c.Enabled {
		l := s.LogService.NewLogger("[pagerduty] ", log.LstdFlags)
		srv := pagerduty.NewService(c, l)
		srv.HTTPDService = s.HTTPDService
		s.TaskMaster.PagerDutyService = srv

		s.Services = append(s.Services, srv)
	}
}

func (s *Server) AppendSensuService() {
	c := s.config.Sensu
	if c.Enabled {
		l := s.LogService.NewLogger("[sensu] ", log.LstdFlags)
		srv := sensu.NewService(c, l)
		s.TaskMaster.SensuService = srv

		s.Services = append(s.Services, srv)
	}
}

func (s *Server) AppendSlackService() {
	c := s.config.Slack
	if c.Enabled {
		l := s.LogService.NewLogger("[slack] ", log.LstdFlags)
		srv := slack.NewService(c, l)
		s.TaskMaster.SlackService = srv

		s.Services = append(s.Services, srv)
	}
}

func (s *Server) AppendTelegramService() {
	c := s.config.Telegram
	if c.Enabled {
		l := s.LogService.NewLogger("[telegram] ", log.LstdFlags)
		srv := telegram.NewService(c, l)
		s.TaskMaster.TelegramService = srv

		s.Services = append(s.Services, srv)
	}
}

func (s *Server) AppendHipChatService() {
	c := s.config.HipChat
	if c.Enabled {
		l := s.LogService.NewLogger("[hipchat] ", log.LstdFlags)
		srv := hipchat.NewService(c, l)
		s.TaskMaster.HipChatService = srv

		s.Services = append(s.Services, srv)
	}
}

func (s *Server) AppendAlertaService() {
	c := s.config.Alerta
	if c.Enabled {
		l := s.LogService.NewLogger("[alerta] ", log.LstdFlags)
		srv := alerta.NewService(c, l)
		s.TaskMaster.AlertaService = srv

		s.Services = append(s.Services, srv)
	}
}

func (s *Server) AppendTalkService() {
	c := s.config.Talk
	if c.Enabled {
		l := s.LogService.NewLogger("[talk] ", log.LstdFlags)
		srv := talk.NewService(c, l)
		s.TaskMaster.TalkService = srv

		s.Services = append(s.Services, srv)
	}
}

func (s *Server) AppendCollectdService() {
	c := s.config.Collectd
	if !c.Enabled {
		return
	}
	srv := collectd.NewService(c)
	w := s.LogService.NewStaticLevelWriter(logging.INFO)
	srv.SetLogOutput(w)

	srv.MetaClient = s.MetaClient
	srv.PointsWriter = s.TaskMaster
	s.Services = append(s.Services, srv)
}

func (s *Server) AppendOpenTSDBService() error {
	c := s.config.OpenTSDB
	if !c.Enabled {
		return nil
	}
	srv, err := opentsdb.NewService(c)
	if err != nil {
		return err
	}
	w := s.LogService.NewStaticLevelWriter(logging.INFO)
	srv.SetLogOutput(w)

	srv.PointsWriter = s.TaskMaster
	srv.MetaClient = s.MetaClient
	s.Services = append(s.Services, srv)
	return nil
}

func (s *Server) AppendGraphiteServices() error {
	for _, c := range s.config.Graphites {
		if !c.Enabled {
			continue
		}
		srv, err := graphite.NewService(c)
		if err != nil {
			return errors.Wrap(err, "creating new graphite service")
		}
		w := s.LogService.NewStaticLevelWriter(logging.INFO)
		srv.SetLogOutput(w)

		srv.PointsWriter = s.TaskMaster
		srv.MetaClient = s.MetaClient
		s.Services = append(s.Services, srv)
	}
	return nil
}

func (s *Server) AppendUDPServices() {
	for _, c := range s.config.UDPs {
		if !c.Enabled {
			continue
		}
		l := s.LogService.NewLogger("[udp] ", log.LstdFlags)
		srv := udp.NewService(c, l)
		srv.PointsWriter = s.TaskMaster
		s.Services = append(s.Services, srv)
	}
}

func (s *Server) AppendStatsService() {
	c := s.config.Stats
	if c.Enabled {
		l := s.LogService.NewLogger("[stats] ", log.LstdFlags)
		srv := stats.NewService(c, l)
		srv.TaskMaster = s.TaskMaster

		s.TaskMaster.TimingService = srv
		s.Services = append(s.Services, srv)
	}
}

func (s *Server) AppendReportingService() {
	c := s.config.Reporting
	if c.Enabled {
		l := s.LogService.NewLogger("[reporting] ", log.LstdFlags)
		srv := reporting.NewService(c, l)

		s.Services = append(s.Services, srv)
	}
}

// Err returns an error channel that multiplexes all out of band errors received from all services.
func (s *Server) Err() <-chan error { return s.err }

type ServiceProvider interface {
	AppendUDFService()
	AppendDeadmanService()
	AppendSMTPService()
	AppendAuthService()
	InitHTTPDService()
	AppendInfluxDBService() error
	AppendStorageService()
	AppendTaskStoreService()
	AppendReplayService()
	AppendOpsGenieService()
	AppendVictorOpsService()
	AppendPagerDutyService()
	AppendTelegramService()
	AppendHipChatService()
	AppendAlertaService()
	AppendSlackService()
	AppendSensuService()
	AppendTalkService()
	AppendCollectdService()
	AppendUDPServices()
	AppendOpenTSDBService() error
	AppendGraphiteServices() error
	AppendStatsService()
	AppendReportingService()
	AppendHTTPDService()
}

// Append all services
func DoAppendServices(s ServiceProvider) error {
	// Append Kapacitor services.
	s.AppendUDFService()
	s.AppendDeadmanService()
	s.AppendSMTPService()
	s.InitHTTPDService()
	if err := s.AppendInfluxDBService(); err != nil {
		return errors.Wrap(err, "influxdb service")
	}
	s.AppendStorageService()
	s.AppendTaskStoreService()
	s.AppendReplayService()
	s.AppendAuthService()

	// Append Alert integration services
	s.AppendOpsGenieService()
	s.AppendVictorOpsService()
	s.AppendPagerDutyService()
	s.AppendTelegramService()
	s.AppendHipChatService()
	s.AppendAlertaService()
	s.AppendSlackService()
	s.AppendSensuService()
	s.AppendTalkService()

	// Append InfluxDB input services
	s.AppendCollectdService()
	s.AppendUDPServices()
	if err := s.AppendOpenTSDBService(); err != nil {
		return errors.Wrap(err, "opentsdb service")
	}
	if err := s.AppendGraphiteServices(); err != nil {
		return errors.Wrap(err, "graphite service")
	}

	// Append StatsService and ReportingService after other services so all stats are ready
	// to be reported
	s.AppendStatsService()
	s.AppendReportingService()

	// Append HTTPD Service last so that the API is not listening till everything else succeeded.
	s.AppendHTTPDService()

	return nil
}

// Open opens all the services.
func (s *Server) Open() error {
	if err := DoAppendServices(s); err != nil {
		return err
	}
	if err := s.StartServices(); err != nil {
		s.Close()
		return err
	}

	go s.WatchServices()

	return nil
}

func (s *Server) StartServices() error {
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
}

// Watch if something dies
func (s *Server) WatchServices() {
	var err error
	select {
	case err = <-s.HTTPDService.Err():
	}
	s.err <- err
}

// Close shuts down the meta and data stores and all services.
func (s *Server) Close() error {
	s.stopProfile()

	// First stop all tasks.
	s.TaskMaster.StopTasks()

	// Close services now that all tasks are stopped.
	for _, service := range s.Services {
		s.Logger.Printf("D! closing service: %T", service)
		err := service.Close()
		if err != nil {
			s.Logger.Printf("E! error closing service %T: %v", service, err)
		}
		s.Logger.Printf("D! closed service: %T", service)
	}

	// Finally close the task master
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

type Queryexecutor struct{}

func (qe *Queryexecutor) Authorize(u *meta.UserInfo, q *influxql.Query, db string) error {
	return nil
}
func (qe *Queryexecutor) ExecuteQuery(q *influxql.Query, db string, chunkSize int) (<-chan *influxql.Result, error) {
	return nil, errors.New("cannot execute queries against Kapacitor")
}
