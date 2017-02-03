// Provides a server type for starting and configuring a Kapacitor server.
package server

import (
	"fmt"
	"io/ioutil"
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
	"github.com/influxdata/kapacitor/command"
	iclient "github.com/influxdata/kapacitor/influxdb"
	"github.com/influxdata/kapacitor/services/alert"
	"github.com/influxdata/kapacitor/services/alerta"
	"github.com/influxdata/kapacitor/services/config"
	"github.com/influxdata/kapacitor/services/deadman"
	"github.com/influxdata/kapacitor/services/hipchat"
	"github.com/influxdata/kapacitor/services/httpd"
	"github.com/influxdata/kapacitor/services/influxdb"
	"github.com/influxdata/kapacitor/services/k8s"
	"github.com/influxdata/kapacitor/services/logging"
	"github.com/influxdata/kapacitor/services/noauth"
	"github.com/influxdata/kapacitor/services/opsgenie"
	"github.com/influxdata/kapacitor/services/pagerduty"
	"github.com/influxdata/kapacitor/services/replay"
	"github.com/influxdata/kapacitor/services/reporting"
	"github.com/influxdata/kapacitor/services/sensu"
	"github.com/influxdata/kapacitor/services/servicetest"
	"github.com/influxdata/kapacitor/services/slack"
	"github.com/influxdata/kapacitor/services/smtp"
	"github.com/influxdata/kapacitor/services/snmptrap"
	"github.com/influxdata/kapacitor/services/stats"
	"github.com/influxdata/kapacitor/services/storage"
	"github.com/influxdata/kapacitor/services/talk"
	"github.com/influxdata/kapacitor/services/task_store"
	"github.com/influxdata/kapacitor/services/telegram"
	"github.com/influxdata/kapacitor/services/udf"
	"github.com/influxdata/kapacitor/services/udp"
	"github.com/influxdata/kapacitor/services/victorops"
	"github.com/influxdata/kapacitor/vars"
	"github.com/pkg/errors"
	"github.com/twinj/uuid"
	"github.com/uber-go/zap"
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

	Commander command.Commander

	TaskMaster       *kapacitor.TaskMaster
	TaskMasterLookup *kapacitor.TaskMasterLookup

	AuthService           auth.Interface
	HTTPDService          *httpd.Service
	StorageService        *storage.Service
	AlertService          *alert.Service
	TaskStore             *task_store.Service
	ReplayService         *replay.Service
	InfluxDBService       *influxdb.Service
	ConfigOverrideService *config.Service
	TesterService         *servicetest.Service
	StatsService          *stats.Service

	MetaClient    *kapacitor.NoopMetaClient
	QueryExecutor *Queryexecutor

	// List of services in startup order
	Services []service
	// Map of service name to index in Services list
	ServicesByName map[string]int

	// Map of services capable of receiving dynamic configuration updates.
	DynamicServices map[string]Updater
	// Channel of incoming configuration updates.
	configUpdates chan config.ConfigUpdate

	BuildInfo BuildInfo
	ClusterID string
	ServerID  string

	// Profiling
	CPUProfile string
	MemProfile string

	LogService logging.Interface
	Logger     zap.Logger
}

// New returns a new instance of Server built from a config.
func New(c *Config, buildInfo BuildInfo, logService logging.Interface) (*Server, error) {
	err := c.Validate()
	if err != nil {
		return nil, fmt.Errorf("%s. To generate a valid configuration file run `kapacitord config > kapacitor.generated.conf`.", err)
	}
	l := logService.Root().With(zap.Bool("server", true))
	s := &Server{
		config:          c,
		BuildInfo:       buildInfo,
		dataDir:         c.DataDir,
		hostname:        c.Hostname,
		err:             make(chan error),
		configUpdates:   make(chan config.ConfigUpdate, 100),
		LogService:      logService,
		MetaClient:      &kapacitor.NoopMetaClient{},
		QueryExecutor:   &Queryexecutor{},
		Logger:          l,
		ServicesByName:  make(map[string]int),
		DynamicServices: make(map[string]Updater),
		Commander:       c.Commander,
	}
	s.Logger.Info("Kapacitor hostname", zap.String("hostname", s.hostname))

	// Setup IDs
	err = s.setupIDs()
	if err != nil {
		return nil, err
	}

	// Set published vars
	vars.ClusterIDVar.Set(s.ClusterID)
	vars.ServerIDVar.Set(s.ServerID)
	vars.HostVar.Set(s.hostname)
	vars.ProductVar.Set(vars.Product)
	vars.VersionVar.Set(s.BuildInfo.Version)
	s.Logger.Info("Kapacitor ids", zap.String("cluster-id", s.ClusterID), zap.String("server-id", s.ServerID))

	// Start Task Master
	s.TaskMasterLookup = kapacitor.NewTaskMasterLookup()
	s.TaskMaster = kapacitor.NewTaskMaster(kapacitor.MainTaskMaster, logService)
	s.TaskMaster.DefaultRetentionPolicy = c.DefaultRetentionPolicy
	s.TaskMaster.Commander = s.Commander
	s.TaskMasterLookup.Set(s.TaskMaster)
	if err := s.TaskMaster.Open(); err != nil {
		return nil, err
	}

	// Append Kapacitor services.
	s.initHTTPDService()
	s.appendStorageService()
	s.appendAuthService()
	s.appendConfigOverrideService()
	s.appendTesterService()
	s.appendAlertService()

	// Append all dynamic services after the config override and tester services.
	s.appendUDFService()
	s.appendDeadmanService()

	if err := s.appendInfluxDBService(); err != nil {
		return nil, errors.Wrap(err, "influxdb service")
	}
	// Append these after InfluxDB because they depend on it
	s.appendTaskStoreService()
	s.appendReplayService()

	// Append Alert integration services
	s.appendAlertaService()
	s.appendHipChatService()
	s.appendOpsGenieService()
	s.appendPagerDutyService()
	s.appendSMTPService()
	s.appendTelegramService()
	s.appendHipChatService()
	s.appendAlertaService()
	s.appendSlackService()
	s.appendSNMPTrapService()
	s.appendSensuService()
	s.appendSlackService()
	s.appendTalkService()
	s.appendTelegramService()
	s.appendVictorOpsService()

	// Append third-party integrations
	if err := s.appendK8sService(); err != nil {
		return nil, errors.Wrap(err, "kubernetes service")
	}

	// Append extra input services
	s.appendCollectdService()
	s.appendUDPServices()
	if err := s.appendOpenTSDBService(); err != nil {
		return nil, errors.Wrap(err, "opentsdb service")
	}
	if err := s.appendGraphiteServices(); err != nil {
		return nil, errors.Wrap(err, "graphite service")
	}

	// Append StatsService and ReportingService after other services so all stats are ready
	// to be reported
	s.appendStatsService()
	s.appendReportingService()

	return s, nil
}

func (s *Server) AppendService(name string, srv Service) {
	i := len(s.Services)
	s.Services = append(s.Services, service{Name: name, Service: srv})
	s.ServicesByName[name] = i
}

type dynamicService interface {
	Service
	Updater
	servicetest.Tester
}

func (s *Server) SetDynamicService(name string, srv dynamicService) {
	s.DynamicServices[name] = srv
	_ = s.TesterService.AddTester(name, srv)
}

func (s *Server) appendStorageService() {
	l := s.LogService.Root().With(zap.String("service", "storage"))
	srv := storage.NewService(s.config.Storage, l)

	s.StorageService = srv
	s.AppendService("storage", srv)
}

func (s *Server) appendConfigOverrideService() {
	l := s.LogService.Root().With(zap.String("service", "config-override"))
	srv := config.NewService(s.config.ConfigOverride, s.config, l, s.configUpdates)
	srv.HTTPDService = s.HTTPDService
	srv.StorageService = s.StorageService

	s.ConfigOverrideService = srv
	s.AppendService("config", srv)
}

func (s *Server) appendAlertService() {
	l := s.LogService.Root().With(zap.String("service", "alert"))
	srv := alert.NewService(s.config.Alert, l)

	srv.Commander = s.Commander
	srv.HTTPDService = s.HTTPDService
	srv.StorageService = s.StorageService

	s.AlertService = srv
	s.TaskMaster.AlertService = srv
	s.AppendService("alert", srv)
}

func (s *Server) appendTesterService() {
	l := s.LogService.Root().With(zap.String("service", "service-tests"))
	srv := servicetest.NewService(servicetest.NewConfig(), l)
	srv.HTTPDService = s.HTTPDService

	s.TesterService = srv
	s.AppendService("tests", srv)
}

func (s *Server) appendSMTPService() {
	c := s.config.SMTP
	l := s.LogService.Root().With(zap.String("service", "smtp"))
	srv := smtp.NewService(c, l)

	s.TaskMaster.SMTPService = srv
	s.AlertService.SMTPService = srv

	s.SetDynamicService("smtp", srv)
	s.AppendService("smtp", srv)
}

func (s *Server) appendInfluxDBService() error {
	c := s.config.InfluxDB
	l := s.LogService.Root().With(zap.String("service", "influxdb"))
	httpPort, err := s.config.HTTP.Port()
	if err != nil {
		return errors.Wrap(err, "failed to get http port")
	}
	srv, err := influxdb.NewService(c, httpPort, s.config.Hostname, s.config.HTTP.AuthEnabled, l)
	if err != nil {
		return err
	}
	srv.HTTPDService = s.HTTPDService
	srv.PointsWriter = s.TaskMaster
	srv.AuthService = s.AuthService
	srv.ClientCreator = iclient.ClientCreator{}

	s.InfluxDBService = srv
	s.TaskMaster.InfluxDBService = srv
	s.SetDynamicService("influxdb", srv)
	s.AppendService("influxdb", srv)
	return nil
}

func (s *Server) initHTTPDService() {
	l := s.LogService.Root().With(zap.String("service", "httpd"))
	srv := httpd.NewService(s.config.HTTP, s.hostname, l, s.LogService)

	srv.Handler.PointsWriter = s.TaskMaster
	srv.Handler.Version = s.BuildInfo.Version

	s.HTTPDService = srv
	s.TaskMaster.HTTPDService = srv
}

func (s *Server) appendTaskStoreService() {
	l := s.LogService.Root().With(zap.String("service", "task_store"))
	srv := task_store.NewService(s.config.Task, l)
	srv.StorageService = s.StorageService
	srv.HTTPDService = s.HTTPDService
	srv.TaskMasterLookup = s.TaskMasterLookup

	s.TaskStore = srv
	s.TaskMaster.TaskStore = srv
	s.AppendService("task_store", srv)
}

func (s *Server) appendReplayService() {
	l := s.LogService.Root().With(zap.String("service", "replay"))
	srv := replay.NewService(s.config.Replay, l)
	srv.StorageService = s.StorageService
	srv.TaskStore = s.TaskStore
	srv.HTTPDService = s.HTTPDService
	srv.InfluxDBService = s.InfluxDBService
	srv.TaskMaster = s.TaskMaster
	srv.TaskMasterLookup = s.TaskMasterLookup

	s.ReplayService = srv
	s.AppendService("replay", srv)
}

func (s *Server) appendK8sService() error {
	c := s.config.Kubernetes
	l := s.LogService.Root().With(zap.String("service", "kubernetes"))
	srv, err := k8s.NewService(c, l)
	if err != nil {
		return err
	}

	s.TaskMaster.K8sService = srv
	s.SetDynamicService("kubernetes", srv)
	s.AppendService("kubernetes", srv)
	return nil
}

func (s *Server) appendDeadmanService() {
	l := s.LogService.Root().With(zap.String("service", "deadman"))
	srv := deadman.NewService(s.config.Deadman, l)

	s.TaskMaster.DeadmanService = srv
	s.AppendService("deadman", srv)
}

func (s *Server) appendUDFService() {
	l := s.LogService.Root().With(zap.String("service", "udf"))
	srv := udf.NewService(s.config.UDF, l)

	s.TaskMaster.UDFService = srv
	s.AppendService("udf", srv)
}

func (s *Server) appendAuthService() {
	l := s.LogService.Root().With(zap.String("service", "noauth"))
	srv := noauth.NewService(l)

	s.AuthService = srv
	s.HTTPDService.Handler.AuthService = srv
	s.AppendService("auth", srv)
}

func (s *Server) appendOpsGenieService() {
	c := s.config.OpsGenie
	l := s.LogService.Root().With(zap.String("service", "opsgenie"))
	srv := opsgenie.NewService(c, l)

	s.TaskMaster.OpsGenieService = srv
	s.AlertService.OpsGenieService = srv

	s.SetDynamicService("opsgenie", srv)
	s.AppendService("opsgenie", srv)
}

func (s *Server) appendVictorOpsService() {
	c := s.config.VictorOps
	l := s.LogService.Root().With(zap.String("service", "victorops"))
	srv := victorops.NewService(c, l)

	s.TaskMaster.VictorOpsService = srv
	s.AlertService.VictorOpsService = srv

	s.SetDynamicService("victorops", srv)
	s.AppendService("victorops", srv)
}

func (s *Server) appendPagerDutyService() {
	c := s.config.PagerDuty
	l := s.LogService.Root().With(zap.String("service", "pagerduty"))
	srv := pagerduty.NewService(c, l)
	srv.HTTPDService = s.HTTPDService

	s.TaskMaster.PagerDutyService = srv
	s.AlertService.PagerDutyService = srv

	s.SetDynamicService("pagerduty", srv)
	s.AppendService("pagerduty", srv)
}

func (s *Server) appendSensuService() {
	c := s.config.Sensu
	l := s.LogService.Root().With(zap.String("service", "sensu"))
	srv := sensu.NewService(c, l)

	s.TaskMaster.SensuService = srv
	s.AlertService.SensuService = srv

	s.SetDynamicService("sensu", srv)
	s.AppendService("sensu", srv)
}

func (s *Server) appendSlackService() {
	c := s.config.Slack
	l := s.LogService.Root().With(zap.String("service", "slack"))
	srv := slack.NewService(c, l)

	s.TaskMaster.SlackService = srv
	s.AlertService.SlackService = srv

	s.SetDynamicService("slack", srv)
	s.AppendService("slack", srv)
}

func (s *Server) appendSNMPTrapService() {
	c := s.config.SNMPTrap
	l := s.LogService.Root().With(zap.String("service", "snmptrap"))
	srv := snmptrap.NewService(c, l)

	s.TaskMaster.SNMPTrapService = srv
	s.AlertService.SNMPTrapService = srv

	s.SetDynamicService("snmptrap", srv)
	s.AppendService("snmptrap", srv)
}

func (s *Server) appendTelegramService() {
	c := s.config.Telegram
	l := s.LogService.Root().With(zap.String("service", "telegram"))
	srv := telegram.NewService(c, l)

	s.TaskMaster.TelegramService = srv
	s.AlertService.TelegramService = srv

	s.SetDynamicService("telegram", srv)
	s.AppendService("telegram", srv)
}

func (s *Server) appendHipChatService() {
	c := s.config.HipChat
	l := s.LogService.Root().With(zap.String("service", "hipchat"))
	srv := hipchat.NewService(c, l)

	s.TaskMaster.HipChatService = srv
	s.AlertService.HipChatService = srv

	s.SetDynamicService("hipchat", srv)
	s.AppendService("hipchat", srv)
}

func (s *Server) appendAlertaService() {
	c := s.config.Alerta
	l := s.LogService.Root().With(zap.String("service", "alerta"))
	srv := alerta.NewService(c, l)

	s.TaskMaster.AlertaService = srv
	s.AlertService.AlertaService = srv

	s.SetDynamicService("alerta", srv)
	s.AppendService("alerta", srv)
}

func (s *Server) appendTalkService() {
	c := s.config.Talk
	l := s.LogService.Root().With(zap.String("service", "talk"))
	srv := talk.NewService(c, l)

	s.TaskMaster.TalkService = srv
	s.AlertService.TalkService = srv

	s.SetDynamicService("talk", srv)
	s.AppendService("talk", srv)
}

func (s *Server) appendCollectdService() {
	c := s.config.Collectd
	if !c.Enabled {
		return
	}
	srv := collectd.NewService(c)
	//srv.WithLogger(s.LogService.Root())

	srv.MetaClient = s.MetaClient
	srv.PointsWriter = s.TaskMaster
	s.AppendService("collectd", srv)
}

func (s *Server) appendOpenTSDBService() error {
	c := s.config.OpenTSDB
	if !c.Enabled {
		return nil
	}
	srv, err := opentsdb.NewService(c)
	if err != nil {
		return err
	}
	//srv.WithLogger(s.LogService.Root())

	srv.PointsWriter = s.TaskMaster
	srv.MetaClient = s.MetaClient
	s.AppendService("opentsdb", srv)
	return nil
}

func (s *Server) appendGraphiteServices() error {
	for i, c := range s.config.Graphites {
		if !c.Enabled {
			continue
		}
		srv, err := graphite.NewService(c)
		if err != nil {
			return errors.Wrap(err, "creating new graphite service")
		}
		//srv.WithLogger(s.LogService.Root())

		srv.PointsWriter = s.TaskMaster
		srv.MetaClient = s.MetaClient
		s.AppendService(fmt.Sprintf("graphite%d", i), srv)
	}
	return nil
}

func (s *Server) appendUDPServices() {
	for i, c := range s.config.UDPs {
		if !c.Enabled {
			continue
		}
		l := s.LogService.Root().With(zap.String("service", "udp"), zap.String("addr", c.BindAddress))
		srv := udp.NewService(c, l)
		srv.PointsWriter = s.TaskMaster
		s.AppendService(fmt.Sprintf("udp%d", i), srv)
	}
}

func (s *Server) appendStatsService() {
	c := s.config.Stats
	if c.Enabled {
		l := s.LogService.Root().With(zap.String("service", "stats"))
		srv := stats.NewService(c, l)
		srv.TaskMaster = s.TaskMaster

		s.StatsService = srv
		s.TaskMaster.TimingService = srv
		s.AppendService("stats", srv)
	}
}

func (s *Server) appendReportingService() {
	c := s.config.Reporting
	if c.Enabled {
		l := s.LogService.Root().With(zap.String("service", "reporting"))
		srv := reporting.NewService(c, l)

		s.AppendService("reporting", srv)
	}
}

// Err returns an error channel that multiplexes all out of band errors received from all services.
func (s *Server) Err() <-chan error { return s.err }

// Open opens all the services.
func (s *Server) Open() error {

	// Start profiling, if set.
	if err := s.startProfile(s.CPUProfile, s.MemProfile); err != nil {
		return err
	}

	if err := s.startServices(); err != nil {
		s.Close()
		return err
	}
	// Open HTTPD Service last so that the API is not listening till everything else succeeded.
	if err := s.HTTPDService.Open(); err != nil {
		return err
	}

	go s.watchServices()
	go s.watchConfigUpdates()

	return nil
}

func (s *Server) startServices() error {
	for _, service := range s.Services {
		s.Logger.Debug("opening service", zap.String("service", service.Name))
		if err := service.Open(); err != nil {
			return fmt.Errorf("open service %s: %s", service.Name, err)
		}
		s.Logger.Debug("opened service", zap.String("service", service.Name))

		// Apply config overrides after the config override service has been opened and before any dynamic services.
		if service.Service == s.ConfigOverrideService && !s.config.SkipConfigOverrides {
			// Apply initial config updates
			s.Logger.Debug("applying configuration overrides")
			configs, err := s.ConfigOverrideService.Config()
			if err != nil {
				return errors.Wrap(err, "failed to apply config overrides")
			}
			for service, config := range configs {
				if srv, ok := s.DynamicServices[service]; !ok {
					return fmt.Errorf("found configuration override for unknown service %q", service)
				} else {
					s.Logger.Debug("applying configuration overrides for service", zap.String("service", service))
					if err := srv.Update(config); err != nil {
						return errors.Wrapf(err, "failed to update configuration for service %s", service)
					}
				}
			}
		}
	}
	return nil
}

// Watch if something dies
func (s *Server) watchServices() {
	var err error
	select {
	case err = <-s.HTTPDService.Err():
	}
	s.err <- err
}

func (s *Server) watchConfigUpdates() {
	for cu := range s.configUpdates {
		if srv, ok := s.DynamicServices[cu.Name]; !ok {
			cu.ErrC <- fmt.Errorf("received configuration update for unknown dynamic service %s", cu.Name)
		} else {
			cu.ErrC <- srv.Update(cu.NewConfig)
		}
	}
}

// Close shuts down the meta and data stores and all services.
func (s *Server) Close() error {
	s.stopProfile()

	// Close all services that write points first.
	if err := s.HTTPDService.Close(); err != nil {
		s.Logger.Error("error closing httpd service", zap.Error(err))
	}
	if s.StatsService != nil {
		if err := s.StatsService.Close(); err != nil {
			s.Logger.Error("error closing stats service", zap.Error(err))
		}
	}

	// Drain the in-flight writes and stop all tasks.
	s.TaskMaster.Drain()
	s.TaskMaster.StopTasks()

	// Close services now that all tasks are stopped.
	for _, service := range s.Services {
		s.Logger.Debug("closing service", zap.String("service", service.Name))
		err := service.Close()
		if err != nil {
			s.Logger.Error(fmt.Sprintf("error closing service: %v", err), zap.String("service", service.Name))
		}
		s.Logger.Debug("closed service", zap.String("service", service.Name))
	}

	s.Logger.Debug("closing task master")
	// Finally close the task master
	if err := s.TaskMaster.Close(); err != nil {
		return err
	}
	s.Logger.Debug("closed task master")
	return nil
}

func (s *Server) setupIDs() error {
	// Create the data dir if not exists
	if f, err := os.Stat(s.dataDir); err != nil && os.IsNotExist(err) {
		if err := os.Mkdir(s.dataDir, 0755); err != nil {
			return errors.Wrapf(err, "data_dir %s does not exist, failed to create it", s.dataDir)
		}
	} else if !f.IsDir() {
		return fmt.Errorf("path data_dir %s exists and is not a directory", s.dataDir)
	}
	clusterIDPath := filepath.Join(s.dataDir, clusterIDFilename)
	clusterID, err := s.readID(clusterIDPath)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if clusterID == "" {
		clusterID = uuid.NewV4().String()
		if err := s.writeID(clusterIDPath, clusterID); err != nil {
			return errors.Wrap(err, "failed to save cluster ID")
		}
	}
	s.ClusterID = clusterID

	serverIDPath := filepath.Join(s.dataDir, serverIDFilename)
	serverID, err := s.readID(serverIDPath)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if serverID == "" {
		serverID = uuid.NewV4().String()
		if err := s.writeID(serverIDPath, serverID); err != nil {
			return errors.Wrap(err, "failed to save server ID")
		}
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

type service struct {
	Service
	Name string
}

// Updater represents a service that can have its configuration updated while running.
type Updater interface {
	Update(c []interface{}) error
}

// prof stores the file locations of active profiles.
var prof struct {
	cpu *os.File
	mem *os.File
}

// StartProfile initializes the cpu and memory profile, if specified.
func (s *Server) startProfile(cpuprofile, memprofile string) error {
	if cpuprofile != "" {
		f, err := os.Create(cpuprofile)
		if err != nil {
			return fmt.Errorf("E! cpuprofile: %v", err)
		}
		s.Logger.Info("writing CPU profile", zap.String("file", cpuprofile))
		prof.cpu = f
		if err := pprof.StartCPUProfile(prof.cpu); err != nil {
			return fmt.Errorf("#! start cpu profile: %v", err)
		}
	}

	if memprofile != "" {
		f, err := os.Create(memprofile)
		if err != nil {
			return fmt.Errorf("E! memprofile: %v", err)
		}
		s.Logger.Info("writing mem profile", zap.String("file", memprofile))
		prof.mem = f
		runtime.MemProfileRate = 4096
	}
	return nil
}

// StopProfile closes the cpu and memory profiles if they are running.
func (s *Server) stopProfile() {
	if prof.cpu != nil {
		pprof.StopCPUProfile()
		prof.cpu.Close()
		s.Logger.Info("CPU profile stopped")
	}
	if prof.mem != nil {
		if err := pprof.Lookup("heap").WriteTo(prof.mem, 0); err != nil {
			s.Logger.Error("failed to write mem profile", zap.Error(err))
		}
		prof.mem.Close()
		s.Logger.Info("mem profile stopped")
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
