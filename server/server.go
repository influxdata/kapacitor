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
	"sync"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/services/collectd"
	"github.com/influxdata/influxdb/services/graphite"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/services/opentsdb"
	"github.com/influxdata/kapacitor"
	"github.com/influxdata/kapacitor/auth"
	"github.com/influxdata/kapacitor/command"
	iclient "github.com/influxdata/kapacitor/influxdb"
	"github.com/influxdata/kapacitor/server/vars"
	"github.com/influxdata/kapacitor/services/alert"
	"github.com/influxdata/kapacitor/services/alerta"
	"github.com/influxdata/kapacitor/services/azure"
	"github.com/influxdata/kapacitor/services/config"
	"github.com/influxdata/kapacitor/services/consul"
	"github.com/influxdata/kapacitor/services/deadman"
	"github.com/influxdata/kapacitor/services/dns"
	"github.com/influxdata/kapacitor/services/ec2"
	"github.com/influxdata/kapacitor/services/file_discovery"
	"github.com/influxdata/kapacitor/services/gce"
	"github.com/influxdata/kapacitor/services/hipchat"
	"github.com/influxdata/kapacitor/services/httpd"
	"github.com/influxdata/kapacitor/services/httppost"
	"github.com/influxdata/kapacitor/services/influxdb"
	"github.com/influxdata/kapacitor/services/k8s"
	"github.com/influxdata/kapacitor/services/logging"
	"github.com/influxdata/kapacitor/services/marathon"
	"github.com/influxdata/kapacitor/services/nerve"
	"github.com/influxdata/kapacitor/services/noauth"
	"github.com/influxdata/kapacitor/services/opsgenie"
	"github.com/influxdata/kapacitor/services/pagerduty"
	"github.com/influxdata/kapacitor/services/pushover"
	"github.com/influxdata/kapacitor/services/replay"
	"github.com/influxdata/kapacitor/services/reporting"
	"github.com/influxdata/kapacitor/services/scraper"
	"github.com/influxdata/kapacitor/services/sensu"
	"github.com/influxdata/kapacitor/services/serverset"
	"github.com/influxdata/kapacitor/services/servicetest"
	"github.com/influxdata/kapacitor/services/slack"
	"github.com/influxdata/kapacitor/services/smtp"
	"github.com/influxdata/kapacitor/services/snmptrap"
	"github.com/influxdata/kapacitor/services/static_discovery"
	"github.com/influxdata/kapacitor/services/stats"
	"github.com/influxdata/kapacitor/services/storage"
	"github.com/influxdata/kapacitor/services/talk"
	"github.com/influxdata/kapacitor/services/task_store"
	"github.com/influxdata/kapacitor/services/telegram"
	"github.com/influxdata/kapacitor/services/triton"
	"github.com/influxdata/kapacitor/services/udf"
	"github.com/influxdata/kapacitor/services/udp"
	"github.com/influxdata/kapacitor/services/victorops"
	"github.com/influxdata/kapacitor/uuid"
	"github.com/pkg/errors"
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

	ScraperService *scraper.Service

	MetaClient    *kapacitor.NoopMetaClient
	QueryExecutor *Queryexecutor

	// List of services in startup order
	Services []Service
	// Map of service name to index in Services list
	ServicesByName map[string]int

	// Map of services capable of receiving dynamic configuration updates.
	DynamicServices map[string]Updater
	// Channel of incoming configuration updates.
	configUpdates chan config.ConfigUpdate

	BuildInfo   BuildInfo
	clusterIDMu sync.Mutex
	ClusterID   uuid.UUID
	ServerID    uuid.UUID

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
	s.Logger.Println("I! Kapacitor hostname:", s.hostname)

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
	s.Logger.Printf("I! ClusterID: %s ServerID: %s", s.ClusterID, s.ServerID)

	// Start Task Master
	s.TaskMasterLookup = kapacitor.NewTaskMasterLookup()
	s.TaskMaster = kapacitor.NewTaskMaster(kapacitor.MainTaskMaster, vars.Info, logService)
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

	// Init alert service
	s.initAlertService()

	// Append all dynamic services after the config override and tester services.
	s.appendUDFService()
	s.appendDeadmanService()

	if err := s.appendInfluxDBService(); err != nil {
		return nil, errors.Wrap(err, "influxdb service")
	}

	// Append Alert integration services
	s.appendAlertaService()
	s.appendHipChatService()
	s.appendOpsGenieService()
	s.appendPagerDutyService()
	s.appendPushoverService()
	s.appendHTTPPostService()
	s.appendSMTPService()
	s.appendTelegramService()
	if err := s.appendSlackService(); err != nil {
		return nil, errors.Wrap(err, "slack service")
	}
	s.appendSNMPTrapService()
	s.appendSensuService()
	s.appendTalkService()
	s.appendVictorOpsService()

	// Append alert service
	s.appendAlertService()

	// Append these after InfluxDB because they depend on it
	s.appendTaskStoreService()
	s.appendReplayService()

	// Append third-party integrations
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

	// Append Scraper and discovery services
	s.appendScraperService()

	if err := s.appendK8sService(); err != nil {
		return nil, errors.Wrap(err, "kubernetes service")
	}

	s.appendAzureService()
	s.appendConsulService()
	s.appendDNSService()
	s.appendEC2Service()
	s.appendFileService()
	s.appendGCEService()
	s.appendMarathonService()
	s.appendNerveService()
	s.appendServersetService()
	s.appendStaticService()
	s.appendTritonService()

	// Append HTTPD Service last so that the API is not listening till everything else succeeded.
	s.appendHTTPDService()

	return s, nil
}

func (s *Server) AppendService(name string, srv Service) {
	if _, ok := s.ServicesByName[name]; ok {
		// Should be unreachable code
		panic("cannot append service twice")
	}
	i := len(s.Services)
	s.Services = append(s.Services, srv)
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
	l := s.LogService.NewLogger("[storage] ", log.LstdFlags)
	srv := storage.NewService(s.config.Storage, l)

	srv.HTTPDService = s.HTTPDService

	s.StorageService = srv
	s.AppendService("storage", srv)
}

func (s *Server) appendConfigOverrideService() {
	l := s.LogService.NewLogger("[config-override] ", log.LstdFlags)
	srv := config.NewService(s.config.ConfigOverride, s.config, l, s.configUpdates)
	srv.HTTPDService = s.HTTPDService
	srv.StorageService = s.StorageService

	s.ConfigOverrideService = srv
	s.AppendService("config", srv)
}

func (s *Server) initAlertService() {
	l := s.LogService.NewLogger("[alert] ", log.LstdFlags)
	srv := alert.NewService(l)

	srv.Commander = s.Commander
	srv.HTTPDService = s.HTTPDService
	srv.StorageService = s.StorageService

	s.AlertService = srv
	s.TaskMaster.AlertService = srv
}

func (s *Server) appendAlertService() {
	s.AppendService("alert", s.AlertService)
}

func (s *Server) appendTesterService() {
	l := s.LogService.NewLogger("[service-tests] ", log.LstdFlags)
	srv := servicetest.NewService(servicetest.NewConfig(), l)
	srv.HTTPDService = s.HTTPDService

	s.TesterService = srv
	s.AppendService("tests", srv)
}

func (s *Server) appendSMTPService() {
	c := s.config.SMTP
	l := s.LogService.NewLogger("[smtp] ", log.LstdFlags)
	srv := smtp.NewService(c, l)

	s.TaskMaster.SMTPService = srv
	s.AlertService.SMTPService = srv

	s.SetDynamicService("smtp", srv)
	s.AppendService("smtp", srv)
}

func (s *Server) appendInfluxDBService() error {
	c := s.config.InfluxDB
	l := s.LogService.NewLogger("[influxdb] ", log.LstdFlags)
	httpPort, err := s.config.HTTP.Port()
	if err != nil {
		return errors.Wrap(err, "failed to get http port")
	}
	srv, err := influxdb.NewService(c, httpPort, s.config.Hostname, vars.Info, s.config.HTTP.AuthEnabled, l)
	if err != nil {
		return err
	}
	srv.HTTPDService = s.HTTPDService
	srv.PointsWriter = s.TaskMaster
	srv.LogService = s.LogService
	srv.AuthService = s.AuthService
	srv.ClientCreator = iclient.ClientCreator{}

	s.InfluxDBService = srv
	s.TaskMaster.InfluxDBService = srv
	s.SetDynamicService("influxdb", srv)
	s.AppendService("influxdb", srv)
	return nil
}

func (s *Server) initHTTPDService() {
	l := s.LogService.NewLogger("[httpd] ", log.LstdFlags)
	srv := httpd.NewService(s.config.HTTP, s.hostname, l, s.LogService)

	srv.Handler.PointsWriter = s.TaskMaster
	srv.Handler.Version = s.BuildInfo.Version

	s.HTTPDService = srv
	s.TaskMaster.HTTPDService = srv
}

func (s *Server) appendHTTPDService() {
	s.AppendService("httpd", s.HTTPDService)
}

func (s *Server) appendTaskStoreService() {
	l := s.LogService.NewLogger("[task_store] ", log.LstdFlags)
	srv := task_store.NewService(s.config.Task, l)
	srv.StorageService = s.StorageService
	srv.HTTPDService = s.HTTPDService
	srv.TaskMasterLookup = s.TaskMasterLookup

	s.TaskStore = srv
	s.TaskMaster.TaskStore = srv
	s.AppendService("task_store", srv)
}

func (s *Server) appendReplayService() {
	l := s.LogService.NewLogger("[replay] ", log.LstdFlags)
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
	l := s.LogService.NewLogger("[kubernetes] ", log.LstdFlags)
	srv, err := k8s.NewService(c, s.ScraperService, l)
	if err != nil {
		return err
	}

	s.TaskMaster.K8sService = srv
	s.SetDynamicService("kubernetes", srv)
	s.AppendService("kubernetes", srv)
	return nil
}

func (s *Server) appendDeadmanService() {
	l := s.LogService.NewLogger("[deadman] ", log.LstdFlags)
	srv := deadman.NewService(s.config.Deadman, l)

	s.TaskMaster.DeadmanService = srv
	s.AppendService("deadman", srv)
}

func (s *Server) appendUDFService() {
	l := s.LogService.NewLogger("[udf] ", log.LstdFlags)
	srv := udf.NewService(s.config.UDF, l)

	s.TaskMaster.UDFService = srv
	s.AppendService("udf", srv)
}

func (s *Server) appendAuthService() {
	l := s.LogService.NewLogger("[noauth] ", log.LstdFlags)
	srv := noauth.NewService(l)

	s.AuthService = srv
	s.HTTPDService.Handler.AuthService = srv
	s.AppendService("auth", srv)
}

func (s *Server) appendOpsGenieService() {
	c := s.config.OpsGenie
	l := s.LogService.NewLogger("[opsgenie] ", log.LstdFlags)
	srv := opsgenie.NewService(c, l)

	s.TaskMaster.OpsGenieService = srv
	s.AlertService.OpsGenieService = srv

	s.SetDynamicService("opsgenie", srv)
	s.AppendService("opsgenie", srv)
}

func (s *Server) appendVictorOpsService() {
	c := s.config.VictorOps
	l := s.LogService.NewLogger("[victorops] ", log.LstdFlags)
	srv := victorops.NewService(c, l)

	s.TaskMaster.VictorOpsService = srv
	s.AlertService.VictorOpsService = srv

	s.SetDynamicService("victorops", srv)
	s.AppendService("victorops", srv)
}

func (s *Server) appendPagerDutyService() {
	c := s.config.PagerDuty
	l := s.LogService.NewLogger("[pagerduty] ", log.LstdFlags)
	srv := pagerduty.NewService(c, l)
	srv.HTTPDService = s.HTTPDService

	s.TaskMaster.PagerDutyService = srv
	s.AlertService.PagerDutyService = srv

	s.SetDynamicService("pagerduty", srv)
	s.AppendService("pagerduty", srv)
}

func (s *Server) appendPushoverService() {
	c := s.config.Pushover
	l := s.LogService.NewLogger("[pushover] ", log.LstdFlags)
	srv := pushover.NewService(c, l)

	s.TaskMaster.PushoverService = srv
	s.AlertService.PushoverService = srv

	s.SetDynamicService("pushover", srv)
	s.AppendService("pushover", srv)
}

func (s *Server) appendHTTPPostService() {
	c := s.config.HTTPPost
	l := s.LogService.NewLogger("[httppost] ", log.LstdFlags)
	srv := httppost.NewService(c, l)

	s.TaskMaster.HTTPPostService = srv
	s.AlertService.HTTPPostService = srv

	s.SetDynamicService("httppost", srv)
	s.AppendService("httppost", srv)
}

func (s *Server) appendSensuService() {
	c := s.config.Sensu
	l := s.LogService.NewLogger("[sensu] ", log.LstdFlags)
	srv := sensu.NewService(c, l)

	s.TaskMaster.SensuService = srv
	s.AlertService.SensuService = srv

	s.SetDynamicService("sensu", srv)
	s.AppendService("sensu", srv)
}

func (s *Server) appendSlackService() error {
	c := s.config.Slack
	l := s.LogService.NewLogger("[slack] ", log.LstdFlags)
	srv, err := slack.NewService(c, l)
	if err != nil {
		return err
	}

	s.TaskMaster.SlackService = srv
	s.AlertService.SlackService = srv

	s.SetDynamicService("slack", srv)
	s.AppendService("slack", srv)
	return nil
}

func (s *Server) appendSNMPTrapService() {
	c := s.config.SNMPTrap
	l := s.LogService.NewLogger("[snmptrap] ", log.LstdFlags)
	srv := snmptrap.NewService(c, l)

	s.TaskMaster.SNMPTrapService = srv
	s.AlertService.SNMPTrapService = srv

	s.SetDynamicService("snmptrap", srv)
	s.AppendService("snmptrap", srv)
}

func (s *Server) appendTelegramService() {
	c := s.config.Telegram
	l := s.LogService.NewLogger("[telegram] ", log.LstdFlags)
	srv := telegram.NewService(c, l)

	s.TaskMaster.TelegramService = srv
	s.AlertService.TelegramService = srv

	s.SetDynamicService("telegram", srv)
	s.AppendService("telegram", srv)
}

func (s *Server) appendHipChatService() {
	c := s.config.HipChat
	l := s.LogService.NewLogger("[hipchat] ", log.LstdFlags)
	srv := hipchat.NewService(c, l)

	s.TaskMaster.HipChatService = srv
	s.AlertService.HipChatService = srv

	s.SetDynamicService("hipchat", srv)
	s.AppendService("hipchat", srv)
}

func (s *Server) appendAlertaService() {
	c := s.config.Alerta
	l := s.LogService.NewLogger("[alerta] ", log.LstdFlags)
	srv := alerta.NewService(c, l)

	s.TaskMaster.AlertaService = srv
	s.AlertService.AlertaService = srv

	s.SetDynamicService("alerta", srv)
	s.AppendService("alerta", srv)
}

func (s *Server) appendTalkService() {
	c := s.config.Talk
	l := s.LogService.NewLogger("[talk] ", log.LstdFlags)
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
	w := s.LogService.NewStaticLevelWriter(logging.INFO)
	srv.SetLogOutput(w)

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
	w := s.LogService.NewStaticLevelWriter(logging.INFO)
	srv.SetLogOutput(w)

	srv.PointsWriter = s.TaskMaster
	srv.MetaClient = s.MetaClient
	s.AppendService("opentsdb", srv)
	return nil
}

func (s *Server) appendGraphiteServices() error {
	for i, c := range s.config.Graphite {
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
		s.AppendService(fmt.Sprintf("graphite%d", i), srv)
	}
	return nil
}

func (s *Server) appendUDPServices() {
	for i, c := range s.config.UDP {
		if !c.Enabled {
			continue
		}
		l := s.LogService.NewLogger("[udp] ", log.LstdFlags)
		srv := udp.NewService(c, l)
		srv.PointsWriter = s.TaskMaster
		s.AppendService(fmt.Sprintf("udp%d", i), srv)
	}
}

func (s *Server) appendStatsService() {
	c := s.config.Stats
	if c.Enabled {
		l := s.LogService.NewLogger("[stats] ", log.LstdFlags)
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
		l := s.LogService.NewLogger("[reporting] ", log.LstdFlags)
		srv := reporting.NewService(c, vars.Info, l)

		s.AppendService("reporting", srv)
	}
}

func (s *Server) appendScraperService() {
	c := s.config.Scraper
	l := s.LogService.NewLogger("[scrapers] ", log.LstdFlags)
	srv := scraper.NewService(c, l)
	srv.PointsWriter = s.TaskMaster
	s.ScraperService = srv
	s.SetDynamicService("scraper", srv)
	s.AppendService("scraper", srv)
}

func (s *Server) appendAzureService() {
	c := s.config.Azure
	l := s.LogService.NewLogger("[azure] ", log.LstdFlags)
	srv := azure.NewService(c, s.ScraperService, l)
	s.SetDynamicService("azure", srv)
	s.AppendService("azure", srv)
}

func (s *Server) appendConsulService() {
	c := s.config.Consul
	l := s.LogService.NewLogger("[consul] ", log.LstdFlags)
	srv := consul.NewService(c, s.ScraperService, l)
	s.SetDynamicService("consul", srv)
	s.AppendService("consul", srv)
}

func (s *Server) appendDNSService() {
	c := s.config.DNS
	l := s.LogService.NewLogger("[dns] ", log.LstdFlags)
	srv := dns.NewService(c, s.ScraperService, l)
	s.SetDynamicService("dns", srv)
	s.AppendService("dns", srv)
}

func (s *Server) appendEC2Service() {
	c := s.config.EC2
	l := s.LogService.NewLogger("[ec2] ", log.LstdFlags)
	srv := ec2.NewService(c, s.ScraperService, l)
	s.SetDynamicService("ec2", srv)
	s.AppendService("ec2", srv)
}

func (s *Server) appendFileService() {
	c := s.config.FileDiscovery
	l := s.LogService.NewLogger("[file-discovery] ", log.LstdFlags)
	srv := file_discovery.NewService(c, s.ScraperService, l)
	s.SetDynamicService("file-discovery", srv)
	s.AppendService("file-discovery", srv)
}

func (s *Server) appendGCEService() {
	c := s.config.GCE
	l := s.LogService.NewLogger("[gce] ", log.LstdFlags)
	srv := gce.NewService(c, s.ScraperService, l)
	s.SetDynamicService("gce", srv)
	s.AppendService("gce", srv)
}

func (s *Server) appendMarathonService() {
	c := s.config.Marathon
	l := s.LogService.NewLogger("[marathon] ", log.LstdFlags)
	srv := marathon.NewService(c, s.ScraperService, l)
	s.SetDynamicService("marathon", srv)
	s.AppendService("marathon", srv)
}

func (s *Server) appendNerveService() {
	c := s.config.Nerve
	l := s.LogService.NewLogger("[nerve] ", log.LstdFlags)
	srv := nerve.NewService(c, s.ScraperService, l)
	s.SetDynamicService("nerve", srv)
	s.AppendService("nerve", srv)
}

func (s *Server) appendServersetService() {
	c := s.config.Serverset
	l := s.LogService.NewLogger("[serverset] ", log.LstdFlags)
	srv := serverset.NewService(c, s.ScraperService, l)
	s.SetDynamicService("serverset", srv)
	s.AppendService("serverset", srv)
}

func (s *Server) appendStaticService() {
	c := s.config.StaticDiscovery
	l := s.LogService.NewLogger("[static-discovery] ", log.LstdFlags)
	srv := static_discovery.NewService(c, s.ScraperService, l)
	s.SetDynamicService("static-discovery", srv)
	s.AppendService("static-discovery", srv)
}

func (s *Server) appendTritonService() {
	c := s.config.Triton
	l := s.LogService.NewLogger("[triton] ", log.LstdFlags)
	srv := triton.NewService(c, s.ScraperService, l)
	s.SetDynamicService("triton", srv)
	s.AppendService("triton", srv)
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

	go s.watchServices()
	go s.watchConfigUpdates()

	return nil
}

func (s *Server) startServices() error {
	for _, service := range s.Services {
		s.Logger.Printf("D! opening service: %T", service)
		if err := service.Open(); err != nil {
			return fmt.Errorf("open service %T: %s", service, err)
		}
		s.Logger.Printf("D! opened service: %T", service)

		// Apply config overrides after the config override service has been opened and before any dynamic services.
		if service == s.ConfigOverrideService && !s.config.SkipConfigOverrides && s.config.ConfigOverride.Enabled {
			// Apply initial config updates
			s.Logger.Println("D! applying configuration overrides")
			configs, err := s.ConfigOverrideService.Config()
			if err != nil {
				return errors.Wrap(err, "failed to apply config overrides")
			}
			for service, config := range configs {
				if srv, ok := s.DynamicServices[service]; !ok {
					return fmt.Errorf("found configuration override for unknown service %q", service)
				} else {
					s.Logger.Println("D! applying configuration overrides for", service)
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
		s.Logger.Printf("E! error closing httpd service: %v", err)
	}
	if s.StatsService != nil {
		if err := s.StatsService.Close(); err != nil {
			s.Logger.Printf("E! error closing stats service: %v", err)
		}
	}

	// Drain the in-flight writes and stop all tasks.
	s.TaskMaster.Drain()
	s.TaskMaster.StopTasks()

	// Close services now that all tasks are stopped.
	for i := len(s.Services) - 1; i >= 0; i-- {
		service := s.Services[i]
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
	// Create the data dir if not exists
	if f, err := os.Stat(s.dataDir); err != nil {
		if os.IsNotExist(err) {
			if err := os.Mkdir(s.dataDir, 0755); err != nil {
				return errors.Wrapf(err, "data_dir %q does not exist, failed to create it", s.dataDir)
			}
		} else {
			return errors.Wrapf(err, "failed to stat data dir %q", s.dataDir)
		}
	} else if !f.IsDir() {
		return fmt.Errorf("path data_dir %s exists and is not a directory", s.dataDir)
	}
	clusterIDPath := filepath.Join(s.dataDir, clusterIDFilename)
	clusterID, err := s.readID(clusterIDPath)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if clusterID == uuid.Nil {
		clusterID = uuid.New()
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
	if serverID == uuid.Nil {
		serverID = uuid.New()
		if err := s.writeID(serverIDPath, serverID); err != nil {
			return errors.Wrap(err, "failed to save server ID")
		}
	}
	s.ServerID = serverID

	return nil
}

func (s *Server) readID(file string) (uuid.UUID, error) {
	f, err := os.Open(file)
	if err != nil {
		return uuid.Nil, err
	}
	defer f.Close()
	b, err := ioutil.ReadAll(f)
	if err != nil {
		return uuid.Nil, err
	}
	return uuid.ParseBytes(b)
}

func (s *Server) writeID(file string, id uuid.UUID) error {
	f, err := os.Create(file)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write([]byte(id.String()))
	if err != nil {
		return err
	}
	return nil
}

func (s *Server) SetClusterID(clusterID uuid.UUID) error {
	s.clusterIDMu.Lock()
	defer s.clusterIDMu.Unlock()
	if s.ClusterID == clusterID {
		return nil
	}
	clusterIDPath := filepath.Join(s.dataDir, clusterIDFilename)
	if err := s.writeID(clusterIDPath, clusterID); err != nil {
		return errors.Wrap(err, "failed to save cluster ID")
	}
	s.ClusterID = clusterID
	vars.ClusterIDVar.Set(s.ClusterID)
	return nil
}

// Service represents a service attached to the server.
type Service interface {
	Open() error
	Close() error
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
		s.Logger.Printf("I! writing CPU profile to: %s\n", cpuprofile)
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
		s.Logger.Printf("I! writing mem profile to: %s\n", memprofile)
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
		s.Logger.Println("I! CPU profile stopped")
	}
	if prof.mem != nil {
		if err := pprof.Lookup("heap").WriteTo(prof.mem, 0); err != nil {
			s.Logger.Printf("I! failed to write mem profile: %v\n", err)
		}
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
