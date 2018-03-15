// Provides a server type for starting and configuring a Kapacitor server.
package server

import (
	"fmt"
	"io/ioutil"
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
	"github.com/influxdata/kapacitor/keyvalue"
	"github.com/influxdata/kapacitor/server/vars"
	"github.com/influxdata/kapacitor/services/alert"
	"github.com/influxdata/kapacitor/services/alerta"
	"github.com/influxdata/kapacitor/services/azure"
	"github.com/influxdata/kapacitor/services/config"
	"github.com/influxdata/kapacitor/services/consul"
	"github.com/influxdata/kapacitor/services/deadman"
	"github.com/influxdata/kapacitor/services/diagnostic"
	"github.com/influxdata/kapacitor/services/dns"
	"github.com/influxdata/kapacitor/services/ec2"
	"github.com/influxdata/kapacitor/services/file_discovery"
	"github.com/influxdata/kapacitor/services/gce"
	"github.com/influxdata/kapacitor/services/hipchat"
	"github.com/influxdata/kapacitor/services/httpd"
	"github.com/influxdata/kapacitor/services/httppost"
	"github.com/influxdata/kapacitor/services/influxdb"
	"github.com/influxdata/kapacitor/services/k8s"
	"github.com/influxdata/kapacitor/services/kafka"
	"github.com/influxdata/kapacitor/services/load"
	"github.com/influxdata/kapacitor/services/marathon"
	"github.com/influxdata/kapacitor/services/mqtt"
	"github.com/influxdata/kapacitor/services/nerve"
	"github.com/influxdata/kapacitor/services/noauth"
	"github.com/influxdata/kapacitor/services/opsgenie"
	"github.com/influxdata/kapacitor/services/opsgenie2"
	"github.com/influxdata/kapacitor/services/pagerduty"
	"github.com/influxdata/kapacitor/services/pagerduty2"
	"github.com/influxdata/kapacitor/services/pushover"
	"github.com/influxdata/kapacitor/services/replay"
	"github.com/influxdata/kapacitor/services/reporting"
	"github.com/influxdata/kapacitor/services/scraper"
	"github.com/influxdata/kapacitor/services/sensu"
	"github.com/influxdata/kapacitor/services/serverset"
	"github.com/influxdata/kapacitor/services/servicetest"
	"github.com/influxdata/kapacitor/services/sideload"
	"github.com/influxdata/kapacitor/services/slack"
	"github.com/influxdata/kapacitor/services/smtp"
	"github.com/influxdata/kapacitor/services/snmptrap"
	"github.com/influxdata/kapacitor/services/static_discovery"
	"github.com/influxdata/kapacitor/services/stats"
	"github.com/influxdata/kapacitor/services/storage"
	"github.com/influxdata/kapacitor/services/swarm"
	"github.com/influxdata/kapacitor/services/talk"
	"github.com/influxdata/kapacitor/services/task_store"
	"github.com/influxdata/kapacitor/services/telegram"
	"github.com/influxdata/kapacitor/services/triton"
	"github.com/influxdata/kapacitor/services/udf"
	"github.com/influxdata/kapacitor/services/udp"
	"github.com/influxdata/kapacitor/services/victorops"
	"github.com/influxdata/kapacitor/uuid"
	"github.com/influxdata/kapacitor/waiter"
	"github.com/pkg/errors"
)

const clusterIDFilename = "cluster.id"
const serverIDFilename = "server.id"

// BuildInfo represents the build details for the server code.
type BuildInfo struct {
	Version  string
	Commit   string
	Branch   string
	Platform string
}

type Diagnostic interface {
	Debug(msg string, ctx ...keyvalue.T)
	Info(msg string, ctx ...keyvalue.T)
	Error(msg string, err error, ctx ...keyvalue.T)
}

// Server represents a container for the metadata and storage data and services.
// It is built using a Config and it manages the startup and shutdown of all
// services in the proper order.
type Server struct {
	dataDir  string
	hostname string

	config *Config

	err chan error

	// clusterIDChanged signals when the cluster ID has changed.
	clusterIDChanged *waiter.WaiterGroup

	Commander command.Commander

	TaskMaster       *kapacitor.TaskMaster
	TaskMasterLookup *kapacitor.TaskMasterLookup

	LoadService           *load.Service
	SideloadService       *sideload.Service
	AuthService           auth.Interface
	HTTPDService          *httpd.Service
	StorageService        *storage.Service
	AlertService          *alert.Service
	TaskStore             *task_store.Service
	ReplayService         *replay.Service
	SessionService        *diagnostic.SessionService
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

	DiagService *diagnostic.Service
	Diag        Diagnostic
}

// New returns a new instance of Server built from a config.
func New(c *Config, buildInfo BuildInfo, diagService *diagnostic.Service) (*Server, error) {
	err := c.Validate()
	if err != nil {
		return nil, fmt.Errorf("invalid configuration: %s. To generate a valid configuration file run `kapacitord config > kapacitor.generated.conf`.", err)
	}
	d := diagService.NewServerHandler()
	s := &Server{
		config:           c,
		BuildInfo:        buildInfo,
		dataDir:          c.DataDir,
		hostname:         c.Hostname,
		err:              make(chan error),
		configUpdates:    make(chan config.ConfigUpdate, 100),
		DiagService:      diagService,
		MetaClient:       &kapacitor.NoopMetaClient{},
		QueryExecutor:    &Queryexecutor{},
		Diag:             d,
		ServicesByName:   make(map[string]int),
		DynamicServices:  make(map[string]Updater),
		Commander:        c.Commander,
		clusterIDChanged: waiter.NewGroup(),
	}
	s.Diag.Info("listing Kapacitor hostname", keyvalue.KV("hostname", s.hostname))

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
	vars.PlatformVar.Set(s.BuildInfo.Platform)
	vars.VersionVar.Set(s.BuildInfo.Version)
	s.Diag.Info("listing ClusterID and ServerID",
		keyvalue.KV("cluster_id", s.ClusterID.String()), keyvalue.KV("server_id", s.ServerID.String()))

	// Start Task Master
	s.TaskMasterLookup = kapacitor.NewTaskMasterLookup()
	kd := diagService.NewKapacitorHandler()
	s.TaskMaster = kapacitor.NewTaskMaster(kapacitor.MainTaskMaster, vars.Info, kd)
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
	s.appendSideloadService()

	// Init alert service
	s.initAlertService()

	// Append all dynamic services after the config override and tester services.
	s.appendUDFService()
	s.appendDeadmanService()

	if err := s.appendInfluxDBService(); err != nil {
		return nil, errors.Wrap(err, "influxdb service")
	}

	if err := s.appendLoadService(); err != nil {
		return nil, errors.Wrap(err, "load service")
	}

	// Append Alert integration services
	s.appendAlertaService()
	s.appendHipChatService()
	s.appendKafkaService()
	if err := s.appendMQTTService(); err != nil {
		return nil, errors.Wrap(err, "mqtt service")
	}
	s.appendOpsGenieService()
	s.appendOpsGenie2Service()
	s.appendPagerDutyService()
	s.appendPagerDuty2Service()
	s.appendPushoverService()
	if err := s.appendHTTPPostService(); err != nil {
		return nil, errors.Wrap(err, "httppost service")
	}
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
	s.appendSessionService()

	// Append third-party integrations
	// Append extra input services
	if err := s.appendCollectdService(); err != nil {
		return nil, errors.Wrap(err, "collectd service")
	}
	s.appendUDPServices()
	if err := s.appendOpenTSDBService(); err != nil {
		return nil, errors.Wrap(err, "opentsdb service")
	}
	if err := s.appendGraphiteServices(); err != nil {
		return nil, errors.Wrap(err, "graphite service")
	}

	// Append Scraper and discovery services
	s.appendScraperService()

	if err := s.appendK8sService(); err != nil {
		return nil, errors.Wrap(err, "kubernetes service")
	}
	if err := s.appendSwarmService(); err != nil {
		return nil, errors.Wrap(err, "docker swarm service")
	}
	if err := s.appendEC2Service(); err != nil {
		return nil, errors.Wrap(err, "Aws service")
	}

	s.appendAzureService()
	s.appendConsulService()
	s.appendDNSService()
	s.appendFileService()
	s.appendGCEService()
	s.appendMarathonService()
	s.appendNerveService()
	s.appendServersetService()
	s.appendStaticService()
	s.appendTritonService()

	// Append StatsService and ReportingService after other services so all stats are ready
	// to be reported
	s.appendStatsService()
	s.appendReportingService()

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
	d := s.DiagService.NewStorageHandler()
	srv := storage.NewService(s.config.Storage, d)

	srv.HTTPDService = s.HTTPDService

	s.StorageService = srv
	s.AppendService("storage", srv)
}

func (s *Server) appendConfigOverrideService() {
	d := s.DiagService.NewConfigOverrideHandler()
	srv := config.NewService(s.config.ConfigOverride, s.config, d, s.configUpdates)
	srv.HTTPDService = s.HTTPDService
	srv.StorageService = s.StorageService

	s.ConfigOverrideService = srv
	s.AppendService("config", srv)
}

func (s *Server) initAlertService() {
	d := s.DiagService.NewAlertServiceHandler()
	srv := alert.NewService(d)

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
	srv := servicetest.NewService(servicetest.NewConfig())
	srv.HTTPDService = s.HTTPDService

	s.TesterService = srv
	s.AppendService("tests", srv)
}

func (s *Server) appendSideloadService() {
	d := s.DiagService.NewSideloadHandler()
	srv := sideload.NewService(d)
	srv.HTTPDService = s.HTTPDService

	s.SideloadService = srv
	s.TaskMaster.SideloadService = srv
	s.AppendService("sideload", srv)
}

func (s *Server) appendSMTPService() {
	c := s.config.SMTP
	d := s.DiagService.NewSMTPHandler()
	srv := smtp.NewService(c, d)

	s.TaskMaster.SMTPService = srv
	s.AlertService.SMTPService = srv

	s.SetDynamicService("smtp", srv)
	s.AppendService("smtp", srv)
}

func (s *Server) appendLoadService() error {
	c := s.config.Load
	d := s.DiagService.NewLoadHandler()
	if s.HTTPDService == nil {
		return errors.New("httpd service must be set for load service")
	}
	if s.HTTPDService.LocalHandler == nil {
		return errors.New("httpd service handler must be set for load service")
	}
	srv, err := load.NewService(c, s.HTTPDService.LocalHandler, d)
	if err != nil {
		return err
	}

	srv.StorageService = s.StorageService

	s.LoadService = srv
	s.AppendService("load", srv)

	return nil
}

func (s *Server) appendInfluxDBService() error {
	c := s.config.InfluxDB
	d := s.DiagService.NewInfluxDBHandler()
	httpPort, err := s.config.HTTP.Port()
	if err != nil {
		return errors.Wrap(err, "failed to get http port")
	}
	srv, err := influxdb.NewService(c, httpPort, s.config.Hostname, vars.Info, s.config.HTTP.AuthEnabled, d)
	if err != nil {
		return err
	}
	w, err := s.newClusterIDChangedWaiter()
	if err != nil {
		return err
	}
	srv.ClusterIDWaiter = w

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
	d := s.DiagService.NewHTTPDHandler()
	srv := httpd.NewService(s.config.HTTP, s.hostname, d)

	srv.LocalHandler.PointsWriter = s.TaskMaster
	srv.Handler.PointsWriter = s.TaskMaster

	srv.LocalHandler.DiagService = s.DiagService
	srv.Handler.DiagService = s.DiagService

	srv.LocalHandler.Version = s.BuildInfo.Version
	srv.Handler.Version = s.BuildInfo.Version

	s.HTTPDService = srv
	s.TaskMaster.HTTPDService = srv
}

func (s *Server) appendHTTPDService() {
	s.AppendService("httpd", s.HTTPDService)
}

func (s *Server) appendTaskStoreService() {
	d := s.DiagService.NewTaskStoreHandler()
	srv := task_store.NewService(s.config.Task, d)
	srv.StorageService = s.StorageService
	srv.HTTPDService = s.HTTPDService
	srv.TaskMasterLookup = s.TaskMasterLookup

	s.TaskStore = srv
	s.TaskMaster.TaskStore = srv
	s.AppendService("task_store", srv)
}

func (s *Server) appendSessionService() {
	srv := s.DiagService.SessionService
	srv.HTTPDService = s.HTTPDService

	s.AppendService("session", srv)
}

func (s *Server) appendReplayService() {
	d := s.DiagService.NewReplayHandler()
	srv := replay.NewService(s.config.Replay, d)
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
	d := s.DiagService.NewK8sHandler()
	srv, err := k8s.NewService(c, s.ScraperService, d)
	if err != nil {
		return err
	}

	s.TaskMaster.K8sService = srv
	s.SetDynamicService("kubernetes", srv)
	s.AppendService("kubernetes", srv)
	return nil
}
func (s *Server) appendSwarmService() error {
	c := s.config.Swarm
	d := s.DiagService.NewSwarmHandler()
	srv, err := swarm.NewService(c, d)
	if err != nil {
		return err
	}

	s.TaskMaster.SwarmService = srv
	s.SetDynamicService("swarm", srv)
	s.AppendService("swarm", srv)
	return nil
}
func (s *Server) appendEC2Service() error {
	c := s.config.EC2
	d := s.DiagService.NewEC2Handler()
	srv, err := ec2.NewService(c, s.ScraperService, d)
	if err != nil {
		return err
	}

	s.TaskMaster.EC2Service = srv
	s.SetDynamicService("ec2", srv)
	s.AppendService("ec2", srv)
	return nil
}
func (s *Server) appendDeadmanService() {
	d := s.DiagService.NewDeadmanHandler()
	srv := deadman.NewService(s.config.Deadman, d)

	s.TaskMaster.DeadmanService = srv
	s.AppendService("deadman", srv)
}

func (s *Server) appendUDFService() {
	d := s.DiagService.NewUDFServiceHandler()
	srv := udf.NewService(s.config.UDF, d)

	s.TaskMaster.UDFService = srv
	s.AppendService("udf", srv)
}

func (s *Server) appendAuthService() {
	d := s.DiagService.NewNoAuthHandler()
	srv := noauth.NewService(d)

	s.AuthService = srv
	s.HTTPDService.Handler.AuthService = srv
	s.AppendService("auth", srv)
}

func (s *Server) appendMQTTService() error {
	cs := s.config.MQTT
	d := s.DiagService.NewMQTTHandler()
	srv, err := mqtt.NewService(cs, d)
	if err != nil {
		return err
	}

	s.TaskMaster.MQTTService = srv
	s.AlertService.MQTTService = srv

	s.SetDynamicService("mqtt", srv)
	s.AppendService("mqtt", srv)
	return nil
}

func (s *Server) appendOpsGenieService() {
	c := s.config.OpsGenie
	d := s.DiagService.NewOpsGenieHandler()
	srv := opsgenie.NewService(c, d)

	s.TaskMaster.OpsGenieService = srv
	s.AlertService.OpsGenieService = srv

	s.SetDynamicService("opsgenie", srv)
	s.AppendService("opsgenie", srv)
}
func (s *Server) appendOpsGenie2Service() {
	c := s.config.OpsGenie2
	d := s.DiagService.NewOpsGenie2Handler()
	srv := opsgenie2.NewService(c, d)

	s.TaskMaster.OpsGenie2Service = srv
	s.AlertService.OpsGenie2Service = srv

	s.SetDynamicService("opsgenie2", srv)
	s.AppendService("opsgenie2", srv)
}

func (s *Server) appendVictorOpsService() {
	c := s.config.VictorOps
	d := s.DiagService.NewVictorOpsHandler()
	srv := victorops.NewService(c, d)

	s.TaskMaster.VictorOpsService = srv
	s.AlertService.VictorOpsService = srv

	s.SetDynamicService("victorops", srv)
	s.AppendService("victorops", srv)
}

func (s *Server) appendPagerDutyService() {
	c := s.config.PagerDuty
	d := s.DiagService.NewPagerDutyHandler()
	srv := pagerduty.NewService(c, d)
	srv.HTTPDService = s.HTTPDService

	s.TaskMaster.PagerDutyService = srv
	s.AlertService.PagerDutyService = srv

	s.SetDynamicService("pagerduty", srv)
	s.AppendService("pagerduty", srv)
}

func (s *Server) appendPagerDuty2Service() {
	c := s.config.PagerDuty2
	d := s.DiagService.NewPagerDuty2Handler()
	srv := pagerduty2.NewService(c, d)
	srv.HTTPDService = s.HTTPDService

	s.TaskMaster.PagerDuty2Service = srv
	s.AlertService.PagerDuty2Service = srv

	s.SetDynamicService("pagerduty2", srv)
	s.AppendService("pagerduty2", srv)
}

func (s *Server) appendPushoverService() {
	c := s.config.Pushover
	d := s.DiagService.NewPushoverHandler()
	srv := pushover.NewService(c, d)

	s.TaskMaster.PushoverService = srv
	s.AlertService.PushoverService = srv

	s.SetDynamicService("pushover", srv)
	s.AppendService("pushover", srv)
}

func (s *Server) appendHTTPPostService() error {
	c := s.config.HTTPPost
	d := s.DiagService.NewHTTPPostHandler()
	srv, err := httppost.NewService(c, d)
	if err != nil {
		return err
	}

	s.TaskMaster.HTTPPostService = srv
	s.AlertService.HTTPPostService = srv

	s.SetDynamicService("httppost", srv)
	s.AppendService("httppost", srv)
	return nil
}

func (s *Server) appendSensuService() {
	c := s.config.Sensu
	d := s.DiagService.NewSensuHandler()
	srv := sensu.NewService(c, d)

	s.TaskMaster.SensuService = srv
	s.AlertService.SensuService = srv

	s.SetDynamicService("sensu", srv)
	s.AppendService("sensu", srv)
}

func (s *Server) appendSlackService() error {
	c := s.config.Slack
	d := s.DiagService.NewSlackHandler()
	srv, err := slack.NewService(c, d)
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
	d := s.DiagService.NewSNMPTrapHandler()
	srv := snmptrap.NewService(c, d)

	s.TaskMaster.SNMPTrapService = srv
	s.AlertService.SNMPTrapService = srv

	s.SetDynamicService("snmptrap", srv)
	s.AppendService("snmptrap", srv)
}

func (s *Server) appendTelegramService() {
	c := s.config.Telegram
	d := s.DiagService.NewTelegramHandler()
	srv := telegram.NewService(c, d)

	s.TaskMaster.TelegramService = srv
	s.AlertService.TelegramService = srv

	s.SetDynamicService("telegram", srv)
	s.AppendService("telegram", srv)
}

func (s *Server) appendHipChatService() {
	c := s.config.HipChat
	d := s.DiagService.NewHipChatHandler()
	srv := hipchat.NewService(c, d)

	s.TaskMaster.HipChatService = srv
	s.AlertService.HipChatService = srv

	s.SetDynamicService("hipchat", srv)
	s.AppendService("hipchat", srv)
}

func (s *Server) appendKafkaService() {
	c := s.config.Kafka
	d := s.DiagService.NewKafkaHandler()
	srv := kafka.NewService(c, d)

	s.TaskMaster.KafkaService = srv
	s.AlertService.KafkaService = srv

	s.SetDynamicService("kafka", srv)
	s.AppendService("kafka", srv)
}

func (s *Server) appendAlertaService() {
	c := s.config.Alerta
	d := s.DiagService.NewAlertaHandler()
	srv := alerta.NewService(c, d)

	s.TaskMaster.AlertaService = srv
	s.AlertService.AlertaService = srv

	s.SetDynamicService("alerta", srv)
	s.AppendService("alerta", srv)
}

func (s *Server) appendTalkService() {
	c := s.config.Talk
	d := s.DiagService.NewTalkHandler()
	srv := talk.NewService(c, d)

	s.TaskMaster.TalkService = srv
	s.AlertService.TalkService = srv

	s.SetDynamicService("talk", srv)
	s.AppendService("talk", srv)
}

func (s *Server) appendCollectdService() error {
	c := s.config.Collectd
	if !c.Enabled {
		return nil
	}
	srv := collectd.NewService(c)
	w, err := s.DiagService.NewStaticLevelHandler("info", "collectd")
	if err != nil {
		return fmt.Errorf("failed to create static level handler for collectd: %v", err)
	}
	srv.SetLogOutput(w)

	srv.MetaClient = s.MetaClient
	srv.PointsWriter = s.TaskMaster
	s.AppendService("collectd", srv)

	return nil
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
	w, err := s.DiagService.NewStaticLevelHandler("info", "opentsdb")
	if err != nil {
		return fmt.Errorf("failed to create static level handler for opentsdb: %v", err)
	}
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
		w, err := s.DiagService.NewStaticLevelHandler("info", "graphite")
		if err != nil {
			return fmt.Errorf("failed to create static level handler for graphite: %v", err)
		}
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
		d := s.DiagService.NewUDPHandler()
		srv := udp.NewService(c, d)
		srv.PointsWriter = s.TaskMaster
		s.AppendService(fmt.Sprintf("udp%d", i), srv)
	}
}

func (s *Server) appendStatsService() {
	c := s.config.Stats
	if c.Enabled {
		d := s.DiagService.NewStatsHandler()
		srv := stats.NewService(c, d)
		srv.TaskMaster = s.TaskMaster

		s.StatsService = srv
		s.TaskMaster.TimingService = srv
		s.AppendService("stats", srv)
	}
}

func (s *Server) appendReportingService() {
	c := s.config.Reporting
	if c.Enabled {
		d := s.DiagService.NewReportingHandler()
		srv := reporting.NewService(c, vars.Info, d)

		s.AppendService("reporting", srv)
	}
}

func (s *Server) appendScraperService() {
	c := s.config.Scraper
	d := s.DiagService.NewScraperHandler()
	srv := scraper.NewService(c, d)
	srv.PointsWriter = s.TaskMaster
	s.ScraperService = srv
	s.SetDynamicService("scraper", srv)
	s.AppendService("scraper", srv)
}

func (s *Server) appendAzureService() {
	c := s.config.Azure
	d := s.DiagService.NewAzureHandler()
	srv := azure.NewService(c, s.ScraperService, d)
	s.SetDynamicService("azure", srv)
	s.AppendService("azure", srv)
}

func (s *Server) appendConsulService() {
	c := s.config.Consul
	d := s.DiagService.NewConsulHandler()
	srv := consul.NewService(c, s.ScraperService, d)
	s.SetDynamicService("consul", srv)
	s.AppendService("consul", srv)
}

func (s *Server) appendDNSService() {
	c := s.config.DNS
	d := s.DiagService.NewDNSHandler()
	srv := dns.NewService(c, s.ScraperService, d)
	s.SetDynamicService("dns", srv)
	s.AppendService("dns", srv)
}

func (s *Server) appendFileService() {
	c := s.config.FileDiscovery
	d := s.DiagService.NewFileDiscoveryHandler()
	srv := file_discovery.NewService(c, s.ScraperService, d)
	s.SetDynamicService("file-discovery", srv)
	s.AppendService("file-discovery", srv)
}

func (s *Server) appendGCEService() {
	c := s.config.GCE
	d := s.DiagService.NewGCEHandler()
	srv := gce.NewService(c, s.ScraperService, d)
	s.SetDynamicService("gce", srv)
	s.AppendService("gce", srv)
}

func (s *Server) appendMarathonService() {
	c := s.config.Marathon
	d := s.DiagService.NewMarathonHandler()
	srv := marathon.NewService(c, s.ScraperService, d)
	s.SetDynamicService("marathon", srv)
	s.AppendService("marathon", srv)
}

func (s *Server) appendNerveService() {
	c := s.config.Nerve
	d := s.DiagService.NewNerveHandler()
	srv := nerve.NewService(c, s.ScraperService, d)
	s.SetDynamicService("nerve", srv)
	s.AppendService("nerve", srv)
}

func (s *Server) appendServersetService() {
	c := s.config.Serverset
	d := s.DiagService.NewServersetHandler()
	srv := serverset.NewService(c, s.ScraperService, d)
	s.SetDynamicService("serverset", srv)
	s.AppendService("serverset", srv)
}

func (s *Server) appendStaticService() {
	c := s.config.StaticDiscovery
	d := s.DiagService.NewStaticDiscoveryHandler()
	srv := static_discovery.NewService(c, s.ScraperService, d)
	s.SetDynamicService("static-discovery", srv)
	s.AppendService("static-discovery", srv)
}

func (s *Server) appendTritonService() {
	c := s.config.Triton
	d := s.DiagService.NewTritonHandler()
	srv := triton.NewService(c, s.ScraperService, d)
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

	if err := s.LoadService.Load(); err != nil {
		return fmt.Errorf("failed to reload tasks/templates/handlers: %v", err)
	}

	go s.watchServices()
	go s.watchConfigUpdates()

	return nil
}

func (s *Server) startServices() error {
	for _, service := range s.Services {
		s.Diag.Debug("opening service", keyvalue.KV("service", fmt.Sprintf("%T", service)))
		if err := service.Open(); err != nil {
			return fmt.Errorf("open service %T: %s", service, err)
		}
		s.Diag.Debug("opened service", keyvalue.KV("service", fmt.Sprintf("%T", service)))

		// Apply config overrides after the config override service has been opened and before any dynamic services.
		if service == s.ConfigOverrideService && !s.config.SkipConfigOverrides && s.config.ConfigOverride.Enabled {
			// Apply initial config updates
			s.Diag.Debug("applying config overrides")
			configs, err := s.ConfigOverrideService.Config()
			if err != nil {
				return errors.Wrap(err, "failed to apply config overrides")
			}
			for service, config := range configs {
				if srv, ok := s.DynamicServices[service]; !ok {
					return fmt.Errorf("found configuration override for unknown service %q", service)
				} else {
					s.Diag.Debug("applying config overrides for service", keyvalue.KV("service", service))
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
	s.clusterIDChanged.Stop()

	// Close all services that write points first.
	if err := s.HTTPDService.Close(); err != nil {
		s.Diag.Error("error closing httpd service", err)
	}
	if s.StatsService != nil {
		if err := s.StatsService.Close(); err != nil {
			s.Diag.Error("error closing stats service", err)
		}
	}

	// Drain the in-flight writes and stop all tasks.
	s.TaskMaster.Drain()
	s.TaskMaster.StopTasks()

	// Close services now that all tasks are stopped.
	for i := len(s.Services) - 1; i >= 0; i-- {
		service := s.Services[i]
		s.Diag.Debug("closing service", keyvalue.KV("service", fmt.Sprintf("%T", service)))
		err := service.Close()
		if err != nil {
			s.Diag.Error("error closing service", err, keyvalue.KV("service", fmt.Sprintf("%T", service)))
		}
		s.Diag.Debug("closed service", keyvalue.KV("service", fmt.Sprintf("%T", service)))
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

func (s *Server) Reload() {
	if err := s.LoadService.Load(); err != nil {
		s.Diag.Error("failed to reload tasks/templates/handlers", err)
	}
	if err := s.SideloadService.Reload(); err != nil {
		s.Diag.Error("failed to reload sideload sources", err)
	}
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
	s.clusterIDChanged.Broadcast()
	return nil
}

func (s *Server) newClusterIDChangedWaiter() (waiter.Waiter, error) {
	return s.clusterIDChanged.NewWaiter()
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
		s.Diag.Info("writing CPU profile", keyvalue.KV("file", cpuprofile))
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
		s.Diag.Info("writing mem profile", keyvalue.KV("file", memprofile))
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
		s.Diag.Info("CPU profile stopped")
	}
	if prof.mem != nil {
		if err := pprof.Lookup("heap").WriteTo(prof.mem, 0); err != nil {
			s.Diag.Error("failed to write mem profile", err)
		}
		prof.mem.Close()
		s.Diag.Info("mem profile stopped")
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
