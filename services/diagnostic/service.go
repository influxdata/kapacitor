package diagnostic

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path"
	"strings"
	"sync"

	klog "github.com/influxdata/kapacitor/services/diagnostic/internal/log"
)

type nopCloser struct {
	f io.Writer
}

func (c *nopCloser) Write(b []byte) (int, error) { return c.f.Write(b) }
func (c *nopCloser) Close() error                { return nil }

type Service struct {
	c Config

	logger *klog.Logger

	f      io.WriteCloser
	stdout io.Writer
	stderr io.Writer

	mu    sync.RWMutex
	level string
}

func NewService(c Config, stdout, stderr io.Writer) *Service {
	return &Service{
		c:      c,
		stdout: stdout,
		stderr: stderr,
	}
}

func BootstrapMainHandler() *CmdHandler {
	s := NewService(NewConfig(), nil, os.Stderr)
	// Should never error
	_ = s.Open()

	return s.NewCmdHandler()
}

func (s *Service) SetLogLevelFromName(lvl string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	level := strings.ToUpper(lvl)
	switch level {
	case "INFO", "ERROR", "WARN", "DEBUG":
		s.level = level
	default:
		return errors.New("invalid log level")
	}

	return nil
}

func logLevelFromName(lvl string) klog.Level {
	var level klog.Level
	switch lvl {
	case "INFO":
		level = klog.InfoLevel
	case "ERROR":
		level = klog.ErrorLevel
	case "WARN":
		level = klog.WarnLevel
	case "DEBUG":
		level = klog.DebugLevel
	}

	return level
}

func (s *Service) Open() error {
	s.mu.Lock()
	s.level = s.c.Level
	s.mu.Unlock()

	levelF := func(lvl klog.Level) bool {
		s.mu.RLock()
		defer s.mu.RUnlock()
		return lvl >= logLevelFromName(s.level)
	}

	switch s.c.File {
	case "STDERR":
		s.f = &nopCloser{f: s.stderr}
	case "STDOUT":
		s.f = &nopCloser{f: s.stdout}
	default:
		dir := path.Dir(s.c.File)
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			err := os.MkdirAll(dir, 0755)
			if err != nil {
				return err
			}
		}

		f, err := os.OpenFile(s.c.File, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0640)
		if err != nil {
			return err
		}
		s.f = f
	}

	s.logger = klog.NewLogger(s.f)
	s.logger.SetLevelF(levelF)
	return nil
}

func (s *Service) Close() error {
	if s.f != nil {
		return s.f.Close()
	}
	return nil
}

func (s *Service) NewVictorOpsHandler() *VictorOpsHandler {
	return &VictorOpsHandler{
		l: s.logger.With(klog.String("service", "victorops")),
	}
}

func (s *Service) NewSlackHandler() *SlackHandler {
	return &SlackHandler{
		l: s.logger.With(klog.String("service", "slack")),
	}
}

func (s *Service) NewTaskStoreHandler() *TaskStoreHandler {
	return &TaskStoreHandler{
		l: s.logger.With(klog.String("service", "task_store")),
	}
}

func (s *Service) NewReportingHandler() *ReportingHandler {
	return &ReportingHandler{
		l: s.logger.With(klog.String("service", "reporting")),
	}
}

func (s *Service) NewStorageHandler() *StorageHandler {
	return &StorageHandler{
		l: s.logger.With(klog.String("service", "storage")),
	}
}

func (s *Service) NewHTTPDHandler() *HTTPDHandler {
	return &HTTPDHandler{
		l: s.logger.With(klog.String("service", "http")),
	}
}

func (s *Service) NewAlertaHandler() *AlertaHandler {
	return &AlertaHandler{
		l: s.logger.With(klog.String("service", "alerta")),
	}
}

func (s *Service) NewKapacitorHandler() *KapacitorHandler {
	return &KapacitorHandler{
		l: s.logger.With(klog.String("service", "kapacitor")),
	}
}

func (s *Service) NewAlertServiceHandler() *AlertServiceHandler {
	return &AlertServiceHandler{
		l: s.logger.With(klog.String("service", "alert")),
	}
}

func (s *Service) NewHipChatHandler() *HipChatHandler {
	return &HipChatHandler{
		l: s.logger.With(klog.String("service", "hipchat")),
	}
}

func (s *Service) NewPagerDutyHandler() *PagerDutyHandler {
	return &PagerDutyHandler{
		l: s.logger.With(klog.String("service", "pagerduty")),
	}
}

func (s *Service) NewSMTPHandler() *SMTPHandler {
	return &SMTPHandler{
		l: s.logger.With(klog.String("service", "smtp")),
	}
}

func (s *Service) NewUDFServiceHandler() *UDFServiceHandler {
	return &UDFServiceHandler{
		l: s.logger.With(klog.String("service", "udf")),
	}
}

func (s *Service) NewOpsGenieHandler() *OpsGenieHandler {
	return &OpsGenieHandler{
		l: s.logger.With(klog.String("service", "opsgenie")),
	}
}

func (s *Service) NewPushoverHandler() *PushoverHandler {
	return &PushoverHandler{
		l: s.logger.With(klog.String("service", "pushover")),
	}
}

func (s *Service) NewHTTPPostHandler() *HTTPPostHandler {
	return &HTTPPostHandler{
		l: s.logger.With(klog.String("service", "httppost")),
	}
}

func (s *Service) NewSensuHandler() *SensuHandler {
	return &SensuHandler{
		l: s.logger.With(klog.String("service", "sensu")),
	}
}

func (s *Service) NewSNMPTrapHandler() *SNMPTrapHandler {
	return &SNMPTrapHandler{
		l: s.logger.With(klog.String("service", "snmp")),
	}
}

func (s *Service) NewTelegramHandler() *TelegramHandler {
	return &TelegramHandler{
		l: s.logger.With(klog.String("service", "telegram")),
	}
}

func (s *Service) NewMQTTHandler() *MQTTHandler {
	return &MQTTHandler{
		l: s.logger.With(klog.String("service", "mqtt")),
	}
}

func (s *Service) NewTalkHandler() *TalkHandler {
	return &TalkHandler{
		l: s.logger.With(klog.String("service", "talk")),
	}
}

func (s *Service) NewConfigOverrideHandler() *ConfigOverrideHandler {
	return &ConfigOverrideHandler{
		l: s.logger.With(klog.String("service", "config-override")),
	}
}

func (s *Service) NewServerHandler() *ServerHandler {
	return &ServerHandler{
		l: s.logger.With(klog.String("source", "srv")),
	}
}

func (s *Service) NewReplayHandler() *ReplayHandler {
	return &ReplayHandler{
		l: s.logger.With(klog.String("service", "replay")),
	}
}

func (s *Service) NewK8sHandler() *K8sHandler {
	return &K8sHandler{
		l: s.logger.With(klog.String("service", "kubernetes")),
	}
}

func (s *Service) NewSwarmHandler() *SwarmHandler {
	return &SwarmHandler{
		l: s.logger.With(klog.String("service", "swarm")),
	}
}

func (s *Service) NewDeadmanHandler() *DeadmanHandler {
	return &DeadmanHandler{
		l: s.logger.With(klog.String("service", "deadman")),
	}
}

func (s *Service) NewNoAuthHandler() *NoAuthHandler {
	return &NoAuthHandler{
		l: s.logger.With(klog.String("service", "noauth")),
	}
}

func (s *Service) NewStatsHandler() *StatsHandler {
	return &StatsHandler{
		l: s.logger.With(klog.String("service", "stats")),
	}
}

func (s *Service) NewUDPHandler() *UDPHandler {
	return &UDPHandler{
		l: s.logger.With(klog.String("service", "udp")),
	}
}

func (s *Service) NewInfluxDBHandler() *InfluxDBHandler {
	return &InfluxDBHandler{
		l: s.logger.With(klog.String("service", "influxdb")),
	}
}

func (s *Service) NewScraperHandler() *ScraperHandler {
	return &ScraperHandler{
		l:   s.logger.With(klog.String("service", "scraper")),
		buf: bytes.NewBuffer(nil),
	}
}

func (s *Service) NewAzureHandler() *ScraperHandler {
	return &ScraperHandler{
		l:   s.logger.With(klog.String("service", "azure")),
		buf: bytes.NewBuffer(nil),
	}
}

func (s *Service) NewConsulHandler() *ScraperHandler {
	return &ScraperHandler{
		l:   s.logger.With(klog.String("service", "consul")),
		buf: bytes.NewBuffer(nil),
	}
}

func (s *Service) NewDNSHandler() *ScraperHandler {
	return &ScraperHandler{
		l:   s.logger.With(klog.String("service", "dns")),
		buf: bytes.NewBuffer(nil),
	}
}

func (s *Service) NewEC2Handler() *ScraperHandler {
	return &ScraperHandler{
		l:   s.logger.With(klog.String("service", "ec2")),
		buf: bytes.NewBuffer(nil),
	}
}

func (s *Service) NewFileDiscoveryHandler() *ScraperHandler {
	return &ScraperHandler{
		l:   s.logger.With(klog.String("service", "file-discovery")),
		buf: bytes.NewBuffer(nil),
	}
}

func (s *Service) NewGCEHandler() *ScraperHandler {
	return &ScraperHandler{
		l:   s.logger.With(klog.String("service", "gce")),
		buf: bytes.NewBuffer(nil),
	}
}

func (s *Service) NewMarathonHandler() *ScraperHandler {
	return &ScraperHandler{
		l:   s.logger.With(klog.String("service", "marathon")),
		buf: bytes.NewBuffer(nil),
	}
}

func (s *Service) NewNerveHandler() *ScraperHandler {
	return &ScraperHandler{
		l:   s.logger.With(klog.String("service", "nerve")),
		buf: bytes.NewBuffer(nil),
	}
}

func (s *Service) NewServersetHandler() *ScraperHandler {
	return &ScraperHandler{
		l:   s.logger.With(klog.String("service", "serverset")),
		buf: bytes.NewBuffer(nil),
	}
}

func (s *Service) NewStaticDiscoveryHandler() *ScraperHandler {
	return &ScraperHandler{
		l:   s.logger.With(klog.String("service", "static-discovery")),
		buf: bytes.NewBuffer(nil),
	}
}

func (s *Service) NewTritonHandler() *ScraperHandler {
	return &ScraperHandler{
		l:   s.logger.With(klog.String("service", "triton")),
		buf: bytes.NewBuffer(nil),
	}
}

func (s *Service) NewStaticLevelHandler(level string, service string) (*StaticLevelHandler, error) {
	var ll logLevel

	switch level {
	case "debug":
		ll = llDebug
	case "error":
		ll = llError
	case "info":
		ll = llInfo
	case "warn":
		ll = llWarn
	default:
		ll = llInvalid
	}

	if ll == llInvalid {
		return nil, errors.New("invalid log level")
	}

	return &StaticLevelHandler{
		l:     s.logger.With(klog.String("service", service)),
		level: ll,
	}, nil
}

func (s *Service) NewCmdHandler() *CmdHandler {
	return &CmdHandler{
		l: s.logger.With(klog.String("service", "run")),
	}
}

func (s *Service) NewLoadHandler() *LoadHandler {
	return &LoadHandler{
		l: s.logger.With(klog.String("service", "load")),
	}
}
