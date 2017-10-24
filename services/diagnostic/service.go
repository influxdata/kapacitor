package diagnostic

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path"
	"strings"
	"sync"
)

type nopCloser struct {
	f io.Writer
}

func (c *nopCloser) Write(b []byte) (int, error) { return c.f.Write(b) }
func (c *nopCloser) Close() error                { return nil }

type Service struct {
	c Config

	logger Logger

	f      io.WriteCloser
	stdout io.Writer
	stderr io.Writer

	SessionService *SessionService

	levelMu sync.RWMutex
	level   string
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
	s.levelMu.Lock()
	defer s.levelMu.Unlock()
	level := strings.ToUpper(lvl)
	switch level {
	case "INFO", "ERROR", "WARN", "DEBUG":
		s.level = level
	default:
		return errors.New("invalid log level")
	}

	return nil
}

func logLevelFromName(lvl string) Level {
	var level Level
	switch lvl {
	case "INFO", "info":
		level = InfoLevel
	case "ERROR", "error":
		level = ErrorLevel
	case "WARN", "warn":
		level = WarnLevel
	case "DEBUG", "debug":
		level = DebugLevel
	}

	return level
}

func (s *Service) Open() error {
	s.levelMu.Lock()
	s.level = s.c.Level
	s.levelMu.Unlock()

	levelF := func(lvl Level) bool {
		s.levelMu.RLock()
		defer s.levelMu.RUnlock()
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

	l := NewServerLogger(s.f)
	l.SetLevelF(levelF)

	s.SessionService = NewSessionService()

	s.logger = NewMultiLogger(
		l,
		s.SessionService.NewLogger(),
	)

	s.SessionService.SetDiagnostic(s.NewSessionHandler())

	return nil
}

func (s *Service) Close() error {
	if s.f != nil {
		return s.f.Close()
	}
	return nil
}

func (s *Service) NewSideloadHandler() *SideloadHandler {
	return &SideloadHandler{
		l: s.logger.With(String("service", "sideload")),
	}
}

func (s *Service) NewVictorOpsHandler() *VictorOpsHandler {
	return &VictorOpsHandler{
		l: s.logger.With(String("service", "victorops")),
	}
}

func (s *Service) NewSlackHandler() *SlackHandler {
	return &SlackHandler{
		l: s.logger.With(String("service", "slack")),
	}
}

func (s *Service) NewTaskStoreHandler() *TaskStoreHandler {
	return &TaskStoreHandler{
		l: s.logger.With(String("service", "task_store")),
	}
}

func (s *Service) NewReportingHandler() *ReportingHandler {
	return &ReportingHandler{
		l: s.logger.With(String("service", "reporting")),
	}
}

func (s *Service) NewStorageHandler() *StorageHandler {
	return &StorageHandler{
		l: s.logger.With(String("service", "storage")),
	}
}

func (s *Service) NewHTTPDHandler() *HTTPDHandler {
	return &HTTPDHandler{
		l: s.logger.With(String("service", "http")),
	}
}

func (s *Service) NewAlertaHandler() *AlertaHandler {
	return &AlertaHandler{
		l: s.logger.With(String("service", "alerta")),
	}
}

func (s *Service) NewKapacitorHandler() *KapacitorHandler {
	return &KapacitorHandler{
		l: s.logger.With(String("service", "kapacitor")),
	}
}

func (s *Service) NewAlertServiceHandler() *AlertServiceHandler {
	return &AlertServiceHandler{
		l: s.logger.With(String("service", "alert")),
	}
}

func (s *Service) NewHipChatHandler() *HipChatHandler {
	return &HipChatHandler{
		l: s.logger.With(String("service", "hipchat")),
	}
}

func (s *Service) NewPagerDutyHandler() *PagerDutyHandler {
	return &PagerDutyHandler{
		l: s.logger.With(String("service", "pagerduty")),
	}
}

func (s *Service) NewSMTPHandler() *SMTPHandler {
	return &SMTPHandler{
		l: s.logger.With(String("service", "smtp")),
	}
}

func (s *Service) NewUDFServiceHandler() *UDFServiceHandler {
	return &UDFServiceHandler{
		l: s.logger.With(String("service", "udf")),
	}
}

func (s *Service) NewOpsGenieHandler() *OpsGenieHandler {
	return &OpsGenieHandler{
		l: s.logger.With(String("service", "opsgenie")),
	}
}

func (s *Service) NewPushoverHandler() *PushoverHandler {
	return &PushoverHandler{
		l: s.logger.With(String("service", "pushover")),
	}
}

func (s *Service) NewHTTPPostHandler() *HTTPPostHandler {
	return &HTTPPostHandler{
		l: s.logger.With(String("service", "httppost")),
	}
}

func (s *Service) NewSensuHandler() *SensuHandler {
	return &SensuHandler{
		l: s.logger.With(String("service", "sensu")),
	}
}

func (s *Service) NewSNMPTrapHandler() *SNMPTrapHandler {
	return &SNMPTrapHandler{
		l: s.logger.With(String("service", "snmp")),
	}
}

func (s *Service) NewTelegramHandler() *TelegramHandler {
	return &TelegramHandler{
		l: s.logger.With(String("service", "telegram")),
	}
}

func (s *Service) NewMQTTHandler() *MQTTHandler {
	return &MQTTHandler{
		l: s.logger.With(String("service", "mqtt")),
	}
}

func (s *Service) NewTalkHandler() *TalkHandler {
	return &TalkHandler{
		l: s.logger.With(String("service", "talk")),
	}
}

func (s *Service) NewConfigOverrideHandler() *ConfigOverrideHandler {
	return &ConfigOverrideHandler{
		l: s.logger.With(String("service", "config-override")),
	}
}

func (s *Service) NewServerHandler() *ServerHandler {
	return &ServerHandler{
		l: s.logger.With(String("source", "srv")),
	}
}

func (s *Service) NewReplayHandler() *ReplayHandler {
	return &ReplayHandler{
		l: s.logger.With(String("service", "replay")),
	}
}

func (s *Service) NewK8sHandler() *K8sHandler {
	return &K8sHandler{
		l: s.logger.With(String("service", "kubernetes")),
	}
}

func (s *Service) NewSwarmHandler() *SwarmHandler {
	return &SwarmHandler{
		l: s.logger.With(String("service", "swarm")),
	}
}

func (s *Service) NewDeadmanHandler() *DeadmanHandler {
	return &DeadmanHandler{
		l: s.logger.With(String("service", "deadman")),
	}
}

func (s *Service) NewNoAuthHandler() *NoAuthHandler {
	return &NoAuthHandler{
		l: s.logger.With(String("service", "noauth")),
	}
}

func (s *Service) NewStatsHandler() *StatsHandler {
	return &StatsHandler{
		l: s.logger.With(String("service", "stats")),
	}
}

func (s *Service) NewUDPHandler() *UDPHandler {
	return &UDPHandler{
		l: s.logger.With(String("service", "udp")),
	}
}

func (s *Service) NewInfluxDBHandler() *InfluxDBHandler {
	return &InfluxDBHandler{
		l: s.logger.With(String("service", "influxdb")),
	}
}

func (s *Service) NewScraperHandler() *ScraperHandler {
	return &ScraperHandler{
		l:   s.logger.With(String("service", "scraper")),
		buf: bytes.NewBuffer(nil),
	}
}

func (s *Service) NewAzureHandler() *ScraperHandler {
	return &ScraperHandler{
		l:   s.logger.With(String("service", "azure")),
		buf: bytes.NewBuffer(nil),
	}
}

func (s *Service) NewConsulHandler() *ScraperHandler {
	return &ScraperHandler{
		l:   s.logger.With(String("service", "consul")),
		buf: bytes.NewBuffer(nil),
	}
}

func (s *Service) NewDNSHandler() *ScraperHandler {
	return &ScraperHandler{
		l:   s.logger.With(String("service", "dns")),
		buf: bytes.NewBuffer(nil),
	}
}

func (s *Service) NewEC2Handler() *ScraperHandler {
	return &ScraperHandler{
		l:   s.logger.With(String("service", "ec2")),
		buf: bytes.NewBuffer(nil),
	}
}

func (s *Service) NewFileDiscoveryHandler() *ScraperHandler {
	return &ScraperHandler{
		l:   s.logger.With(String("service", "file-discovery")),
		buf: bytes.NewBuffer(nil),
	}
}

func (s *Service) NewGCEHandler() *ScraperHandler {
	return &ScraperHandler{
		l:   s.logger.With(String("service", "gce")),
		buf: bytes.NewBuffer(nil),
	}
}

func (s *Service) NewMarathonHandler() *ScraperHandler {
	return &ScraperHandler{
		l:   s.logger.With(String("service", "marathon")),
		buf: bytes.NewBuffer(nil),
	}
}

func (s *Service) NewNerveHandler() *ScraperHandler {
	return &ScraperHandler{
		l:   s.logger.With(String("service", "nerve")),
		buf: bytes.NewBuffer(nil),
	}
}

func (s *Service) NewServersetHandler() *ScraperHandler {
	return &ScraperHandler{
		l:   s.logger.With(String("service", "serverset")),
		buf: bytes.NewBuffer(nil),
	}
}

func (s *Service) NewStaticDiscoveryHandler() *ScraperHandler {
	return &ScraperHandler{
		l:   s.logger.With(String("service", "static-discovery")),
		buf: bytes.NewBuffer(nil),
	}
}

func (s *Service) NewTritonHandler() *ScraperHandler {
	return &ScraperHandler{
		l:   s.logger.With(String("service", "triton")),
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
		l:     s.logger.With(String("service", service)),
		level: ll,
	}, nil
}

func (s *Service) NewCmdHandler() *CmdHandler {
	return &CmdHandler{
		l: s.logger.With(String("service", "run")),
	}
}

func (s *Service) NewSessionHandler() *SessionHandler {
	return &SessionHandler{
		l: s.logger.With(String("service", "sessions")),
	}
}

func (s *Service) NewLoadHandler() *LoadHandler {
	return &LoadHandler{
		l: s.logger.With(String("service", "load")),
	}
}
