package mqtt

import (
	"log"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/influxdata/kapacitor/alert"
)

type QOSLevel byte

const (
	LEVEL0 QOSLevel = iota
	LEVEL1
	LEVEL2
)

type Service struct {
	Config Config
	Logger *log.Logger

	client pahomqtt.Client
	token  pahomqtt.Token
}

func NewService(c Config, l *log.Logger) *Service {
	return &Service{
		Config: c,
		Logger: l,
	}
}

// TODO(timraymond): improve logging here and in Close
func (s *Service) Open() error {
	opts := pahomqtt.NewClientOptions()
	opts.AddBroker(s.Config.Broker())
	opts.SetClientID(s.Config.ClientID) // TODO(timraymond): should we provide a random one?
	opts.SetUsername(s.Config.Username)
	opts.SetPassword(s.Config.Password)
	opts.SetCleanSession(false) // wtf is this? Why does it default to false?

	s.client = pahomqtt.NewClient(opts)
	s.token = s.client.Connect()

	s.token.Wait()

	if err := s.token.Error(); err != nil {
		s.Logger.Println("E! Error connecting to MQTT broker at", s.Config.Broker(), "err:", err) //TODO(timraymond): put a legit error in
		return err
	}
	s.Logger.Println("I! Connected to MQTT Broker at", s.Config.Broker())
	return nil

}

func (s *Service) Close() error {
	s.client.Disconnect(250) // what is this code?
	s.Logger.Println("I! MQTT Client Disconnected")
	return nil
}

func (s *Service) Alert(qos QOSLevel, topic, message string) error {
	s.client.Publish(topic, byte(qos), false, message) // should retained be configureable?
	return nil
}

func (s *Service) Update(newConfig []interface{}) error {
	return nil
}

func (s *Service) Handler(c HandlerConfig, l *log.Logger) alert.Handler {
	return &handler{
		s:      s,
		c:      c,
		logger: l,
	}
}

type HandlerConfig struct {
	Topic string
	QOS   QOSLevel
}

type handler struct {
	s      *Service
	c      HandlerConfig
	logger *log.Logger
}

func (h *handler) Handle(event alert.Event) {
	if err := h.s.Alert(h.c.QOS, h.c.Topic, event.State.Message); err != nil {
		h.logger.Println("E! failed to post message to MQTT broker", err)
	}
}

func (s *Service) DefaultHandlerConfig() HandlerConfig {
	return HandlerConfig{
		Topic: "Barnacles",
		QOS:   LEVEL2,
	}
}

type testOptions struct{}

func (s *Service) TestOptions() interface{} {
	return "foo"
}

func (s *Service) Test(o interface{}) error {
	// stubbed out for POC
	return nil
}
