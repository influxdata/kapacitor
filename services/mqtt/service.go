package mqtt

import (
	"log"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"
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

	Client *pahomqtt.Client
}

func NewService(c Config, l *log.Logger) *Service {
	return &Service{}
}

// TODO(timraymond): improve logging here and in Close
func (s *Service) Open() error {
	opts := pahomqtt.NewClientOptions()
	opts.AddBroker(s.Config.Broker)
	opts.SetClientID(s.Config.ClientID) // TODO(timraymond): should we provide a random one?
	opts.SetUsername(s.Config.Username)
	opts.SetPassword(s.Config.Password)
	opts.SetCleanSession(false) // wtf is this? Why does it default to false?

	s.Client = pahomqtt.NewClient(opts)
	s.Token = s.Client.Connect()

	token.Wait()

	if err := token.Error(); err != nil {
		log.Println("E! MQTT done broke yo") //TODO(timraymond): put a legit error in
	}
}

func (s *Service) Close() error {
	s.Client.Disconnect(250) // what is this code?
	log.Println("I! MQTT Client Disconnected")
}

func (s *Service) Alert(topic, message string, qos QOSLevel) error {
	s.Client.Publish(topic, byte(qos), false, message) // should retained be configureable?
}

func (s *Service) Update(newConfig []interface{}) error {
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
	if err := h.s.Alert(h.c.Topic, h.c.QOS, event.State.Message); err != nil {
		h.logger.Println("E! failed to post message to MQTT broker", err)
	}
}

func (s *Service) DefaultHandlerConfig() HandlerConfig {
	return HandlerConfig{
		Topic: "Barnacles",
		QOS:   LEVEL2,
	}
}
