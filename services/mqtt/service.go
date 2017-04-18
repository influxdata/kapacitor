package mqtt

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	"github.com/influxdata/kapacitor/alert"
)

// QoSLevel indicates the quality of service for messages delivered to a
// broker.
type QoSLevel byte

var (
	ErrInvalidQoS = errors.New("invalid QoS specified. Valid options are \"AtMostOnce\", \"AtLeastOnce\", \"ExactlyOnce\"")
)

func (q *QoSLevel) UnmarshalText(text []byte) error {
	switch string(text) {
	case "AtMostOnce":
		*q = AtMostOnce
	case "AtLeastOnce":
		*q = AtLeastOnce
	case "ExactlyOnce":
		*q = ExactlyOnce
	default:
		return ErrInvalidQoS
	}
	return nil
}

func (q *QoSLevel) MarshalText() (text []byte, err error) {
	switch *q {
	case AtMostOnce:
		return []byte("AtMostOnce"), nil
	case AtLeastOnce:
		return []byte("AtLeastOnce"), nil
	case ExactlyOnce:
		return []byte("ExactlyOnce"), nil
	default:
		return []byte{}, ErrInvalidQoS
	}
}

const (
	// best effort delivery. "fire and forget"
	AtMostOnce QoSLevel = iota
	// guarantees delivery to at least one receiver. May deliver multiple times.
	AtLeastOnce
	// guarantees delivery only once. Safest and slowest.
	ExactlyOnce
)

type Service struct {
	logger *log.Logger

	configValue atomic.Value

	mu     sync.RWMutex
	client Client
}

func NewService(c Config, l *log.Logger) *Service {
	s := &Service{
		logger: l,
	}

	cl := NewClient(c)

	s.configValue.Store(c)
	s.client = cl
	return s
}

func (s *Service) config() Config {
	return s.configValue.Load().(Config)
}

func (s *Service) Open() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.logger.Println("I! Starting MQTT service")
	return s.client.Connect()
}

func (s *Service) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.logger.Println("I! Stopping MQTT service")
	s.client.Disconnect()
	return nil
}

func (s *Service) Alert(qos QoSLevel, retained bool, topic, message string) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.client.Publish(topic, byte(qos), retained, message)
}

func (s *Service) Update(newConfig []interface{}) error {
	if l := len(newConfig); l != 1 {
		return fmt.Errorf("expected only one new config object, got %d", l)
	}
	if c, ok := newConfig[0].(Config); !ok {
		return fmt.Errorf("expected config object to be of type %T, got %T", c, newConfig[0])
	} else {
		s.configValue.Store(c)

		go func() {
			// lock out concurrent publishers
			s.mu.Lock()
			defer s.mu.Unlock()

			c := s.config()
			s.client.Disconnect()
			cl := NewClient(c)
			if err := cl.Connect(); err != nil {
				s.logger.Println("E! Error updating MQTT config. Using previous configuration: err:", err)
				return
			}
			s.client = cl
		}()
	}
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
	Topic    string
	QoS      QoSLevel
	Retained bool
}

type handler struct {
	s      *Service
	c      HandlerConfig
	logger *log.Logger
}

func (h *handler) Handle(event alert.Event) {
	if err := h.s.Alert(h.c.QoS, h.c.Retained, h.c.Topic, event.State.Message); err != nil {
		h.logger.Println("E! failed to post message to MQTT broker", err)
	}
}

func (s *Service) DefaultHandlerConfig() HandlerConfig {
	c := s.config()
	return HandlerConfig{
		Topic:    c.DefaultTopic,
		QoS:      c.DefaultQoS,
		Retained: c.DefaultRetained,
	}
}

type testOptions struct {
	Topic    string   `json:"topic"`
	Message  string   `json:"message"`
	QoS      QoSLevel `json:"qos"`
	Retained bool     `json:"retained"`
}

func (s *Service) TestOptions() interface{} {
	c := s.config()
	return &testOptions{
		Topic:   c.DefaultTopic,
		QoS:     c.DefaultQoS,
		Message: "test MQTT message",
	}
}

func (s *Service) Test(o interface{}) error {
	options, ok := o.(*testOptions)
	if !ok {
		return fmt.Errorf("unexpected options type %T", options)
	}
	return s.Alert(options.QoS, options.Retained, options.Topic, options.Message)
}
