package mqtt

import (
	"fmt"
	"log"
	"sync"

	"github.com/influxdata/kapacitor/alert"
	"github.com/influxdata/kapacitor/keyvalue"
	"github.com/pkg/errors"
)

type Diagnostic interface {
	WithContext(ctx ...keyvalue.T) Diagnostic
	Error(msg string, err error)
	CreatingAlertHandler(c HandlerConfig)
	HandlingEvent()
}

// QoSLevel indicates the quality of service for messages delivered to a
// broker.
type QoSLevel byte

var (
	ErrInvalidQoS = errors.New("invalid QoS")
)

func (q *QoSLevel) UnmarshalText(text []byte) error {
	switch string(text) {
	case "at-most-once":
		*q = AtMostOnce
	case "at-least-once":
		*q = AtLeastOnce
	case "exactly-one":
		*q = ExactlyOnce
	default:
		return ErrInvalidQoS
	}
	return nil
}

func (q QoSLevel) MarshalText() (text []byte, err error) {
	switch q {
	case AtMostOnce:
		return []byte("at-most-once"), nil
	case AtLeastOnce:
		return []byte("at-least-once"), nil
	case ExactlyOnce:
		return []byte("exactly-once"), nil
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
	diag Diagnostic

	mu      sync.RWMutex
	clients map[string]Client
	configs map[string]Config

	defaultBrokerName string
}

func NewService(cs Configs, d Diagnostic) (*Service, error) {
	configs := cs.index()
	clients := make(map[string]Client, len(cs))

	var defaultBrokerName string
	for name, c := range configs {
		if c.Enabled {
			cli, err := c.NewClient()
			if err != nil {
				return nil, err
			}
			clients[name] = cli
		}
		if c.Default {
			defaultBrokerName = c.Name
		}
	}
	if len(cs) == 1 {
		defaultBrokerName = cs[0].Name
	}

	return &Service{
		diag:              d,
		configs:           configs,
		clients:           clients,
		defaultBrokerName: defaultBrokerName,
	}, nil
}

func (s *Service) Open() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for name, client := range s.clients {
		if client == nil {
			return fmt.Errorf("no client found for MQTT broker %q", name)
		}
		if err := client.Connect(); err != nil {
			return errors.Wrapf(err, "failed to connect to MQTT broker %q", name)
		}
	}
	return nil
}

func (s *Service) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, client := range s.clients {
		if client != nil {
			client.Disconnect()
		}
	}
	return nil
}

func (s *Service) Alert(brokerName, topic string, qos QoSLevel, retained bool, message string) error {
	log.Println("D! ALERT", topic, message)
	s.mu.RLock()
	defer s.mu.RUnlock()
	if topic == "" {
		return fmt.Errorf("missing MQTT topic")
	}
	if brokerName == "" {
		brokerName = s.defaultBrokerName
	}
	client := s.clients[brokerName]
	if client == nil {
		return fmt.Errorf("unknown MQTT broker %q", brokerName)
	}
	return client.Publish(topic, qos, retained, []byte(message))
}

func (s *Service) Update(newConfigs []interface{}) error {
	cs := make(Configs, len(newConfigs))
	for i, c := range newConfigs {
		config, ok := c.(Config)
		if !ok {
			return fmt.Errorf("expected config object to be of type %T, got %T", config, c)
		}
		cs[i] = config
	}
	return s.update(cs)
}

func (s *Service) update(cs Configs) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := cs.Validate(); err != nil {
		return err
	}

	configs := cs.index()
	for name, c := range configs {
		if c.Default {
			s.defaultBrokerName = name
		}
		old, ok := s.configs[name]
		if ok && old.Equal(c) {
			continue
		}
		client := s.clients[name]

		if client != nil {
			client.Disconnect()
		}
		s.clients[name] = nil

		if c.Enabled {
			client, err := c.NewClient()
			if err != nil {
				return err
			}

			if err := client.Connect(); err != nil {
				return err
			}
			s.clients[name] = client
		}
	}
	if len(cs) == 1 {
		s.defaultBrokerName = cs[0].Name
	}

	// Disconnect and remove old clients
	for name := range s.configs {
		if _, ok := configs[name]; !ok {
			client := s.clients[name]
			if client != nil {
				client.Disconnect()
			}
			delete(s.clients, name)
		}
	}
	s.configs = configs
	return nil
}

func (s *Service) Handler(c HandlerConfig, ctx ...keyvalue.T) alert.Handler {
	d := s.diag.WithContext(ctx...)
	d.CreatingAlertHandler(c)
	return &handler{
		s:    s,
		c:    c,
		diag: d,
	}
}

type HandlerConfig struct {
	BrokerName string   `mapstructure:"broker-name"`
	Topic      string   `mapstructure:"topic"`
	QoS        QoSLevel `mapstructure:"qos"`
	Retained   bool     `mapstructure:"retained"`
}

type handler struct {
	s    *Service
	c    HandlerConfig
	diag Diagnostic
}

func (h *handler) Handle(event alert.Event) {
	h.diag.HandlingEvent()
	if err := h.s.Alert(h.c.BrokerName, h.c.Topic, h.c.QoS, h.c.Retained, event.State.Message); err != nil {
		h.diag.Error("failed to post message to MQTT broker", err)
	}
}

type testOptions struct {
	BrokerName string   `json:"broker-name"`
	Topic      string   `json:"topic"`
	Message    string   `json:"message"`
	QoS        QoSLevel `json:"qos"`
	Retained   bool     `json:"retained"`
}

func (s *Service) TestOptions() interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return &testOptions{
		BrokerName: s.defaultBrokerName,
		Message:    "test MQTT message",
	}
}

func (s *Service) Test(o interface{}) error {
	options, ok := o.(*testOptions)
	if !ok {
		return fmt.Errorf("unexpected options type %T", options)
	}
	return s.Alert(options.BrokerName, options.Topic, options.QoS, options.Retained, options.Message)
}
