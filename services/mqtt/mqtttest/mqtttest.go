package mqtttest

import (
	"errors"

	"github.com/influxdata/kapacitor/services/mqtt"
)

// ClientCreator provides a NewClient method for creating new MockClients.
// All configs and clients created are recorded.
type ClientCreator struct {
	Clients []*MockClient
	Configs []mqtt.Config
}

func (s *ClientCreator) NewClient(c mqtt.Config) (mqtt.Client, error) {
	cli := new(MockClient)
	s.Clients = append(s.Clients, cli)
	s.Configs = append(s.Configs, c)
	return cli, nil
}

type MockClient struct {
	connected bool

	PublishData []PublishData
}

func NewClient(mqtt.Config) (mqtt.Client, error) {
	return new(MockClient), nil
}

func (m *MockClient) Connect() error {
	m.connected = true
	return nil
}

func (m *MockClient) Disconnect() {
	m.connected = false
}

func (m *MockClient) Publish(topic string, qos mqtt.QoSLevel, retained bool, message []byte) error {
	if !m.connected {
		return errors.New("Publish() called before Connect()")
	}
	m.PublishData = append(m.PublishData, PublishData{
		Topic:    topic,
		QoS:      qos,
		Retained: retained,
		Message:  message,
	})
	return nil
}

type PublishData struct {
	Topic    string
	QoS      mqtt.QoSLevel
	Retained bool
	Message  []byte
}
