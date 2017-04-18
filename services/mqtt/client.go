package mqtt

import (
	"errors"
	"log"
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"
)

// Client describes an immutable MQTT client, designed to accommodate the
// incongruencies between real clients and mock clients.
type Client interface {
	Connect() error
	Disconnect()
	Publish(string, byte, bool, string) error
}

// NewClient produces a disconnected MQTT client
var NewClient = func(c Config) Client {
	return &PahoClient{
		host:     c.Broker(),
		port:     c.Port,
		username: c.Username,
		password: c.Password,
		clientID: c.ClientID,
	}
}

type PahoClient struct {
	host string
	port uint16

	username string
	password string

	clientID string

	client pahomqtt.Client
}

var _ Client = &PahoClient{}

// DefaultQuiesceTimeout is the duration the client will wait for outstanding
// messages to be published before forcing a disconnection
const DefaultQuiesceTimeout time.Duration = 250 * time.Millisecond

func (p *PahoClient) Connect() error {
	log.Printf("Current config: %#v\n", p)
	opts := pahomqtt.NewClientOptions()
	opts.AddBroker(p.host)
	opts.SetClientID(p.clientID)
	opts.SetUsername(p.username)
	opts.SetPassword(p.password)

	// Using a clean session forces the broker to dispose of client session
	// information after disconnecting. Retention of this is useful for
	// constrained clients.  Since Kapacitor is only publishing, it has no
	// storage requirements and can reduce load on the broker by using a clean
	// session.
	opts.SetCleanSession(true)

	p.client = pahomqtt.NewClient(opts)
	token := p.client.Connect()
	log.Printf("Current config: %#v\n", p)

	token.Wait() // Tokens are futures

	return token.Error()
}

func (p *PahoClient) Disconnect() {
	if p.client != nil {
		p.client.Disconnect(uint(DefaultQuiesceTimeout / time.Millisecond))
	}
}

var foo int = 1

func (p *PahoClient) Publish(topic string, qos byte, retained bool, message string) error {
	p.client.Publish(topic, qos, retained, message)
	return nil
}

var _ Client = &MockClient{}

type MockClient struct {
	connected bool
}

func (m *MockClient) Connect() error {
	m.connected = true
	return nil
}

func (m *MockClient) Disconnect() {
	m.connected = false
}

func (m *MockClient) Publish(topic string, qos byte, retained bool, message string) error {
	if !m.connected {
		return errors.New("Publish() called before Connect()")
	}
	return nil
}
