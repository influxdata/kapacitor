package mqtt

import (
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/influxdata/kapacitor/tlsconfig"
)

// Client describes an immutable MQTT client, designed to accommodate the
// incongruencies between real clients and mock clients.
type Client interface {
	Connect() error
	Disconnect()
	Publish(topic string, qos QoSLevel, retained bool, message []byte) error
}

// newClient produces a disconnected MQTT client
var newClient = func(c Config) (Client, error) {
	opts := pahomqtt.NewClientOptions()
	opts.AddBroker(c.URL)
	if c.ClientID != "" {
		opts.SetClientID(c.ClientID)
	} else {
		opts.SetClientID(c.Name)
	}
	opts.SetUsername(c.Username)
	opts.SetPassword(c.Password)

	tlsConfig, err := tlsconfig.Create(c.SSLCA, c.SSLCert, c.SSLKey, c.InsecureSkipVerify)
	if err != nil {
		return nil, err
	}
	opts.SetTLSConfig(tlsConfig)

	return &PahoClient{
		opts: opts,
	}, nil
}

type PahoClient struct {
	opts   *pahomqtt.ClientOptions
	client pahomqtt.Client
}

// DefaultQuiesceTimeout is the duration the client will wait for outstanding
// messages to be published before forcing a disconnection
const DefaultQuiesceTimeout time.Duration = 250 * time.Millisecond

func (p *PahoClient) Connect() error {
	// Using a clean session forces the broker to dispose of client session
	// information after disconnecting. Retention of this is useful for
	// constrained clients.  Since Kapacitor is only publishing, it has no
	// storage requirements and can reduce load on the broker by using a clean
	// session.
	p.opts.SetCleanSession(true)

	p.client = pahomqtt.NewClient(p.opts)
	token := p.client.Connect()
	token.Wait()
	return token.Error()
}

func (p *PahoClient) Disconnect() {
	if p.client != nil {
		p.client.Disconnect(uint(DefaultQuiesceTimeout / time.Millisecond))
	}
}

func (p *PahoClient) Publish(topic string, qos QoSLevel, retained bool, message []byte) error {
	token := p.client.Publish(topic, byte(qos), retained, message)
	token.Wait()
	return token.Error()
}
