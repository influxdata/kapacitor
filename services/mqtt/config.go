package mqtt

import (
	"errors"
	"net"
	"net/url"
	"strconv"
)

type Config struct {
	// Enabled indicates whether the service should be enabled
	Enabled bool `toml:"enabled" override:"enabled"`
	// URL of the MQTT Broker
	Host string `toml:"host" override:"host"`
	// Port of the MQTT Broker
	Port uint16 `toml:"port" override:"port"`

	ClientID string `toml:"client-id" override:"client-id"`
	Username string `toml:"username" override:"username"`
	Password string `toml:"password" override:"password,redact"`

	DefaultTopic    string   `toml:"default-topic" override:"default-topic"`
	DefaultQoS      QoSLevel `toml:"default-qos" override:"default-qos"`
	DefaultRetained bool     `toml:"retained" override:"retained"`
}

// Broker formats the configured Host and Port as tcp://host:port, suitable for
// consumption by the Paho MQTT Client
func (c Config) Broker() string {
	portStr := strconv.FormatUint(uint64(c.Port), 10)
	u := &url.URL{
		Scheme: "tcp",
		Host:   net.JoinHostPort(c.Host, portStr),
	}
	return u.String()
}

func NewConfig() Config {
	return Config{}
}

func (c Config) Validate() error {
	if c.Enabled {
		if c.Host == "" || c.Port == 0 {
			return errors.New("must specify host and port for mqtt service")
		}

		if c.DefaultTopic == "" {
			return errors.New("must specify default MQTT topic")
		}
	}
	return nil
}
