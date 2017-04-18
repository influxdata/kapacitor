package mqtt

import (
	"errors"
	"fmt"
)

type Config struct {
	// Enabled indicates whether the service should be enabled
	Enabled bool `toml:"enabled" override:"enabled"`
	// URL of the MQTT Broker
	Host string `toml:"host" override:"host"`
	// Port of the MQTT Broker
	Port uint16 `toml:"port" override:"port"`

	ClientID string `toml:"client_id" override:"client_id"`
	Username string `toml:"username" override:"username"`
	Password string `toml:"password" override:"password"`
}

// Broker formats the configured Host and Port as tcp://host:port, suitable for
// consumption by the Paho MQTT Client
func (c Config) Broker() string {
	return fmt.Sprintf("tcp://%s:%d", c.Host, c.Port)
}

func NewConfig() Config {
	return Config{}
}

func (c Config) Validate() error {
	if c.Enabled && (c.Host == "" || c.Port == 0) {
		return errors.New("must specify host and port for mqtt service")
	}
	return nil
}
