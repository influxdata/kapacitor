package mqtt

import "errors"

type Config struct {
	// Enabled indicates whether the service should be enabled
	Enabled bool `toml:"enabled" override:"enabled"`
	// URL of the MQTT Broker
	Host string `toml:"url" override:"host"`
	// Port of the MQTT Broker
	Port string `toml:"port" override:"port"`

	ClientID string `toml:"client_id" override:"client_id"`
	Username string `toml:"username" override:"username"`
	Password string `toml:"password" override:"password"`
}

// Broker formats the configured Host and Port as tcp://host:port, suitable for
// consumption by the Paho MQTT Client
func (c Config) Broker() string {
	return "tcp://" + c.Host + ":" + c.Port
}

func NewConfig() Config {
	return Config{}
}

func (c Config) Validate() error {
	if c.Enabled && (c.Host == "" || c.Port == "") {
		return errors.New("must specify host and port for mqtt service")
	}
	return nil
}
