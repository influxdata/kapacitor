package mqtt

type Config struct {
	// Enabled indicates whether the service should be enabled
	Enabled bool `toml:"enabled" override:"enabled"`
	// URL of the MQTT Broker
	Host string `toml:"url" override:"host"`
	// Port of the MQTT Broker
	Port string `toml:"port" override:"port"`
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
