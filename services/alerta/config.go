package alerta

type Config struct {
	// Whether Alerta integration is enabled.
	Enabled bool `toml:"enabled"`
	// The Alerta URL.
	URL string `toml:"url"`
	// The authentication token for this notification, can be overridden per alert.
	Token string `toml:"token"`
	// The environment in which to raise the alert.
	Environment string `toml:"environment"`
	// The origin of the alert.
	Origin string `toml:"origin"`
}

func NewConfig() Config {
	return Config{}
}
