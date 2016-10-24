package config

type Config struct {
	Enabled bool `toml:"enabled"`
}

func NewConfig() Config {
	return Config{
		Enabled: true,
	}
}
