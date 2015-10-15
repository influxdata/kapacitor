package smtp

import (
	"time"

	"github.com/influxdb/influxdb/toml"
)

type Config struct {
	Enabled  bool   `toml:"enabled"`
	Host     string `toml:"host"`
	Port     int    `toml:"port"`
	Username string `toml:"username"`
	Password string `toml:"password"`
	// Close connection to SMTP server after idle timeout has elapsed
	IdleTimeout toml.Duration `toml:"idle-timeout"`
}

func NewConfig() Config {
	return Config{
		Host:        "localhost",
		Port:        25,
		Username:    "",
		Password:    "",
		IdleTimeout: toml.Duration(time.Second * 30),
	}
}
