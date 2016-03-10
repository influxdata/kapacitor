package httpd

import (
	"time"

	"github.com/influxdb/influxdb/toml"
)

const (
	DefaultShutdownTimeout = toml.Duration(time.Second * 10)
)

type Config struct {
	BindAddress      string        `toml:"bind-address"`
	AuthEnabled      bool          `toml:"auth-enabled"`
	LogEnabled       bool          `toml:"log-enabled"`
	WriteTracing     bool          `toml:"write-tracing"`
	PprofEnabled     bool          `toml:"pprof-enabled"`
	HttpsEnabled     bool          `toml:"https-enabled"`
	HttpsCertificate string        `toml:"https-certificate"`
	ShutdownTimeout  toml.Duration `toml:"shutdown-timeout"`
}

func NewConfig() Config {
	return Config{
		BindAddress:      ":9092",
		LogEnabled:       true,
		HttpsCertificate: "/etc/ssl/kapacitor.pem",
		ShutdownTimeout:  DefaultShutdownTimeout,
	}
}
