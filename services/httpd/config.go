package httpd

import (
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/influxdata/influxdb/toml"
	"github.com/pkg/errors"
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
	SharedSecret     string        `toml:"shared-secret"`

	// Enable gzipped encoding
	// NOTE: this is ignored in toml since it is only consumed by the tests
	GZIP bool `toml:"-"`
}

func NewConfig() Config {
	return Config{
		BindAddress:      ":9092",
		LogEnabled:       true,
		HttpsCertificate: "/etc/ssl/kapacitor.pem",
		ShutdownTimeout:  DefaultShutdownTimeout,
		GZIP:             true,
	}
}

func (c Config) Validate() error {
	_, port, err := net.SplitHostPort(c.BindAddress)
	if err != nil {
		return errors.Wrapf(err, "invalid http bind address %s", c.BindAddress)
	}
	if port == "" {
		return errors.Wrapf(err, "invalid http bind address, no port specified %s", c.BindAddress)
	}
	if pn, err := strconv.ParseInt(port, 10, 64); err != nil {
		return errors.Wrapf(err, "invalid http bind address port %s", port)
	} else if pn > 65535 || pn < 0 {
		return fmt.Errorf("invalid http bind address port %d: out of range", pn)
	}

	return nil
}

// Determine HTTP port from BindAddress.
func (c Config) Port() (int, error) {
	if err := c.Validate(); err != nil {
		return -1, err
	}
	// Ignore errors since we already validated
	_, portStr, _ := net.SplitHostPort(c.BindAddress)
	port, _ := strconv.ParseInt(portStr, 10, 64)
	return int(port), nil
}
