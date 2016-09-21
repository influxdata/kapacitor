package smtp

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/influxdata/influxdb/toml"
)

type Config struct {
	Enabled  bool   `toml:"enabled"`
	Host     string `toml:"host"`
	Port     int    `toml:"port"`
	Username string `toml:"username"`
	Password string `toml:"password" override:",redact"`
	// Whether to skip TLS verify.
	NoVerify bool `toml:"no-verify"`
	// Whether all alerts should trigger an email.
	Global bool `toml:"global"`
	// Whether all alerts should automatically use stateChangesOnly mode.
	// Only applies if global is also set.
	StateChangesOnly bool `toml:"state-changes-only"`
	// From address
	From string `toml:"from"`
	// Default To addresses
	To []string `toml:"to"`
	// Close connection to SMTP server after idle timeout has elapsed
	IdleTimeout toml.Duration `toml:"idle-timeout"`
}

func NewConfig() Config {
	return Config{
		Host:        "localhost",
		Port:        25,
		IdleTimeout: toml.Duration(time.Second * 30),
	}
}

func (c Config) Validate() error {
	if c.Host == "" {
		return errors.New("host cannot be empty")
	}
	if c.Port <= 0 {
		return fmt.Errorf("invalid port %d", c.Port)
	}
	if c.IdleTimeout < 0 {
		return errors.New("idle timeout must be positive")
	}
	// Poor mans email validation, but since emails have a very large domain this is probably good enough
	// to catch user error.
	if c.From != "" && !strings.ContainsRune(c.From, '@') {
		return fmt.Errorf("invalid from email address: %q", c.From)
	}
	for _, t := range c.To {
		if !strings.ContainsRune(t, '@') {
			return fmt.Errorf("invalid to email address: %q", t)
		}
	}
	return nil
}
