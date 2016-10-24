package smtp

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/influxdata/influxdb/toml"
)

type Config struct {
	Enabled  bool   `toml:"enabled" override:"enabled"`
	Host     string `toml:"host" override:"host"`
	Port     int    `toml:"port" override:"port"`
	Username string `toml:"username" override:"username"`
	Password string `toml:"password" override:"password,redact"`
	// Whether to skip TLS verify.
	NoVerify bool `toml:"no-verify" override:"no-verify"`
	// Whether all alerts should trigger an email.
	Global bool `toml:"global" override:"global"`
	// Whether all alerts should automatically use stateChangesOnly mode.
	// Only applies if global is also set.
	StateChangesOnly bool `toml:"state-changes-only" override:"state-changes-only"`
	// From address
	From string `toml:"from" override:"from"`
	// Default To addresses
	To []string `toml:"to" override:"to"`
	// Close connection to SMTP server after idle timeout has elapsed
	IdleTimeout toml.Duration `toml:"idle-timeout" override:"idle-timeout"`
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
	if c.Enabled && c.From == "" {
		return errors.New("must provide a 'from' address")
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
