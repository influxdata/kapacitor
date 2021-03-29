package auth

import (
	"fmt"
	"time"

	"github.com/influxdata/influxdb/toml"
	"golang.org/x/crypto/bcrypt"
)

const (
	// Default cost is 10
	DefaultBcryptCost      = bcrypt.DefaultCost
	DefaultCacheExpiration = 10 * time.Minute
)

type Config struct {
	Enabled                bool          `toml:"enabled"`
	CacheExpiration        toml.Duration `toml:"cache-expiration"`
	BcryptCost             int           `toml:"bcrypt-cost"`
	MetaAddr               string        `toml:"meta-addr"`
	MetaUsername           string        `toml:"meta-username"`
	MetaPassword           string        `toml:"meta-password"`
	MetaUseTLS             bool          `toml:"meta-use-tls"`
	MetaCA                 string        `toml:"meta-ca"`
	MetaCert               string        `toml:"meta-cert"`
	MetaKey                string        `toml:"meta-key"`
	MetaInsecureSkipVerify bool          `toml:"meta-insecure-skip-verify"`
}

func NewDisabledConfig() Config {
	return Config{
		Enabled: false,
	}
}

func NewEnabledConfig() Config {
	return Config{
		Enabled:         true,
		CacheExpiration: toml.Duration(DefaultCacheExpiration),
		BcryptCost:      DefaultBcryptCost,
	}
}

func (c Config) Validate() error {
	if !c.Enabled {
		return nil
	}
	if c.BcryptCost < bcrypt.MinCost {
		return fmt.Errorf("must provide a bcrypt cost >= %d, got %d", bcrypt.MinCost, c.BcryptCost)
	}
	return nil
}
