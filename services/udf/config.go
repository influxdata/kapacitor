package udf

import (
	"fmt"
	"time"

	"github.com/influxdata/config"
)

type Config struct {
	Functions map[string]FunctionConfig `toml:"functions"`
}

type FunctionConfig struct {
	Prog    string            `toml:"prog"`
	Args    []string          `toml:"args"`
	Timeout config.Duration   `toml:"timeout"`
	Env     map[string]string `toml:"env"`
}

func NewConfig() Config {
	return Config{}
}

func (c Config) Validate() error {
	for name, fc := range c.Functions {
		if time.Duration(fc.Timeout) <= time.Millisecond {
			return fmt.Errorf("timeout for %s is too small: %s", name, fc.Timeout)
		}
	}
	return nil
}
