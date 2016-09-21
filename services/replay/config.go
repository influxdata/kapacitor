package replay

import (
	"fmt"
)

const (
	DefaultDir = "./replay"
)

type Config struct {
	Dir string `toml:"dir"`
}

func (c Config) Validate() error {
	if c.Dir == "" {
		return fmt.Errorf("must specify dir")
	}
	return nil
}

func NewConfig() Config {
	return Config{
		Dir: DefaultDir,
	}
}
