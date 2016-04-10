package tag_store

import (
	"fmt"
)

type Config struct {
	Dir string `toml:"dir"`
}

func NewConfig() Config {
	return Config{
		Dir: "./tags",
	}
}

func (c Config) Validate() error {
	if c.Dir == "" {
		return fmt.Errorf("must specify tag_store dir")
	}
	return nil
}
