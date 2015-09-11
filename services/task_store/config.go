package task_store

import (
	"fmt"
)

type Config struct {
	Dir string `toml:"dir"`
}

func NewConfig() Config {
	return Config{
		Dir: "./tasks",
	}
}

func (c Config) Validate() error {
	if c.Dir == "" {
		return fmt.Errorf("must specify task_store dir")
	}
	return nil
}
