package task_store

import (
	"fmt"
	"time"

	"github.com/influxdata/influxdb/toml"
)

type Config struct {
	Dir              string        `toml:"dir"`
	SnapshotInterval toml.Duration `toml:"snapshot-interval"`
}

func NewConfig() Config {
	return Config{
		Dir:              "./tasks",
		SnapshotInterval: toml.Duration(time.Minute),
	}
}

func (c Config) Validate() error {
	if c.Dir == "" {
		return fmt.Errorf("must specify task_store dir")
	}
	return nil
}
