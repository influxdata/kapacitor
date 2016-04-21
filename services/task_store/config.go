package task_store

import (
	"time"

	"github.com/influxdata/influxdb/toml"
)

type Config struct {
	// Deprecated, only needed to find old db and migrate
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
	return nil
}
