package task_store

import (
	"time"

	"github.com/influxdata/influxdb/toml"
)

const (
	DefaultDir = "./tasks"
)

type Config struct {
	// Deprecated, only needed to find old db and migrate
	Dir              string        `toml:"dir"`
	SnapshotInterval toml.Duration `toml:"snapshot-interval"`
}

func NewConfig() Config {
	return Config{
		Dir:              DefaultDir,
		SnapshotInterval: toml.Duration(time.Minute),
	}
}

func (c Config) Validate() error {
	return nil
}
