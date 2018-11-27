package alert

import (
	"time"

	"github.com/influxdata/influxdb/toml"
)

const (
	DefaultShutdownTimeout = toml.Duration(time.Second * 10)
)

type Config struct {
	// Whether we persist the alert topics to BoltDB or not
	PersistTopics bool `toml:"persist-topics"`
}

func NewConfig() Config {
	return Config{
		PersistTopics: true,
	}
}

func (c Config) Validate() error {
	return nil
}
