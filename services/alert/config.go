package alert

import (
	"time"

	"github.com/influxdata/influxdb/toml"
	"github.com/influxdata/kapacitor/alert"
)

const (
	DefaultShutdownTimeout = toml.Duration(time.Second * 10)
)

type Config struct {
	// Whether we persist the alert topics to BoltDB or not
	PersistTopics     bool `toml:"persist-topics"`
	TopicBufferLength int  `toml:"topic-buffer-length"`
}

func NewConfig() Config {
	return Config{
		PersistTopics:     true,
		TopicBufferLength: alert.DefaultEventBufferSize,
	}
}

func (c Config) Validate() error {
	return nil
}
