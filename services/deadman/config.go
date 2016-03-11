package deadman

import (
	"time"

	"github.com/influxdata/influxdb/toml"
)

const (
	// Default deadman's switch interval
	DefaultInterval = toml.Duration(time.Second * 10)
	// Default deadman's switch threshold
	DefaultThreshold = float64(0)
	// Default deadman's switch id
	DefaultId = "{{ .Group }}:NODE_NAME for task '{{ .TaskName }}'"
	// Default deadman's switch message
	DefaultMessage = "{{ .ID }} is {{ if eq .Level \"OK\" }}alive{{ else }}dead{{ end }}: {{ index .Fields \"emitted\" | printf \"%0.3f\" }} points/INTERVAL."
)

type Config struct {
	Interval  toml.Duration `toml:"interval"`
	Threshold float64       `toml:"threshold"`
	Id        string        `toml:"id"`
	Message   string        `toml:"message"`
	Global    bool          `toml:"global"`
}

func NewConfig() Config {
	return Config{
		Interval:  DefaultInterval,
		Threshold: DefaultThreshold,
		Id:        DefaultId,
		Message:   DefaultMessage,
	}
}
