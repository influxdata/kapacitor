package snmptrap

import (
	"github.com/influxdata/kapacitor"
	"github.com/pkg/errors"
)

type Config struct {
	// Whether Snmptrap is enabled.
	Enabled bool `toml:"enabled" override:"enabled"`
	// NMS IP (Network Management Station).
	TargetIp string `toml:"target-ip" override:"target-ip"`
	// NMS port
	TargetPort int `toml:"target-port" override:"target-port"`
	// SNMP Community
	Community string `toml:"community" override:"community"`
	// SNMP Version
	Version string `toml:"version" override:"version"`
	// Whether all alerts should automatically post to snmptrap
	Global bool `toml:"global" override:"global"`
	// Whether all alerts should automatically use stateChangesOnly mode.
	// Only applies if global is also set.
	StateChangesOnly bool `toml:"state-changes-only" override:"state-changes-only"`
}

func NewConfig() Config {
	return Config{
		Community: kapacitor.Product,
		Version:   "2c",
	}
}

func (c Config) Validate() error {
	if c.Enabled && c.TargetIp == "" {
		return errors.New("must specify target-ip")
	}
	if c.Enabled && c.TargetPort <= 0 {
		return errors.New("must specify target-port")
	}
	return nil
}
