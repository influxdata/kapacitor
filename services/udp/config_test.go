package udp_test

import (
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/influxdata/influxdb/services/udp"
)

func TestConfig_Parse(t *testing.T) {
	// Parse configuration.
	var c udp.Config
	if _, err := toml.Decode(`
enabled = true
bind-address = ":4444"
database = "awesomedb"
retention-policy = "awesomerp"
`, &c); err != nil {
		t.Fatal(err)
	}

	// Validate configuration.
	if c.Enabled != true {
		t.Fatalf("unexpected enabled: %v", c.Enabled)
	} else if c.BindAddress != ":4444" {
		t.Fatalf("unexpected bind address: %s", c.BindAddress)
	} else if c.Database != "awesomedb" {
		t.Fatalf("unexpected database: %s", c.Database)
	} else if c.RetentionPolicy != "awesomerp" {
		t.Fatalf("unexpected retention policy: %s", c.RetentionPolicy)
	}
}
