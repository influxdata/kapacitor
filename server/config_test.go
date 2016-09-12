package server_test

import (
	"os"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/influxdata/kapacitor/server"
)

// Ensure the configuration can be parsed.
func TestConfig_Parse(t *testing.T) {
	// Parse configuration.
	var c server.Config
	if _, err := toml.Decode(`
[replay]
dir = "/tmp/replay"

[storage]
boltdb = "/tmp/kapacitor.db"
`, &c); err != nil {
		t.Fatal(err)
	}

	// Validate configuration.
	if c.Replay.Dir != "/tmp/replay" {
		t.Fatalf("unexpected replay dir: %s", c.Replay.Dir)
	} else if c.Storage.BoltDBPath != "/tmp/kapacitor.db" {
		t.Fatalf("unexpected storage boltdb-path: %s", c.Storage.BoltDBPath)
	}
}

// Ensure the configuration can be parsed.
func TestConfig_Parse_EnvOverride(t *testing.T) {
	// Parse configuration.
	var c server.Config
	if _, err := toml.Decode(`
[[influxdb]]
urls=["http://localhost:8086"]

[replay]
dir = "/tmp/replay"

[storage]
boltdb = "/tmp/kapacitor.db"

[vars]
my_custom_var = "42"
override_me = "please?"
`, &c); err != nil {
		t.Fatal(err)
	}

	if err := os.Setenv("KAPACITOR_REPLAY_DIR", "/var/lib/kapacitor/replay"); err != nil {
		t.Fatalf("failed to set env var: %v", err)
	}

	if err := os.Setenv("KAPACITOR_STORAGE_BOLTDB", "/var/lib/kapacitor/kapacitor.db"); err != nil {
		t.Fatalf("failed to set env var: %v", err)
	}

	if err := os.Setenv("KAPACITOR_INFLUXDB_0_URLS_0", "http://localhost:18086"); err != nil {
		t.Fatalf("failed to set env var: %v", err)
	}

	if err := os.Setenv("KAPACITOR_VARS_override_me", "ok"); err != nil {
		t.Fatalf("failed to set env var: %v", err)
	}

	if err := os.Setenv("KAPACITOR_VARS_my_other_custom_var", "6*9"); err != nil {
		t.Fatalf("failed to set env var: %v", err)
	}

	if err := c.ApplyEnvOverrides(); err != nil {
		t.Fatalf("failed to apply env overrides: %v", err)
	}

	// Validate configuration.
	if c.Replay.Dir != "/var/lib/kapacitor/replay" {
		t.Fatalf("unexpected replay dir: %s", c.Replay.Dir)
	}
	if c.Storage.BoltDBPath != "/var/lib/kapacitor/kapacitor.db" {
		t.Fatalf("unexpected storage boltdb-path: %s", c.Storage.BoltDBPath)
	}
	if c.InfluxDB[0].URLs[0] != "http://localhost:18086" {
		t.Fatalf("unexpected url 0: %s", c.InfluxDB[0].URLs[0])
	}
	if c.Vars["my_custom_var"] != "42" {
		t.Fatalf("unexpected my_custom_var: %s", c.Vars["my_custom_var"])
	}
	if c.Vars["override_me"] != "ok" {
		t.Fatalf("unexpected override_me: %s", c.Vars["override_me"])
	}
	if c.Vars["my_other_custom_var"] != "6*9" {
		t.Fatalf("unexpected my_other_custom_var: %s", c.Vars["my_other_custom_var"])
	}

}
