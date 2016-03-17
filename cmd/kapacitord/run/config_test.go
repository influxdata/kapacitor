package run_test

import (
	"os"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/influxdata/kapacitor/cmd/kapacitord/run"
)

// Ensure the configuration can be parsed.
func TestConfig_Parse(t *testing.T) {
	// Parse configuration.
	var c run.Config
	if _, err := toml.Decode(`
[replay]
dir = "/tmp/replay"

[task]
dir = "/tmp/task"
`, &c); err != nil {
		t.Fatal(err)
	}

	// Validate configuration.
	if c.Replay.Dir != "/tmp/replay" {
		t.Fatalf("unexpected replay dir: %s", c.Replay.Dir)
	} else if c.Task.Dir != "/tmp/task" {
		t.Fatalf("unexpected task dir: %s", c.Task.Dir)
	}
}

// Ensure the configuration can be parsed.
func TestConfig_Parse_EnvOverride(t *testing.T) {
	// Parse configuration.
	var c run.Config
	if _, err := toml.Decode(`
[influxdb]
urls=["http://localhost:8086"]

[replay]
dir = "/tmp/replay"

[task]
dir = "/tmp/task"
`, &c); err != nil {
		t.Fatal(err)
	}

	if err := os.Setenv("KAPACITOR_REPLAY_DIR", "/var/lib/kapacitor/replay"); err != nil {
		t.Fatalf("failed to set env var: %v", err)
	}

	if err := os.Setenv("KAPACITOR_TASK_DIR", "/var/lib/kapacitor/task"); err != nil {
		t.Fatalf("failed to set env var: %v", err)
	}

	if err := os.Setenv("KAPACITOR_INFLUXDB_URLS_0", "http://localhost:18086"); err != nil {
		t.Fatalf("failed to set env var: %v", err)
	}

	if err := c.ApplyEnvOverrides(); err != nil {
		t.Fatalf("failed to apply env overrides: %v", err)
	}

	// Validate configuration.
	if c.Replay.Dir != "/var/lib/kapacitor/replay" {
		t.Fatalf("unexpected replay dir: %s", c.Replay.Dir)
	} else if c.Task.Dir != "/var/lib/kapacitor/task" {
		t.Fatalf("unexpected task dir: %s", c.Task.Dir)
	} else if c.InfluxDB.URLs[0] != "http://localhost:18086" {
		t.Fatalf("unexpected url 0: %s", c.InfluxDB.URLs[0])
	}
}
