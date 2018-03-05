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

[[httppost]]
headers = { Authorization = "your-key" }
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

	if err := os.Setenv("KAPACITOR_HTTPPOST_0_HEADERS_Authorization", "my-key"); err != nil {
		t.Fatalf("failed to set env var: %v", err)
	}

	if err := c.ApplyEnvOverrides(); err != nil {
		t.Fatalf("failed to apply env overrides: %v", err)
	}

	// Validate configuration.
	if c.Replay.Dir != "/var/lib/kapacitor/replay" {
		t.Fatalf("unexpected replay dir: %s", c.Replay.Dir)
	} else if c.Storage.BoltDBPath != "/var/lib/kapacitor/kapacitor.db" {
		t.Fatalf("unexpected storage boltdb-path: %s", c.Storage.BoltDBPath)
	} else if c.InfluxDB[0].URLs[0] != "http://localhost:18086" {
		t.Fatalf("unexpected url 0: %s", c.InfluxDB[0].URLs[0])
	} else if c.HTTPPost[0].Headers["Authorization"] != "my-key" {
		t.Fatalf("unexpected header Authorization: %s", c.InfluxDB[0].URLs[0])
	}
}

// Ensure the configuration can be parsed.
func TestConfig_Single_Conf(t *testing.T) {
	// Parse configuration.
	var c server.Config
	if _, err := toml.Decode(`
[slack]
enabled = true
channel = "#kapacitor_test"
`, &c); err != nil {
		t.Fatal(err)
	}

	// Validate configuration.
	if c.Slack[0].Channel != "#kapacitor_test" || c.Slack[0].Enabled != true {
		t.Fatalf("unexpected slack channel or channel not enabled: %s, %v", c.Slack[0].Channel, c.Slack[0].Enabled)
	}
}

// Ensure the configuration can be parsed.
func TestConfig_Single_Multiple_Conf(t *testing.T) {
	// Parse configuration.
	var c server.Config
	if _, err := toml.Decode(`
[[slack]]
enabled = true
channel = "#kapacitor_test"
`, &c); err != nil {
		t.Fatal(err)
	}

	// Validate configuration.
	if c.Slack[0].Channel != "#kapacitor_test" || c.Slack[0].Enabled != true {
		t.Fatalf("unexpected slack channel or channel not enabled: %s, %v", c.Slack[0].Channel, c.Slack[0].Enabled)
	}
}

// Ensure the configuration can be parsed.
func TestConfig_Multiple_Multiple_Conf(t *testing.T) {
	// Parse configuration.
	var c server.Config
	if _, err := toml.Decode(`
[[slack]]
workspace = "private"
url = "private.slack.com"
default = true
enabled = true
channel = "#kapacitor_private"
[[slack]]
workspace = "public"
url = "public.slack.com"
default = false
enabled = false
channel = "#kapacitor_public"
`, &c); err != nil {
		t.Fatal(err)
	}

	if err := c.Slack.Validate(); err != nil {
		t.Fatalf("Expected config to be valid, %v", err)
	}

	// Validate configuration.
	if c.Slack[0].Workspace != "private" || c.Slack[0].Channel != "#kapacitor_private" || c.Slack[0].Enabled != true {
		t.Fatalf("unexpected slack workspace, channel or channel not enabled: %s %s, %v", c.Slack[0].Workspace, c.Slack[0].Channel, c.Slack[0].Enabled)
	} else if c.Slack[1].Workspace != "public" || c.Slack[1].Channel != "#kapacitor_public" || c.Slack[1].Enabled != false {
		t.Fatalf("unexpected slack workspace, channel or channel enabled: %s %s, %v", c.Slack[1].Workspace, c.Slack[1].Channel, c.Slack[1].Enabled)
	}
}

// Ensure the configuration can be parsed.
func TestConfig_Invalid_Multiple_Conf(t *testing.T) {
	// Parse configuration.
	var c server.Config
	cStr := `
[[slack]]
workspace = "private"
url = "private.slack.com"
default = false
enabled = true
channel = "#kapacitor_private"
[[slack]]
workspace = "public"
url = "public.slack.com"
default = false
enabled = false
channel = "#kapacitor_public"
`
	if _, err := toml.Decode(cStr, &c); err != nil {
		t.Fatal(err)
	}

	if err := c.Slack.Validate(); err == nil {
		t.Fatalf("Expected config to be invalid, %s", cStr)
	}

	cStr = `
[[slack]]
workspace = "private"
default = true
enabled = true
channel = "#kapacitor_private"
[[slack]]
workspace = "public"
url = "public.slack.com"
default = false
enabled = false
channel = "#kapacitor_public"
`
	if _, err := toml.Decode(cStr, &c); err != nil {
		t.Fatal(err)
	}

	if err := c.Slack.Validate(); err == nil {
		t.Fatalf("Expected config to be invalid, %s", cStr)
	}
}
