package run_test

import (
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"text/template"
	"time"

	"github.com/influxdata/kapacitor/cmd/kapacitord/run"
)

func TestCommand_PIDFile(t *testing.T) {
	tmpdir, err := ioutil.TempDir(os.TempDir(), "kapacitord-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpdir)

	// Write out a config file that does not attempt to connect to influxdb.
	configFile := filepath.Join(tmpdir, "kapacitor.conf")
	configTemplate := template.Must(template.New("config_file").Parse(`data_dir = "{{.TempDir}}/data"
[[influxdb]]
  enabled = false
[replay]
  dir = "{{.TempDir}}/replay"
[storage]
  boltdb = "{{.TempDir}}/kapacitor.db"
[task]
  dir = "{{.TempDir}}/tasks"
[load]
  dir = "{{.TempDir}}/load"
[reporting]
  enabled = false
`))
	var buf bytes.Buffer
	if err := configTemplate.Execute(&buf, map[string]string{"TempDir": tmpdir}); err != nil {
		t.Fatalf("unable to generate config file: %s", err)
	}
	if err := ioutil.WriteFile(configFile, buf.Bytes(), 0600); err != nil {
		t.Fatalf("unable to write %s: %s", configFile, err)
	}

	pidFile := filepath.Join(tmpdir, "kapacitor.pid")

	cmd := run.NewCommand()
	if err := cmd.Run("-config", configFile, "-pidfile", pidFile); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if _, err := os.Stat(pidFile); err != nil {
		t.Fatalf("could not stat pid file: %s", err)
	}
	go cmd.Close()

	timeout := time.NewTimer(5 * time.Second)
	select {
	case <-timeout.C:
		t.Fatal("unexpected timeout")
	case <-cmd.Closed:
		timeout.Stop()
	}

	if _, err := os.Stat(pidFile); err == nil {
		t.Fatal("expected pid file to be removed")
	}
}
