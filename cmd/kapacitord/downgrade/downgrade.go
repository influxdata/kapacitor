package downgrade

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/influxdata/influxdb/pkg/errors"
	"github.com/influxdata/kapacitor/cmd/kapacitord/run"
	"github.com/influxdata/kapacitor/keyvalue"
	"github.com/influxdata/kapacitor/services/alert"
	"github.com/influxdata/kapacitor/services/diagnostic"
	"github.com/influxdata/kapacitor/services/httpd"
	"github.com/influxdata/kapacitor/services/storage"
)

const downgradeUsage = `usage: downgrade

	downgrade converts reverts a topic store format upgrade`

type Diagnostic interface {
	Error(msg string, err error)
	KapacitorStarting(version, branch, commit string)
	GoVersion()
	Info(msg string, ctx ...keyvalue.T)
}

type HTTPDService interface {
	AddRoutes([]httpd.Route) error
	DelRoutes([]httpd.Route)
}

// Command represents the command executed by "kapacitord downgrade".
type Command struct {
	Stdout io.Writer
	Stderr io.Writer

	storageService *storage.Service
	diagService    *diagnostic.Service
}

func NewCommand() *Command {
	return &Command{
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	}
}

func (cmd *Command) Run(args ...string) (rErr error) {
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	fs.Usage = func() { fmt.Fprintln(cmd.Stderr, downgradeUsage) }

	pcc := run.NewPrintConfigCommand()
	config, err := pcc.PrepareConfig(args, fs)
	if err != nil {
		return err
	}

	// Initialize Logging Services
	cmd.diagService = diagnostic.NewService(config.Logging, cmd.Stdout, cmd.Stderr)
	if err = cmd.diagService.Open(); err != nil {
		return fmt.Errorf("failed to open diagnostic service: %v", err)
	}
	defer errors.Capture(&rErr, cmd.diagService.Close)

	d := cmd.diagService.NewStorageHandler()
	cmd.storageService = storage.NewService(config.Storage, d)
	cmd.storageService.HTTPDService = &NoOpHTTPDService{}

	if err = cmd.storageService.Open(); err != nil {
		return fmt.Errorf("open service %T: %s", cmd.storageService, err)
	}
	defer errors.Capture(&rErr, cmd.storageService.Close)()
	cmd.diagService.Logger.Info("Starting downgrade of topic store")
	return alert.MigrateTopicStoreV2V1(cmd.storageService)
}

type NoOpHTTPDService struct {
}

func (s *NoOpHTTPDService) AddRoutes([]httpd.Route) error {
	return nil
}

func (s *NoOpHTTPDService) DelRoutes([]httpd.Route) {
}
