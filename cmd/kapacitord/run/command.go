package run

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"

	"github.com/BurntSushi/toml"
	"github.com/influxdata/kapacitor/server"
	"github.com/influxdata/kapacitor/services/diagnostic"
)

const logo = `
'##:::'##::::'###::::'########:::::'###:::::'######::'####:'########::'#######::'########::
 ##::'##::::'## ##::: ##.... ##:::'## ##:::'##... ##:. ##::... ##..::'##.... ##: ##.... ##:
 ##:'##::::'##:. ##:: ##:::: ##::'##:. ##:: ##:::..::: ##::::: ##:::: ##:::: ##: ##:::: ##:
 #####::::'##:::. ##: ########::'##:::. ##: ##:::::::: ##::::: ##:::: ##:::: ##: ########::
 ##. ##::: #########: ##.....::: #########: ##:::::::: ##::::: ##:::: ##:::: ##: ##.. ##:::
 ##:. ##:: ##.... ##: ##:::::::: ##.... ##: ##::: ##:: ##::::: ##:::: ##:::: ##: ##::. ##::
 ##::. ##: ##:::: ##: ##:::::::: ##:::: ##:. ######::'####:::: ##::::. #######:: ##:::. ##:
..::::..::..:::::..::..:::::::::..:::::..:::......:::....:::::..::::::.......:::..:::::..::

`

type Diagnostic interface {
	Error(msg string, err error)
	KapacitorStarting(version, branch, commit string)
	GoVersion()
	Info(msg string)
}

// Command represents the command executed by "kapacitord run".
type Command struct {
	Version  string
	Branch   string
	Commit   string
	Platform string

	closing chan struct{}
	pidfile string
	Closed  chan struct{}

	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer

	Server      *server.Server
	diagService *diagnostic.Service

	Diag Diagnostic
}

// NewCommand return a new instance of Command.
func NewCommand() *Command {
	return &Command{
		closing: make(chan struct{}),
		Closed:  make(chan struct{}),
		Stdin:   os.Stdin,
		Stdout:  os.Stdout,
		Stderr:  os.Stderr,
	}
}

// Run parses the config from args and runs the server.
func (cmd *Command) Run(args ...string) error {
	// Parse the command line flags.
	options, err := cmd.ParseFlags(args...)
	if err != nil {
		return err
	}

	// Print sweet Kapacitor logo.
	fmt.Print(logo)

	// Parse config
	config, err := cmd.ParseConfig(FindConfigPath(options.ConfigPath))
	if err != nil {
		return fmt.Errorf("parse config: %s", err)
	}

	// Apply any environment variables on top of the parsed config
	if err := config.ApplyEnvOverrides(); err != nil {
		return fmt.Errorf("apply env config: %v", err)
	}

	// Override config hostname if specified in the command line args.
	if options.Hostname != "" {
		config.Hostname = options.Hostname
	}

	// Override config logging file if specified in the command line args.
	if options.LogFile != "" {
		config.Logging.File = options.LogFile
	}

	// Override config logging level if specified in the command line args.
	if options.LogLevel != "" {
		config.Logging.Level = options.LogLevel
	}

	// Initialize Logging Services
	cmd.diagService = diagnostic.NewService(config.Logging, cmd.Stdout, cmd.Stderr)
	if err := cmd.diagService.Open(); err != nil {
		return fmt.Errorf("failed to open diagnostic service: %v", err)
	}

	// Initialize cmd diagnostic
	cmd.Diag = cmd.diagService.NewCmdHandler()

	// Mark start-up in log.,
	cmd.Diag.KapacitorStarting(cmd.Version, cmd.Branch, cmd.Commit)
	cmd.Diag.GoVersion()

	// Write the PID file.
	if err := cmd.writePIDFile(options.PIDFile); err != nil {
		return fmt.Errorf("write pid file: %s", err)
	}
	cmd.pidfile = options.PIDFile

	// Create server from config and start it.
	buildInfo := server.BuildInfo{Version: cmd.Version, Commit: cmd.Commit, Branch: cmd.Branch, Platform: cmd.Platform}
	s, err := server.New(config, buildInfo, cmd.diagService)
	if err != nil {
		return fmt.Errorf("create server: %s", err)
	}
	s.CPUProfile = options.CPUProfile
	s.MemProfile = options.MemProfile
	if err := s.Open(); err != nil {
		return fmt.Errorf("open server: %s", err)
	}

	cmd.Server = s

	// Begin monitoring the server's error channel.
	go cmd.monitorServerErrors()

	return nil
}

// Close shuts down the server.
func (cmd *Command) Close() error {
	defer close(cmd.Closed)
	defer cmd.removePIDFile()
	close(cmd.closing)
	if cmd.Server != nil {
		return cmd.Server.Close()
	}
	if cmd.diagService != nil {
		return cmd.diagService.Close()
	}
	return nil
}

func (cmd *Command) monitorServerErrors() {
	for {
		select {
		case err := <-cmd.Server.Err():
			if err != nil {
				cmd.Diag.Error("encountered error", err)
			}
		case <-cmd.closing:
			return
		}
	}
}

func (cmd *Command) removePIDFile() {
	if cmd.pidfile != "" {
		if err := os.Remove(cmd.pidfile); err != nil {
			cmd.Diag.Error("unable to remove pidfile", err)
		}
	}
}

// ParseFlags parses the command line flags from args and returns an options set.
func (cmd *Command) ParseFlags(args ...string) (Options, error) {
	var options Options
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	fs.StringVar(&options.ConfigPath, "config", "", "")
	fs.StringVar(&options.PIDFile, "pidfile", "", "")
	fs.StringVar(&options.Hostname, "hostname", "", "")
	fs.StringVar(&options.CPUProfile, "cpuprofile", "", "")
	fs.StringVar(&options.MemProfile, "memprofile", "", "")
	fs.StringVar(&options.LogFile, "log-file", "", "")
	fs.StringVar(&options.LogLevel, "log-level", "", "")
	fs.Usage = func() { fmt.Fprintln(cmd.Stderr, usage) }
	if err := fs.Parse(args); err != nil {
		return Options{}, err
	}
	return options, nil
}

// writePIDFile writes the process ID to path.
func (cmd *Command) writePIDFile(path string) error {
	// Ignore if path is not set.
	if path == "" {
		return nil
	}

	// Ensure the required directory structure exists.
	if err := os.MkdirAll(filepath.Dir(path), 0777); err != nil {
		return fmt.Errorf("mkdir: %s", err)
	}

	// Retrieve the PID and write it.
	pid := strconv.Itoa(os.Getpid())
	if err := ioutil.WriteFile(path, []byte(pid), 0666); err != nil {
		return fmt.Errorf("write file: %s", err)
	}

	return nil
}

// ParseConfig parses the config at path.
// Returns a demo configuration if path is blank.
func (cmd *Command) ParseConfig(path string) (*server.Config, error) {
	// Use demo configuration if no config path is specified.
	if path == "" {
		log.Println("No configuration provided, using default settings")
		return server.NewDemoConfig()
	}

	log.Println("Using configuration at:", path)

	config := server.NewConfig()
	if _, err := toml.DecodeFile(path, &config); err != nil {
		return nil, err
	}

	return config, nil
}

var usage = `usage: run [flags]

run starts the Kapacitor server.

        -config <path>
                          Set the path to the configuration file.

        -hostname <name>
                          Override the hostname, the 'hostname' configuration
                          option will be overridden.

        -pidfile <path>
                          Write process ID to a file.

        -log-file <path>
                          Write logs to a file.

        -log-level <level>
                          Sets the log level. One of debug,info,error.
`

// Options represents the command line options that can be parsed.
type Options struct {
	ConfigPath string
	PIDFile    string
	Hostname   string
	CPUProfile string
	MemProfile string
	LogFile    string
	LogLevel   string
}
