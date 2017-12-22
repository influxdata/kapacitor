package run

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/BurntSushi/toml"
	"github.com/influxdata/kapacitor/server"
)

// PrintConfigCommand represents the command executed by "kapacitord config".
type PrintConfigCommand struct {
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
}

// NewPrintConfigCommand return a new instance of PrintConfigCommand.
func NewPrintConfigCommand() *PrintConfigCommand {
	return &PrintConfigCommand{
		Stdin:  os.Stdin,
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	}
}

// Run parses and prints the current config loaded.
func (cmd *PrintConfigCommand) Run(args ...string) error {
	// Parse command flags.
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	configPath := fs.String("config", "", "")
	hostname := fs.String("hostname", "", "")
	fs.Usage = func() { fmt.Fprintln(cmd.Stderr, printConfigUsage) }
	if err := fs.Parse(args); err != nil {
		return err
	}

	// Parse config from path.
	config, err := cmd.parseConfig(FindConfigPath(*configPath))
	if err != nil {
		return fmt.Errorf("parse config: %s", err)
	}

	// Apply any environment variables on top of the parsed config
	if err := config.ApplyEnvOverrides(); err != nil {
		return fmt.Errorf("apply env config: %v", err)
	}

	// Override config properties.
	if *hostname != "" {
		config.Hostname = *hostname
	}

	// Validate the configuration.
	if err := config.Validate(); err != nil {
		return fmt.Errorf("%s. To generate a valid configuration file run `kapacitord config > kapacitor.generated.conf`.", err)
	}

	toml.NewEncoder(cmd.Stdout).Encode(config)
	fmt.Fprint(cmd.Stdout, "\n")

	return nil
}

// FindConfigPath returns the config path specified or searches for a valid config path.
// It will return a path by searching in this order:
//   1. The given configPath
//   2. The environment variable KAPACITOR_CONFIG_PATH
//   3. The first non empty kapacitor.conf file in the path:
//        - ~/.kapacitor/
//        - /etc/kapacitor/
func FindConfigPath(configPath string) string {
	if configPath != "" {
		if configPath == os.DevNull {
			return ""
		}
		return configPath
	} else if envVar := os.Getenv("KAPACITOR_CONFIG_PATH"); envVar != "" {
		return envVar
	}

	for _, path := range []string{
		os.ExpandEnv("${HOME}/.kapacitor/kapacitor.conf"),
		"/etc/kapacitor/kapacitor.conf",
	} {
		if fi, err := os.Stat(path); err == nil && fi.Size() != 0 {
			return path
		}
	}
	return ""
}

// ParseConfig parses the config at path.
// Returns a demo configuration if path is blank.
func (cmd *PrintConfigCommand) parseConfig(path string) (*server.Config, error) {
	config, err := server.NewDemoConfig()
	if err != nil {
		config = server.NewConfig()
	}

	if path == "" {
		return config, nil
	}

	log.Println("Merging with configuration at:", path)
	if _, err := toml.DecodeFile(path, &config); err != nil {
		return nil, err
	}
	return config, nil
}

var printConfigUsage = `usage: config

	config displays the default configuration
`
