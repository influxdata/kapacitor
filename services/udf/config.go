package udf

import (
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/influxdata/influxdb/toml"
)

type Config struct {
	Functions map[string]FunctionConfig `toml:"functions"`
}

type FunctionConfig struct {
	// General config
	Timeout toml.Duration `toml:"timeout"`

	// Config for connecting to domain socket
	Socket string `toml:"socket"`

	// Config for creating process
	Prog string            `toml:"prog"`
	Args []string          `toml:"args"`
	Env  map[string]string `toml:"env"`
}

func NewConfig() Config {
	return Config{}
}

func (c Config) Validate() error {
	for name, fc := range c.Functions {
		err := fc.Validate()
		if err != nil {
			return fmt.Errorf("UDF %s: %s", name, err.Error())
		}
	}
	return nil
}

func (c FunctionConfig) Validate() error {
	if time.Duration(c.Timeout) <= time.Millisecond {
		return fmt.Errorf("timeout is too small: %s", c.Timeout)
	}
	// Check for Python2 - which is being removed
	if strings.Contains(c.Prog, "python") {
		cmd := exec.Command(c.Prog, "--version")
		v, _ := cmd.CombinedOutput()
		if strings.Contains(string(v), "Python 2") {
			lgr := log.New(os.Stderr, "[DEPRECATION WARNING]", 0)
			lgr.Printf("\nDetected user defined function using Python 2.  " +
				"Support for Python 2 UDF processes is being discontinued.  " +
				"It may be removed in a future release.\n")
		}
	}
	// We have socket config ensure the process config is empty
	if c.Socket != "" {
		if c.Prog != "" || len(c.Args) != 0 || len(c.Env) != 0 {
			return errors.New("both socket and process config provided")
		}
	} else if c.Prog == "" {
		return errors.New("must set either prog or socket")
	}
	return nil
}
