package help

import (
	"fmt"
	"io"
	"os"
	"strings"
)

// Command displays help for command-line sub-commands.
type Command struct {
	Stdout io.Writer
}

// NewCommand returns a new instance of Command.
func NewCommand() *Command {
	return &Command{
		Stdout: os.Stdout,
	}
}

// Run executes the command.
func (cmd *Command) Run(args ...string) error {
	fmt.Fprintln(cmd.Stdout, strings.TrimSpace(usage))
	return nil
}

const usage = `
Configure and start a Kapacitor server.

Usage:

	kapacitord [[command] [arguments]]

The commands are:

    config               display the default configuration
    run                  run node with existing configuration
    version              displays the Kapacitor version

"run" is the default command.

Use "kapacitord help [command]" for more information about a command.
`
