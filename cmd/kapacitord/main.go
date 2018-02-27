package main

import (
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/influxdata/kapacitor/cmd/kapacitord/help"
	"github.com/influxdata/kapacitor/cmd/kapacitord/run"
	"github.com/influxdata/kapacitor/services/diagnostic"
)

type Diagnostic run.Diagnostic

// These variables are populated via the Go linker.
var (
	version string
	commit  string
	branch  string
)

func init() {
	// If commit or branch are not set, make that clear.
	if commit == "" {
		commit = "unknown"
	}
	if branch == "" {
		branch = "unknown"
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())

	m := NewMain()
	if err := m.Run(os.Args[1:]...); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// Main represents the program execution.
type Main struct {
	Diag Diagnostic

	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
}

// NewMain return a new instance of Main.
func NewMain() *Main {
	return &Main{
		Diag:   diagnostic.BootstrapMainHandler(),
		Stdin:  os.Stdin,
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	}
}

// Run determines and runs the command specified by the CLI args.
func (m *Main) Run(args ...string) error {
	name, args := ParseCommandName(args)

	// Extract name from args.
	switch name {
	case "", "run":
		cmd := run.NewCommand()

		// Tell the server the build details.
		cmd.Version = version
		cmd.Commit = commit
		cmd.Branch = branch
		cmd.Platform = "OSS"

		err := cmd.Run(args...)
		// Use diagnostic from cmd since it may have special config now.
		if cmd.Diag != nil {
			m.Diag = cmd.Diag
		}
		if err != nil {
			m.Diag.Error("encountered error", err)
			return fmt.Errorf("run: %s", err)
		}

		signalCh := make(chan os.Signal, 1)
		signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
		m.Diag.Info("listening for signals")

	Loop:
		for s := range signalCh {
			switch s.String() {
			case syscall.SIGTERM.String():
				m.Diag.Info("SIGTERM received, initializing clean shutdown...")
				go func() {
					cmd.Close()
				}()
				break Loop

			case syscall.SIGHUP.String():
				m.Diag.Info("SIGHUP received, reloading tasks/templates/handlers directory...")
				cmd.Server.Reload()

			default:
				m.Diag.Info("signal received, initializing clean shutdown...")
				go func() {
					cmd.Close()
				}()
				break Loop
			}
		}

		// Block again until another signal is received, a shutdown timeout elapses,
		// or the Command is gracefully closed
		m.Diag.Info("waiting for clean shutdown...")
		select {
		case <-signalCh:
			m.Diag.Info("second signal received, initializing hard shutdown")
		case <-time.After(time.Second * 30):
			m.Diag.Info("time limit reached, initializing hard shutdown")
		case <-cmd.Closed:
			m.Diag.Info("server shutdown completed")
		}

		// goodbye.

	case "config":
		if err := run.NewPrintConfigCommand().Run(args...); err != nil {
			return fmt.Errorf("config: %s", err)
		}
	case "version":
		if err := NewVersionCommand().Run(args...); err != nil {
			return fmt.Errorf("version: %s", err)
		}
	case "help":
		if err := help.NewCommand().Run(args...); err != nil {
			return fmt.Errorf("help: %s", err)
		}
	default:
		return fmt.Errorf(`unknown command "%s"`+"\n"+`Run 'kapacitord help' for usage`+"\n\n", name)
	}

	return nil
}

// ParseCommandName extracts the command name and args from the args list.
func ParseCommandName(args []string) (string, []string) {
	// Retrieve command name as first argument.
	var name string
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		name = args[0]
	}

	// Special case -h immediately following binary name
	if len(args) > 0 && args[0] == "-h" {
		name = "help"
	}

	// If command is "help" and has an argument then rewrite args to use "-h".
	if name == "help" && len(args) > 1 {
		args[0], args[1] = args[1], "-h"
		name = args[0]
	}

	// If a named command is specified then return it with its arguments.
	if name != "" {
		return name, args[1:]
	}
	return "", args
}

// Command represents the command executed by "kapacitord version".
type VersionCommand struct {
	Stdout io.Writer
	Stderr io.Writer
}

// NewVersionCommand return a new instance of VersionCommand.
func NewVersionCommand() *VersionCommand {
	return &VersionCommand{
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	}
}

// Run prints the current version and commit info.
func (cmd *VersionCommand) Run(args ...string) error {
	// Parse flags in case -h is specified.
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	fs.Usage = func() { fmt.Fprintln(cmd.Stderr, strings.TrimSpace(versionUsage)) }
	if err := fs.Parse(args); err != nil {
		return err
	}

	// Print version info.
	fmt.Fprintf(cmd.Stdout, "Kapacitor OSS version %s (git: %s %s)\n", version, branch, commit)

	return nil
}

var versionUsage = `
usage: version

	version displays the Kapacitor version, build branch and git commit hash
`
