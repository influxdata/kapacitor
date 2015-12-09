package command

import (
	"io"
	"os/exec"
)

type Command interface {
	Start() error
	Wait() error

	StdinPipe() (io.WriteCloser, error)
	StdoutPipe() (io.ReadCloser, error)
	StderrPipe() (io.ReadCloser, error)

	Kill()
}

type Commander interface {
	NewCommand() Command
}

// Necessary information to create a new command
type CommandInfo struct {
	Prog string
	Args []string
	Env  []string
}

type killCmd struct {
	*exec.Cmd
}

func (k killCmd) Kill() {
	if k.Process != nil {
		k.Process.Kill()
	}
}

// Create a new Command using golang exec package and the information.
func (ci CommandInfo) NewCommand() Command {
	cmd := exec.Command(ci.Prog, ci.Args...)
	cmd.Env = ci.Env
	return killCmd{Cmd: cmd}
}
