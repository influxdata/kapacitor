package command

import (
	"io"
	"os/exec"
)

type Command interface {
	Start() error
	Wait() error

	Stdin(io.Reader)
	Stdout(io.Writer)
	Stderr(io.Writer)
	StdinPipe() (io.WriteCloser, error)
	StdoutPipe() (io.Reader, error)
	StderrPipe() (io.Reader, error)

	Kill()
}

// Spec contains the necessary information to create a new command.
type Spec struct {
	Prog string
	Args []string
	Env  []string
}

// Commander creates new commands.
type Commander interface {
	NewCommand(Spec) Command
}

// ExecCommander implements Commander using the stdlib os/exec package.
type execCommander struct{}

// ExecCommander creates commands using the os/exec package.
var ExecCommander = execCommander{}

// Create a new Command using golang exec package and the information.
func (execCommander) NewCommand(s Spec) Command {
	c := exec.Command(s.Prog, s.Args...)
	c.Env = s.Env
	return execCmd{c}
}

// ExecCmd implements Command using the stdlib os/exec package.
type execCmd struct {
	*exec.Cmd
}

func (c execCmd) Stdin(in io.Reader)   { c.Cmd.Stdin = in }
func (c execCmd) Stdout(out io.Writer) { c.Cmd.Stdout = out }
func (c execCmd) Stderr(err io.Writer) { c.Cmd.Stderr = err }

func (c execCmd) StdoutPipe() (io.Reader, error) { return c.Cmd.StdoutPipe() }
func (c execCmd) StderrPipe() (io.Reader, error) { return c.Cmd.StderrPipe() }

func (c execCmd) Kill() {
	if c.Cmd.Process != nil {
		c.Cmd.Process.Kill()
	}
}
