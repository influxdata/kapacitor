package commandtest

import (
	"errors"
	"io"
	"io/ioutil"
	"sync"

	"github.com/influxdata/kapacitor/command"
)

type CommanderTest struct {
	NewCommandHook func(c *CommandTest)
}

func (c CommanderTest) NewCommand(ci command.CommandInfo) command.Command {
	cmd := &CommandTest{
		Info: ci,
	}
	if c.NewCommandHook != nil {
		c.NewCommandHook(cmd)
	}
	return cmd
}

type CommandTest struct {
	sync.Mutex
	Info command.CommandInfo

	StdinPipeFunc  func() (io.WriteCloser, error)
	StdoutPipeFunc func() (io.Reader, error)
	StderrPipeFunc func() (io.Reader, error)

	Started     bool
	Waited      bool
	Killed      bool
	StdinData   []byte
	StdoutValue io.Writer
	StderrValue io.Writer

	stdin io.Reader
}

func (c *CommandTest) Start() error {
	c.Lock()
	defer c.Unlock()
	c.Started = true
	data, err := ioutil.ReadAll(c.stdin)
	if err != nil {
		return err
	}
	c.StdinData = data
	return nil
}
func (c *CommandTest) Wait() error {
	c.Lock()
	c.Waited = true
	c.Unlock()
	return nil
}
func (c *CommandTest) Stdin(in io.Reader) {
	c.Lock()
	c.stdin = in
	c.Unlock()
}
func (c *CommandTest) Stdout(out io.Writer) {
	c.Lock()
	c.StdoutValue = out
	c.Unlock()
}
func (c *CommandTest) Stderr(err io.Writer) {
	c.Lock()
	c.StderrValue = err
	c.Unlock()
}
func (c *CommandTest) Kill() {
	c.Lock()
	c.Killed = true
	c.Unlock()
}
func (c *CommandTest) StdinPipe() (io.WriteCloser, error) {
	if c.StdinPipeFunc != nil {
		return c.StdinPipeFunc()
	}
	return nil, errors.New("not implemented")
}
func (c *CommandTest) StdoutPipe() (io.Reader, error) {
	if c.StdoutPipeFunc != nil {
		return c.StdoutPipeFunc()
	}
	return nil, errors.New("not implemented")
}
func (c *CommandTest) StderrPipe() (io.Reader, error) {
	if c.StderrPipeFunc != nil {
		return c.StderrPipeFunc()
	}
	return nil, errors.New("not implemented")
}
