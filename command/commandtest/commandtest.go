package commandtest

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"reflect"
	"sync"

	"github.com/influxdata/kapacitor/command"
)

type Commander struct {
	sync.Mutex
	cmds []*Command
}

func (c *Commander) NewCommand(s command.Spec) command.Command {
	c.Lock()
	defer c.Unlock()
	cmd := &Command{
		Spec: s,
	}
	c.cmds = append(c.cmds, cmd)
	return cmd
}

func (c *Commander) Commands() []*Command {
	c.Lock()
	defer c.Unlock()
	return c.cmds
}

type Command struct {
	sync.Mutex
	Spec command.Spec

	Started   bool
	Waited    bool
	Killed    bool
	StdinData []byte

	StdinPipeCalled  bool
	StdoutPipeCalled bool
	StderrPipeCalled bool

	stdin  io.Reader
	stdinR *io.PipeReader
	stdinW *io.PipeWriter
}

func (c *Command) Compare(o *Command) error {
	c.Lock()
	o.Lock()
	defer c.Unlock()
	defer o.Unlock()

	if got, exp := o.Spec, c.Spec; !reflect.DeepEqual(got, exp) {
		return fmt.Errorf("unexpected command infos value: got %v exp %v", got, exp)
	}
	if got, exp := o.Started, c.Started; got != exp {
		return fmt.Errorf("unexpected started value: got %v exp %v", got, exp)
	}
	if got, exp := o.Waited, c.Waited; got != exp {
		return fmt.Errorf("unexpected waited value: got %v exp %v", got, exp)
	}
	if got, exp := o.Killed, c.Killed; got != exp {
		return fmt.Errorf("unexpected killed value: got %v exp %v", got, exp)
	}
	if got, exp := o.StdinData, c.StdinData; !bytes.Equal(got, exp) {
		return fmt.Errorf("unexpected stdin data value:\ngot\n%q\nexp\n%q\n", string(got), string(exp))
	}

	if got, exp := o.StdinPipeCalled, c.StdinPipeCalled; got != exp {
		return fmt.Errorf("unexpected StdinPipeCalled value: got %v exp %v", got, exp)
	}
	if got, exp := o.StdoutPipeCalled, c.StdoutPipeCalled; got != exp {
		return fmt.Errorf("unexpected StdoutPipeCalled value: got %v exp %v", got, exp)
	}
	if got, exp := o.StderrPipeCalled, c.StderrPipeCalled; got != exp {
		return fmt.Errorf("unexpected StderrPipeCalled value: got %v exp %v", got, exp)
	}
	return nil
}

func (c *Command) Start() error {
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
func (c *Command) Wait() error {
	c.Lock()
	c.Waited = true
	c.Unlock()
	return nil
}
func (c *Command) Stdin(in io.Reader) {
	c.Lock()
	c.stdin = in
	c.Unlock()
}
func (c *Command) Stdout(out io.Writer) {
	// Not useful to keep value so just ignore it
}
func (c *Command) Stderr(err io.Writer) {
	// Not useful to keep value so just ignore it
}
func (c *Command) Kill() {
	c.Lock()
	c.Killed = true
	c.Unlock()
}
func (c *Command) StdinPipe() (io.WriteCloser, error) {
	c.Lock()
	defer c.Unlock()
	c.StdinPipeCalled = true
	c.stdinR, c.stdinW = io.Pipe()
	c.stdin = c.stdinR
	return c.stdinW, nil
}
func (c *Command) StdoutPipe() (io.Reader, error) {
	c.Lock()
	defer c.Unlock()
	c.StdoutPipeCalled = true
	return new(bytes.Buffer), nil
}
func (c *Command) StderrPipe() (io.Reader, error) {
	c.Lock()
	defer c.Unlock()
	c.StderrPipeCalled = true
	return new(bytes.Buffer), nil
}
