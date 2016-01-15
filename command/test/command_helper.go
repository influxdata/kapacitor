package test

import (
	"bufio"
	"io"

	"github.com/influxdata/kapacitor/command"
	"github.com/influxdata/kapacitor/udf"
)

type CommandHelper struct {
	inr *io.PipeReader
	inw *io.PipeWriter

	outr *io.PipeReader
	outw *io.PipeWriter

	errr *io.PipeReader
	errw *io.PipeWriter

	Requests  chan *udf.Request
	Responses chan *udf.Response
	ErrC      chan error
}

func NewCommandHelper() *CommandHelper {
	cmd := &CommandHelper{
		Requests:  make(chan *udf.Request),
		Responses: make(chan *udf.Response),
		ErrC:      make(chan error, 1),
	}
	return cmd
}

func (c *CommandHelper) NewCommand() command.Command {
	return c
}

// Forcefully kill the command.
// This will likely cause a panic.
func (c *CommandHelper) Kill() {
	close(c.Requests)
	close(c.Responses)
	close(c.ErrC)
	c.inr.Close()
	c.inw.Close()
	c.outr.Close()
	c.outw.Close()
	c.errr.Close()
	c.errw.Close()
}

func (c *CommandHelper) readRequests() error {
	defer c.inr.Close()
	defer close(c.Requests)
	buf := bufio.NewReader(c.inr)
	var b []byte
	for {
		req := &udf.Request{}
		err := udf.ReadMessage(&b, buf, req)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		c.Requests <- req
	}
}

func (c *CommandHelper) writeResponses() error {
	defer c.outw.Close()
	for response := range c.Responses {
		udf.WriteMessage(response, c.outw)
	}
	return nil
}

func (c *CommandHelper) Start() error {
	go func() {
		readErrC := make(chan error, 1)
		writeErrC := make(chan error, 1)
		go func() {
			readErrC <- c.readRequests()
		}()
		go func() {
			writeErrC <- c.writeResponses()
		}()
		var readErr, writeErr error
		for readErrC != nil || writeErrC != nil {
			select {
			case readErr = <-readErrC:
				readErrC = nil
			case writeErr = <-writeErrC:
				writeErrC = nil
			}
		}

		if readErr != nil {
			c.ErrC <- readErr
		} else {
			c.ErrC <- writeErr
		}
	}()
	return nil
}

func (c *CommandHelper) Wait() error {
	return nil
}

// Wrapps the STDIN pipe and when it is closed
// closes the STDOUT and STDERR pipes of the command.
type cmdCloser struct {
	*io.PipeWriter
	cmd *CommandHelper
}

func (cc *cmdCloser) Close() error {
	cc.cmd.errw.Close()
	return cc.PipeWriter.Close()
}

func (c *CommandHelper) StdinPipe() (io.WriteCloser, error) {
	c.inr, c.inw = io.Pipe()
	closer := &cmdCloser{
		PipeWriter: c.inw,
		cmd:        c,
	}
	return closer, nil
}

func (c *CommandHelper) StdoutPipe() (io.ReadCloser, error) {
	c.outr, c.outw = io.Pipe()
	return c.outr, nil
}

func (c *CommandHelper) StderrPipe() (io.ReadCloser, error) {
	c.errr, c.errw = io.Pipe()
	return c.errr, nil
}
