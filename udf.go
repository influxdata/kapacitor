package kapacitor

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/influxdata/kapacitor/command"
	"github.com/influxdata/kapacitor/edge"
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/services/diagnostic"
	"github.com/influxdata/kapacitor/udf"
	"github.com/influxdata/kapacitor/udf/agent"
	"github.com/pkg/errors"
)

// User defined function
type UDFNode struct {
	node
	u       *pipeline.UDFNode
	udf     udf.Interface
	aborted chan struct{}

	wg      sync.WaitGroup
	mu      sync.Mutex
	stopped bool
}

// Create a new UDFNode that sends incoming data to child udf
func newUDFNode(et *ExecutingTask, n *pipeline.UDFNode, d diagnostic.Diagnostic) (*UDFNode, error) {
	un := &UDFNode{
		node:    node{Node: n, et: et, diagnostic: d},
		u:       n,
		aborted: make(chan struct{}),
	}
	// Create the UDF
	f, err := et.tm.UDFService.Create(
		n.UDFName,
		et.Task.ID,
		n.Name(),
		d,
		un.abortedCallback,
	)
	if err != nil {
		return nil, err
	}
	un.udf = f
	un.node.runF = un.runUDF
	un.node.stopF = un.stopUDF
	return un, nil
}

var errNodeAborted = errors.New("node aborted")

func (n *UDFNode) stopUDF() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.stopped {
		n.stopped = true
		if n.udf != nil {
			n.udf.Abort(errNodeAborted)
		}
	}
}

func (n *UDFNode) runUDF(snapshot []byte) (err error) {
	defer func() {
		n.mu.Lock()
		defer n.mu.Unlock()
		//Ignore stopped errors if the udf was stopped externally
		if n.stopped && (err == udf.ErrServerStopped || err == errNodeAborted) {
			err = nil
		}
		n.stopped = true
	}()

	if err := n.udf.Open(); err != nil {
		return err
	}
	if err := n.udf.Init(n.u.Options); err != nil {
		return err
	}
	if snapshot != nil {
		if err := n.udf.Restore(snapshot); err != nil {
			return err
		}
	}

	forwardErr := make(chan error, 1)
	go func() {
		out := n.udf.Out()
		for m := range out {
			if err := edge.Forward(n.outs, m); err != nil {
				forwardErr <- err
				return
			}
		}
		forwardErr <- nil
	}()

	// The abort callback needs to know when we are done writing
	// so we wrap in a wait group.
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		in := n.udf.In()
		for m, ok := n.ins[0].Emit(); ok; m, ok = n.ins[0].Emit() {
			n.timer.Start()
			select {
			case in <- m:
			case <-n.aborted:
				return
			}
			n.timer.Stop()
		}
	}()

	// wait till we are done writing
	n.wg.Wait()

	// Close the udf
	if err := n.udf.Close(); err != nil {
		return err
	}

	// Wait/Return any error from the forwarding goroutine
	return <-forwardErr
}

func (n *UDFNode) abortedCallback() {
	close(n.aborted)
	// wait till we are done writing
	n.wg.Wait()
}

func (n *UDFNode) snapshot() ([]byte, error) {
	return n.udf.Snapshot()
}

// UDFProcess wraps an external process and sends and receives data
// over STDIN and STDOUT. Lines received over STDERR are logged
// via normal Kapacitor logging.
type UDFProcess struct {
	taskName string
	nodeName string

	server    *udf.Server
	commander command.Commander
	cmdSpec   command.Spec
	cmd       command.Command

	stderr io.Reader

	// Group for waiting on the process itself
	processGroup   sync.WaitGroup
	logStdErrGroup sync.WaitGroup

	mu sync.Mutex

	diagnostic    diagnostic.Diagnostic
	timeout       time.Duration
	abortCallback func()
}

func NewUDFProcess(
	taskName, nodeName string,
	commander command.Commander,
	cmdSpec command.Spec,
	d diagnostic.Diagnostic,
	timeout time.Duration,
	abortCallback func(),
) *UDFProcess {
	return &UDFProcess{
		taskName:      taskName,
		nodeName:      nodeName,
		commander:     commander,
		cmdSpec:       cmdSpec,
		diagnostic:    d,
		timeout:       timeout,
		abortCallback: abortCallback,
	}
}

// Open the UDFProcess
func (p *UDFProcess) Open() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	cmd := p.commander.NewCommand(p.cmdSpec)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}
	p.stderr = stderr

	err = cmd.Start()
	if err != nil {
		return err
	}
	p.cmd = cmd

	outBuf := bufio.NewReader(stdout)

	p.server = udf.NewServer(
		p.taskName,
		p.nodeName,
		outBuf,
		stdin,
		p.diagnostic,
		p.timeout,
		p.abortCallback,
		cmd.Kill,
	)
	if err := p.server.Start(); err != nil {
		return err
	}

	p.logStdErrGroup.Add(1)
	go p.logStdErr()

	// Wait for process to terminate
	p.processGroup.Add(1)
	go func() {
		// First wait for the pipe read writes to finish
		p.logStdErrGroup.Wait()
		p.server.WaitIO()
		err := cmd.Wait()
		if err != nil {
			err = fmt.Errorf("process exited unexpectedly: %v", err)
			defer p.server.Abort(err)
		}
		p.processGroup.Done()
	}()

	return nil
}

// Stop the UDFProcess cleanly.
//
// Calling Close should only be done once the owner has stopped writing to the *In channel,
// at which point the remaining data will be processed and the subprocess will be allowed to exit cleanly.
func (p *UDFProcess) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	err := p.server.Stop()
	p.processGroup.Wait()
	return err
}

// Replay any lines from STDERR of the process to the Kapacitor log.
func (p *UDFProcess) logStdErr() {
	defer p.logStdErrGroup.Done()
	scanner := bufio.NewScanner(p.stderr)
	for scanner.Scan() {
		p.diagnostic.Diag(
			"level", "info",
			"P", scanner.Text(), // TODO: ???
		)
	}
}

func (p *UDFProcess) Abort(err error)                    { p.server.Abort(err) }
func (p *UDFProcess) Init(options []*agent.Option) error { return p.server.Init(options) }
func (p *UDFProcess) Snapshot() ([]byte, error)          { return p.server.Snapshot() }
func (p *UDFProcess) Restore(snapshot []byte) error      { return p.server.Restore(snapshot) }
func (p *UDFProcess) In() chan<- edge.Message            { return p.server.In() }
func (p *UDFProcess) Out() <-chan edge.Message           { return p.server.Out() }
func (p *UDFProcess) Info() (udf.Info, error)            { return p.server.Info() }

type UDFSocket struct {
	taskName string
	nodeName string

	server *udf.Server
	socket Socket

	diagnostic    diagnostic.Diagnostic
	timeout       time.Duration
	abortCallback func()
}

type Socket interface {
	Open() error
	Close() error
	In() io.WriteCloser
	Out() io.Reader
}

func NewUDFSocket(
	taskName, nodeName string,
	socket Socket,
	d diagnostic.Diagnostic,
	timeout time.Duration,
	abortCallback func(),
) *UDFSocket {
	return &UDFSocket{
		taskName:      taskName,
		nodeName:      nodeName,
		socket:        socket,
		diagnostic:    d,
		timeout:       timeout,
		abortCallback: abortCallback,
	}
}

func (s *UDFSocket) Open() error {
	err := s.socket.Open()
	if err != nil {
		return err
	}
	in := s.socket.In()
	out := s.socket.Out()
	outBuf := bufio.NewReader(out)

	s.server = udf.NewServer(
		s.taskName,
		s.nodeName,
		outBuf,
		in,
		s.diagnostic,
		s.timeout,
		s.abortCallback,
		func() { s.socket.Close() },
	)
	return s.server.Start()
}

func (s *UDFSocket) Close() error {
	if err := s.server.Stop(); err != nil {
		// Always close the socket
		s.socket.Close()
		return errors.Wrap(err, "stopping UDF server")
	}
	if err := s.socket.Close(); err != nil {
		return errors.Wrap(err, "closing UDF socket connection")
	}
	return nil
}

func (s *UDFSocket) Abort(err error)                    { s.server.Abort(err) }
func (s *UDFSocket) Init(options []*agent.Option) error { return s.server.Init(options) }
func (s *UDFSocket) Snapshot() ([]byte, error)          { return s.server.Snapshot() }
func (s *UDFSocket) Restore(snapshot []byte) error      { return s.server.Restore(snapshot) }
func (s *UDFSocket) In() chan<- edge.Message            { return s.server.In() }
func (s *UDFSocket) Out() <-chan edge.Message           { return s.server.Out() }
func (s *UDFSocket) Info() (udf.Info, error)            { return s.server.Info() }

type socket struct {
	path string
	conn *net.UnixConn
}

func NewSocketConn(path string) Socket {
	return &socket{
		path: path,
	}
}

func (s *socket) Open() error {
	addr, err := net.ResolveUnixAddr("unix", s.path)
	if err != nil {
		return err
	}
	// Connect to socket
	b := backoff.NewExponentialBackOff()
	b.MaxElapsedTime = time.Minute * 5

	err = backoff.Retry(func() error {
		conn, err := net.DialUnix("unix", nil, addr)
		if err != nil {
			return err
		}
		s.conn = conn
		return nil
	},
		b,
	)
	return err
}

func (s *socket) Close() error {
	return s.conn.Close()
}

type unixCloser struct {
	*net.UnixConn
}

func (u unixCloser) Close() error {
	// Only close the write end of the socket connection.
	// The socket connection as a whole will be closed later.
	return u.CloseWrite()
}

func (s *socket) In() io.WriteCloser {
	return unixCloser{s.conn}
}

func (s *socket) Out() io.Reader {
	return s.conn
}
