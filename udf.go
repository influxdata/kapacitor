package kapacitor

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/influxdata/kapacitor/command"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/udf"
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
func newUDFNode(et *ExecutingTask, n *pipeline.UDFNode, l *log.Logger) (*UDFNode, error) {
	un := &UDFNode{
		node:    node{Node: n, et: et, logger: l},
		u:       n,
		aborted: make(chan struct{}),
	}
	// Create the UDF
	f, err := et.tm.UDFService.Create(
		n.UDFName,
		l,
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

func (u *UDFNode) stopUDF() {
	u.mu.Lock()
	defer u.mu.Unlock()
	if !u.stopped {
		u.stopped = true
		if u.udf != nil {
			u.udf.Abort(errNodeAborted)
		}
	}
}

func (u *UDFNode) runUDF(snapshot []byte) (err error) {
	defer func() {
		u.mu.Lock()
		defer u.mu.Unlock()
		//Ignore stopped errors if the udf was stopped externally
		if u.stopped && (err == udf.ErrServerStopped || err == errNodeAborted) {
			err = nil
		}
		u.stopped = true
	}()
	err = u.udf.Open()
	if err != nil {
		return
	}
	err = u.udf.Init(u.u.Options)
	if err != nil {
		return
	}
	if snapshot != nil {
		err = u.udf.Restore(snapshot)
		if err != nil {
			return
		}
	}
	forwardErr := make(chan error, 1)
	go func() {
		switch u.Provides() {
		case pipeline.StreamEdge:
			pointOut := u.udf.PointOut()
			for p := range pointOut {
				for _, out := range u.outs {
					err := out.CollectPoint(p)
					if err != nil {
						forwardErr <- err
						return
					}
				}
			}
		case pipeline.BatchEdge:
			batchOut := u.udf.BatchOut()
			for b := range batchOut {
				for _, out := range u.outs {
					err := out.CollectBatch(b)
					if err != nil {
						forwardErr <- err
						return
					}
				}
			}
		}
		forwardErr <- nil
	}()

	// The abort callback needs to know when we are done writing
	// so we wrap in a wait group.
	u.wg.Add(1)
	go func() {
		defer u.wg.Done()
		switch u.Wants() {
		case pipeline.StreamEdge:
			pointIn := u.udf.PointIn()
			for p, ok := u.ins[0].NextPoint(); ok; p, ok = u.ins[0].NextPoint() {
				u.timer.Start()
				select {
				case pointIn <- p:
				case <-u.aborted:
					return
				}
				u.timer.Stop()
			}
		case pipeline.BatchEdge:
			batchIn := u.udf.BatchIn()
			for b, ok := u.ins[0].NextBatch(); ok; b, ok = u.ins[0].NextBatch() {
				u.timer.Start()
				select {
				case batchIn <- b:
				case <-u.aborted:
					return
				}
				u.timer.Stop()
			}
		}
	}()
	// wait till we are done writing
	u.wg.Wait()

	// Close the udf
	err = u.udf.Close()
	if err != nil {
		return
	}
	// Wait/Return any error from the forwarding goroutine
	err = <-forwardErr
	return
}

func (u *UDFNode) abortedCallback() {
	close(u.aborted)
	// wait till we are done writing
	u.wg.Wait()
}

func (u *UDFNode) snapshot() ([]byte, error) {
	return u.udf.Snapshot()
}

// UDFProcess wraps an external process and sends and receives data
// over STDIN and STDOUT. Lines received over STDERR are logged
// via normal Kapacitor logging.
type UDFProcess struct {
	server    *udf.Server
	commander command.Commander
	cmdSpec   command.Spec
	cmd       command.Command

	stderr io.Reader

	// Group for waiting on the process itself
	processGroup   sync.WaitGroup
	logStdErrGroup sync.WaitGroup

	mu sync.Mutex

	logger        *log.Logger
	timeout       time.Duration
	abortCallback func()
}

func NewUDFProcess(
	commander command.Commander,
	cmdSpec command.Spec,
	l *log.Logger,
	timeout time.Duration,
	abortCallback func(),
) *UDFProcess {
	return &UDFProcess{
		commander:     commander,
		cmdSpec:       cmdSpec,
		logger:        l,
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
		outBuf,
		stdin,
		p.logger,
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
		p.logger.Println("I!P", scanner.Text())
	}
}

func (p *UDFProcess) Abort(err error)                  { p.server.Abort(err) }
func (p *UDFProcess) Init(options []*udf.Option) error { return p.server.Init(options) }
func (p *UDFProcess) Snapshot() ([]byte, error)        { return p.server.Snapshot() }
func (p *UDFProcess) Restore(snapshot []byte) error    { return p.server.Restore(snapshot) }
func (p *UDFProcess) PointIn() chan<- models.Point     { return p.server.PointIn() }
func (p *UDFProcess) BatchIn() chan<- models.Batch     { return p.server.BatchIn() }
func (p *UDFProcess) PointOut() <-chan models.Point    { return p.server.PointOut() }
func (p *UDFProcess) BatchOut() <-chan models.Batch    { return p.server.BatchOut() }
func (p *UDFProcess) Info() (udf.Info, error)          { return p.server.Info() }

type UDFSocket struct {
	server *udf.Server
	socket Socket

	logger        *log.Logger
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
	socket Socket,
	l *log.Logger,
	timeout time.Duration,
	abortCallback func(),
) *UDFSocket {
	return &UDFSocket{
		socket:        socket,
		logger:        l,
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
		outBuf,
		in,
		s.logger,
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

func (s *UDFSocket) Abort(err error)                  { s.server.Abort(err) }
func (s *UDFSocket) Init(options []*udf.Option) error { return s.server.Init(options) }
func (s *UDFSocket) Snapshot() ([]byte, error)        { return s.server.Snapshot() }
func (s *UDFSocket) Restore(snapshot []byte) error    { return s.server.Restore(snapshot) }
func (s *UDFSocket) PointIn() chan<- models.Point     { return s.server.PointIn() }
func (s *UDFSocket) BatchIn() chan<- models.Batch     { return s.server.BatchIn() }
func (s *UDFSocket) PointOut() <-chan models.Point    { return s.server.PointOut() }
func (s *UDFSocket) BatchOut() <-chan models.Batch    { return s.server.BatchOut() }
func (s *UDFSocket) Info() (udf.Info, error)          { return s.server.Info() }

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
