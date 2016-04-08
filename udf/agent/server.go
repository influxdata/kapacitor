package agent

import (
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
)

// A server accepts connections on a listener and
// spawns new Agents for each connection.
type Server struct {
	listener net.Listener
	accepter Accepter

	conns chan net.Conn

	mu       sync.Mutex
	stopped  bool
	stopping chan struct{}
}

type Accepter interface {
	// Accept new connections from the listener and handle them accordingly.
	// The typical action is to create a new Agent with the connection as both its in and out objects.
	Accept(net.Conn)
}

// Create a new server.
func NewServer(l net.Listener, a Accepter) *Server {
	return &Server{
		listener: l,
		accepter: a,
		conns:    make(chan net.Conn),
		stopping: make(chan struct{}),
	}
}

func (s *Server) Serve() error {
	return s.run()
}

func (s *Server) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return
	}
	s.stopped = true
	close(s.stopping)
}

// Register a signal handler to stop the Server for the given signals.
func (s *Server) StopOnSignals(signals ...os.Signal) {
	c := make(chan os.Signal)
	signal.Notify(c, signals...)
	go func() {
		for range c {
			s.Stop()
		}
	}()
}

func (s *Server) run() error {
	errC := make(chan error, 1)
	go func() {
		for {
			conn, err := s.listener.Accept()
			if err != nil {
				errC <- err
			}
			s.conns <- conn
		}
	}()
	for {
		select {
		case <-s.stopping:
			s.listener.Close()
			return nil
		case err := <-errC:
			// If err is listener closed err ignore and return nil
			if strings.Contains(err.Error(), "closed") {
				return nil
			}
			return err
		case conn := <-s.conns:
			s.accepter.Accept(conn)
		}
	}
}
