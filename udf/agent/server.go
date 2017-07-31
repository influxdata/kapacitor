package agent

import (
	"net"
	"os"
	"os/signal"
	"sync"
)

// A server accepts connections on a listener and
// spawns new Agents for each connection.
type Server struct {
	listener net.Listener
	accepter Accepter

	mu       sync.Mutex
	stopped  bool
	stopping chan struct{}

	wg sync.WaitGroup
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
		stopping: make(chan struct{}),
	}
}

// Server starts the server and blocks.
func (s *Server) Serve() error {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return nil
	}
	s.wg.Add(1)
	s.mu.Unlock()

	defer s.wg.Done()
	return s.run()
}

// Stop closes the listener and stops all server activity.
func (s *Server) Stop() {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return
	}
	s.stopped = true
	s.listener.Close()
	s.mu.Unlock()

	close(s.stopping)
	s.wg.Wait()
}

// StopOnSignals registers a signal handler to stop the Server for the given signals.
func (s *Server) StopOnSignals(signals ...os.Signal) {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return
	}
	s.wg.Add(1)
	s.mu.Unlock()

	c := make(chan os.Signal)
	signal.Notify(c, signals...)
	go func() {
		defer s.wg.Done()
		select {
		case <-s.stopping:
		case <-c:
			s.Stop()
		}
	}()
}

func (s *Server) run() error {
	conns := make(chan net.Conn)
	errC := make(chan error, 1)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			conn, err := s.listener.Accept()
			if err != nil {
				errC <- err
			}
			conns <- conn
		}
	}()
	for {
		select {
		case <-s.stopping:
			return nil
		case err := <-errC:
			s.mu.Lock()
			stopped := s.stopped
			s.mu.Unlock()
			if stopped {
				return nil
			}
			return err
		case conn := <-conns:
			s.accepter.Accept(conn)
		}
	}
}
