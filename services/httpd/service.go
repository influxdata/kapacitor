package httpd

import (
	"crypto/tls"
	"expvar"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/influxdata/kapacitor/services/diagnostic"
	"github.com/influxdata/kapacitor/services/logging"
	"github.com/influxdata/kapacitor/services/notary"
)

type Notary notary.Notary

type Service struct {
	ln    net.Listener
	addr  string
	https bool
	cert  string
	err   chan error

	externalURL string

	server *http.Server
	mu     sync.Mutex
	wg     sync.WaitGroup

	new             chan net.Conn
	active          chan net.Conn
	idle            chan net.Conn
	closed          chan net.Conn
	stop            chan chan struct{}
	shutdownTimeout time.Duration

	Handler *Handler

	httpServerLogger *log.Logger

	diagnostic Diagnostic

	notary Notary
}

// TODO: not a fan of the stutter
// also not a fan of calling it a diagnosticer
// or diagnostic generator
type Diagnostic diagnostic.Diagnostic

func NewService(c Config, hostname string, li logging.Interface, d Diagnostic, ds diagnostic.Service) *Service {
	statMap := &expvar.Map{}
	statMap.Init()
	port, _ := c.Port()
	u := url.URL{
		Host:   fmt.Sprintf("%s:%d", hostname, port),
		Scheme: "http",
	}
	if c.HttpsEnabled {
		u.Scheme = "https"
	}
	s := &Service{
		addr:            c.BindAddress,
		https:           c.HttpsEnabled,
		cert:            c.HttpsCertificate,
		externalURL:     u.String(),
		err:             make(chan error, 1),
		shutdownTimeout: time.Duration(c.ShutdownTimeout),
		Handler: NewHandler(
			c.AuthEnabled,
			c.LogEnabled,
			c.WriteTracing,
			c.GZIP,
			statMap,
			li,
			d,
			ds,
			c.SharedSecret,
		),
		// TODO: not totally sure what to do about this
		httpServerLogger: li.NewStaticLevelLogger("[httpd]", log.LstdFlags, logging.ERROR),

		diagnostic: d,
	}
	return s
}

// Open starts the service
func (s *Service) Open() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.diagnostic.Diag(
		"level", "info",
		"msg", "starting HTTP service",
	)
	s.diagnostic.Diag(
		"level", "info",
		"auth_enabled", s.Handler.requireAuthentication, // TODO: idk if I like this
	)

	// Open listener.
	if s.https {
		cert, err := tls.LoadX509KeyPair(s.cert, s.cert)
		if err != nil {
			return err
		}

		listener, err := tls.Listen("tcp", s.addr, &tls.Config{
			Certificates: []tls.Certificate{cert},
		})
		if err != nil {
			return err
		}

		s.diagnostic.Diag(
			"level", "info",
			"msg", "listening on HTTPS",
			"address", listener.Addr(),
		)
		s.ln = listener
	} else {
		listener, err := net.Listen("tcp", s.addr)
		if err != nil {
			return err
		}

		s.diagnostic.Diag(
			"level", "info",
			"msg", "listening on HTTP",
			"address", listener.Addr(),
		)
		s.ln = listener
	}

	// Define server
	s.server = &http.Server{
		Handler:   s.Handler,
		ConnState: s.connStateHandler,
		ErrorLog:  s.httpServerLogger,
	}

	s.new = make(chan net.Conn)
	s.active = make(chan net.Conn)
	s.idle = make(chan net.Conn)
	s.closed = make(chan net.Conn)
	s.stop = make(chan chan struct{})

	// Begin listening for requests in a separate goroutine.
	go s.manage()

	s.wg.Add(1)
	go s.serve()
	return nil
}

// Close closes the underlying listener.
func (s *Service) Close() error {
	defer s.diagnostic.Diag("level", "info", "msg", "closed HTTP service")
	s.mu.Lock()
	defer s.mu.Unlock()
	// If server is not set we were never started
	if s.server == nil {
		return nil
	}
	// First turn off KeepAlives so that new connections will not become idle
	s.server.SetKeepAlivesEnabled(false)
	// Signal to manage loop we are stopping
	stopping := make(chan struct{})
	s.stop <- stopping

	// Next close the listener so no new connections can be made
	err := s.ln.Close()
	if err != nil {
		return err
	}

	<-stopping
	s.wg.Wait()
	s.server = nil
	return nil
}

func (s *Service) Err() <-chan error {
	return s.err
}

func (s *Service) connStateHandler(c net.Conn, state http.ConnState) {
	switch state {
	case http.StateNew:
		s.new <- c
	case http.StateActive:
		s.active <- c
	case http.StateIdle:
		s.idle <- c
	case http.StateHijacked, http.StateClosed:
		s.closed <- c
	}
}

// Watch connection state and handle stop request.
func (s *Service) manage() {
	defer func() {
		close(s.new)
		close(s.active)
		close(s.idle)
		close(s.closed)
	}()

	var stopDone chan struct{}
	conns := map[net.Conn]http.ConnState{}
	var timeout <-chan time.Time

	for {
		select {
		case c := <-s.new:
			conns[c] = http.StateNew
		case c := <-s.active:
			conns[c] = http.StateActive
		case c := <-s.idle:
			conns[c] = http.StateIdle

			// if we're already stopping, close it
			if stopDone != nil {
				c.Close()
			}
		case c := <-s.closed:
			delete(conns, c)

			// if we're waiting to stop and are all empty, we just closed the last
			// connection and we're done.
			if stopDone != nil && len(conns) == 0 {
				close(stopDone)
				return
			}
		case stopDone = <-s.stop:
			// if we're already all empty, we're already done
			if len(conns) == 0 {
				close(stopDone)
				return
			}

			// close current idle connections right away
			for c, cs := range conns {
				if cs == http.StateIdle {
					c.Close()
				}
			}

			timeout = time.After(s.shutdownTimeout)

			// continue the loop and wait for all the ConnState updates which will
			// eventually close(stopDone) and return from this goroutine.
		case <-timeout:
			s.diagnostic.Diag(
				"level", "error",
				"msg", "shutdown timedout, forcefully closing all remaining connections",
			)
			// Connections didn't close in time.
			// Forcefully close all connections.
			for c := range conns {
				c.Close()
			}
		}

	}
}

// serve serves the handler from the listener.
func (s *Service) serve() {
	defer s.wg.Done()
	err := s.server.Serve(s.ln)
	// The listener was closed so exit
	// See https://github.com/golang/go/issues/4373
	if !strings.Contains(err.Error(), "closed") {
		s.err <- fmt.Errorf("listener failed: addr=%s, err=%s", s.Addr(), err)
	} else {
		s.err <- nil
	}
}

func (s *Service) Addr() net.Addr {
	if s.ln != nil {
		return s.ln.Addr()
	}
	return nil
}

func (s *Service) URL() string {
	if s.ln != nil {
		if s.https {
			return "https://" + s.Addr().String() + BasePath
		}
		return "http://" + s.Addr().String() + BasePath
	}
	return ""
}

// URL that should resolve externally to the server HTTP endpoint.
// It is possible that the URL does not resolve correctly  if the hostname config setting is incorrect.
func (s *Service) ExternalURL() string {
	return s.externalURL
}

func (s *Service) AddRoutes(routes []Route) error {
	return s.Handler.AddRoutes(routes)
}

func (s *Service) AddPreviewRoutes(routes []Route) error {
	return s.Handler.AddPreviewRoutes(routes)
}

func (s *Service) DelRoutes(routes []Route) {
	s.Handler.DelRoutes(routes)
}
