package httpd

import (
	"crypto/tls"
	"expvar"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
)

type Service struct {
	ln    net.Listener
	addr  string
	https bool
	cert  string
	err   chan error

	Handler *Handler

	Logger *log.Logger
}

func NewService(c Config) *Service {
	statMap := &expvar.Map{}
	statMap.Init()
	s := &Service{
		addr:  c.BindAddress,
		https: c.HttpsEnabled,
		cert:  c.HttpsCertificate,
		err:   make(chan error),
		Handler: NewHandler(
			c.AuthEnabled,
			c.LogEnabled,
			c.WriteTracing,
			statMap,
		),
		Logger: log.New(os.Stderr, "[http] ", log.LstdFlags),
	}
	s.Handler.Logger = s.Logger
	return s
}

// Open starts the service
func (s *Service) Open() error {
	s.Logger.Println("Starting HTTP service")
	s.Logger.Println("Authentication enabled:", s.Handler.requireAuthentication)

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

		s.Logger.Println("Listening on HTTPS:", listener.Addr().String())
		s.ln = listener
	} else {
		listener, err := net.Listen("tcp", s.addr)
		if err != nil {
			return err
		}

		s.Logger.Println("Listening on HTTP:", listener.Addr().String())
		s.ln = listener
	}

	// Begin listening for requests in a separate goroutine.
	go s.serve()
	return nil
}

// Close closes the underlying listener.
func (s *Service) Close() error {
	if s.ln != nil {
		return s.ln.Close()
	}
	return nil
}

func (s *Service) Err() <-chan error {
	return s.err
}

func (s *Service) Addr() net.Addr {
	if s.ln != nil {
		return s.ln.Addr()
	}
	return nil
}

// serve serves the handler from the listener.
func (s *Service) serve() {
	go func() {
		err := http.Serve(s.ln, s.Handler)
		// The listener was closed so exit
		// See https://github.com/golang/go/issues/4373
		if err != nil && !strings.Contains(err.Error(), "closed") {
			s.err <- fmt.Errorf("listener failed: addr=%s, err=%s", s.Addr(), err)
		} else {
			s.err <- nil
		}
	}()
}

func (s *Service) AddRoutes(routes []Route) error {
	return s.Handler.AddRoutes(routes)
}

func (s *Service) DelRoutes(routes []Route) {
	s.Handler.DelRoutes(routes)
}
