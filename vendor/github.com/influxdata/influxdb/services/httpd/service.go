package httpd // import "github.com/influxdata/influxdb/services/httpd"

import (
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/influxdata/influxdb/models"
)

// statistics gathered by the httpd package.
const (
	statRequest                      = "req"                // Number of HTTP requests served
	statCQRequest                    = "cqReq"              // Number of CQ-execute requests served
	statQueryRequest                 = "queryReq"           // Number of query requests served
	statWriteRequest                 = "writeReq"           // Number of write requests serverd
	statPingRequest                  = "pingReq"            // Number of ping requests served
	statStatusRequest                = "statusReq"          // Number of status requests served
	statWriteRequestBytesReceived    = "writeReqBytes"      // Sum of all bytes in write requests
	statQueryRequestBytesTransmitted = "queryRespBytes"     // Sum of all bytes returned in query reponses
	statPointsWrittenOK              = "pointsWrittenOK"    // Number of points written OK
	statPointsWrittenFail            = "pointsWrittenFail"  // Number of points that failed to be written
	statAuthFail                     = "authFail"           // Number of authentication failures
	statRequestDuration              = "reqDurationNs"      // Number of (wall-time) nanoseconds spent inside requests
	statQueryRequestDuration         = "queryReqDurationNs" // Number of (wall-time) nanoseconds spent inside query requests
	statWriteRequestDuration         = "writeReqDurationNs" // Number of (wall-time) nanoseconds spent inside write requests
	statRequestsActive               = "reqActive"          // Number of currently active requests
	statWriteRequestsActive          = "writeReqActive"     // Number of currently active write requests
	statClientError                  = "clientError"        // Number of HTTP responses due to client error
	statServerError                  = "serverError"        // Number of HTTP responses due to server error
)

// Service manages the listener and handler for an HTTP endpoint.
type Service struct {
	ln    net.Listener
	addr  string
	https bool
	cert  string
	key   string
	limit int
	err   chan error

	Handler *Handler

	Logger *log.Logger
}

// NewService returns a new instance of Service.
func NewService(c Config) *Service {
	s := &Service{
		addr:    c.BindAddress,
		https:   c.HTTPSEnabled,
		cert:    c.HTTPSCertificate,
		key:     c.HTTPSPrivateKey,
		limit:   c.MaxConnectionLimit,
		err:     make(chan error),
		Handler: NewHandler(c),
		Logger:  log.New(os.Stderr, "[httpd] ", log.LstdFlags),
	}
	if s.key == "" {
		s.key = s.cert
	}
	s.Handler.Logger = s.Logger
	return s
}

// Open starts the service
func (s *Service) Open() error {
	s.Logger.Println("Starting HTTP service")
	s.Logger.Println("Authentication enabled:", s.Handler.Config.AuthEnabled)

	// Open listener.
	if s.https {
		cert, err := tls.LoadX509KeyPair(s.cert, s.key)
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

	// Enforce a connection limit if one has been given.
	if s.limit > 0 {
		s.ln = LimitListener(s.ln, s.limit)
	}

	// wait for the listeners to start
	timeout := time.Now().Add(time.Second)
	for {
		if s.ln.Addr() != nil {
			break
		}

		if time.Now().After(timeout) {
			return fmt.Errorf("unable to open without http listener running")
		}
		time.Sleep(10 * time.Millisecond)
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

// SetLogOutput sets the writer to which all logs are written. It must not be
// called after Open is called.
func (s *Service) SetLogOutput(w io.Writer) {
	l := log.New(w, "[httpd] ", log.LstdFlags)
	s.Logger = l
	s.Handler.Logger = l
}

// Err returns a channel for fatal errors that occur on the listener.
func (s *Service) Err() <-chan error { return s.err }

// Addr returns the listener's address. Returns nil if listener is closed.
func (s *Service) Addr() net.Addr {
	if s.ln != nil {
		return s.ln.Addr()
	}
	return nil
}

// Statistics returns statistics for periodic monitoring.
func (s *Service) Statistics(tags map[string]string) []models.Statistic {
	return s.Handler.Statistics(models.Tags{"bind": s.addr}.Merge(tags))
}

// serve serves the handler from the listener.
func (s *Service) serve() {
	// The listener was closed so exit
	// See https://github.com/golang/go/issues/4373
	err := http.Serve(s.ln, s.Handler)
	if err != nil && !strings.Contains(err.Error(), "closed") {
		s.err <- fmt.Errorf("listener failed: addr=%s, err=%s", s.Addr(), err)
	}
}
