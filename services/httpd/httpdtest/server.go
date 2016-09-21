package httpdtest

import (
	"expvar"
	"log"
	"net/http/httptest"

	"github.com/influxdata/kapacitor/services/httpd"
	"github.com/influxdata/kapacitor/services/logging/loggingtest"
)

type Server struct {
	Handler *httpd.Handler
	Server  *httptest.Server
}

func NewServer(verbose bool) *Server {
	statMap := &expvar.Map{}
	statMap.Init()
	ls := loggingtest.New()
	s := &Server{
		Handler: httpd.NewHandler(
			false,
			verbose,
			verbose,
			false,
			statMap,
			ls.NewLogger("[httpdtest] ", log.LstdFlags),
			ls,
			"",
		),
	}

	s.Server = httptest.NewServer(s.Handler)
	return s
}

func (s *Server) Close() error {
	s.Server.Close()
	return nil
}

func (s *Server) AddRoutes(routes []httpd.Route) error {
	return s.Handler.AddRoutes(routes)
}

func (s *Server) DelRoutes(routes []httpd.Route) {
	s.Handler.DelRoutes(routes)
}
