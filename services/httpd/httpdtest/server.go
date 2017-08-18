package httpdtest

import (
	"expvar"
	"net/http/httptest"

	"github.com/influxdata/kapacitor/services/diagnostic"
	"github.com/influxdata/kapacitor/services/httpd"
)

type Server struct {
	Handler *httpd.Handler
	Server  *httptest.Server
}

func NewServer(verbose bool) *Server {
	statMap := &expvar.Map{}
	statMap.Init()
	// TODO: revisit
	ds := diagnostic.NewService()
	d := ds.NewDiagnostic(nil)
	s := &Server{
		Handler: httpd.NewHandler(
			false,
			verbose,
			verbose,
			false,
			statMap,
			d,
			ds,
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
