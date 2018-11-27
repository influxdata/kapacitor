package sensutest

import (
	"encoding/json"
	"net"
	"sync"
)

type Server struct {
	l        *net.TCPListener
	requests []Request
	Addr     string
	wg       sync.WaitGroup
	closed   bool
}

func NewServer() (*Server, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return nil, err
	}
	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return nil, err
	}
	s := &Server{
		l:    l,
		Addr: l.Addr().String(),
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.run()
	}()
	return s, nil
}

func (s *Server) Requests() []Request {
	return s.requests
}

func (s *Server) Close() {
	if s.closed {
		return
	}
	s.closed = true
	s.l.Close()
	s.wg.Wait()
}

func (s *Server) run() {
	for {
		conn, err := s.l.Accept()
		if err != nil {
			return
		}
		func() {
			defer conn.Close()
			r := Request{}
			json.NewDecoder(conn).Decode(&r)
			s.requests = append(s.requests, r)
		}()
	}
}

type Request struct {
	Name     string                 `json:"name"`
	Source   string                 `json:"source"`
	Output   string                 `json:"output"`
	Status   int                    `json:"status"`
	Handlers []string               `json:"handlers"`
	Metadata map[string]interface{} `json:"-"`
}

func (r *Request) UnmarshalJSON(data []byte) error {
	type Alias Request
	raw := struct {
		*Alias
	}{}
	err := json.Unmarshal(data, &raw)
	if err != nil {
		return err
	}
	if raw.Alias != nil {
		*r = *(*Request)(raw.Alias)
	}
	if r.Metadata == nil {
		r.Metadata = make(map[string]interface{})
	}
	json.Unmarshal(data, &r.Metadata)
	delete(r.Metadata, "name")
	delete(r.Metadata, "source")
	delete(r.Metadata, "output")
	delete(r.Metadata, "status")
	delete(r.Metadata, "handlers")
	if len(r.Metadata) == 0 {
		r.Metadata = nil
	}
	return nil
}
