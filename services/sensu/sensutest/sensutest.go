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
	Name   string `json:"name"`
	Source string `json:"source"`
	Output string `json:"output"`
	Status int    `json:"status"`
}
