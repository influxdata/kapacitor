package alerttest

import (
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"

	"github.com/influxdata/kapacitor/alert"
	"github.com/influxdata/kapacitor/command"
	"github.com/influxdata/kapacitor/command/commandtest"
)

type Log struct {
	path string
}

func NewLog(p string) *Log {
	return &Log{
		path: p,
	}
}

func (l *Log) Data() ([]alert.Data, error) {
	f, err := os.Open(l.path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	dec := json.NewDecoder(f)
	var data []alert.Data
	for dec.More() {
		ad := alert.Data{}
		err := dec.Decode(&ad)
		if err != nil {
			return nil, err
		}
		data = append(data, ad)
	}
	return data, nil
}

func (l *Log) Mode() (os.FileMode, error) {
	stat, err := os.Stat(l.path)
	if err != nil {
		return 0, err
	}
	return stat.Mode(), nil
}

type Exec struct {
	ct        *commandtest.Commander
	Commander command.Commander
}

func NewExec() *Exec {
	ct := new(commandtest.Commander)
	return &Exec{
		ct:        ct,
		Commander: ct,
	}
}

func (e *Exec) Commands() []*commandtest.Command {
	return e.ct.Commands()
}

type TCPServer struct {
	Addr string

	l *net.TCPListener

	data []alert.Data

	wg     sync.WaitGroup
	closed bool
}

func NewTCPServer() (*TCPServer, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return nil, err
	}
	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return nil, err
	}
	s := &TCPServer{
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

func (s *TCPServer) Data() []alert.Data {
	return s.data
}

func (s *TCPServer) Close() {
	if s.closed {
		return
	}
	s.closed = true
	s.l.Close()
	s.wg.Wait()
}

func (s *TCPServer) run() {
	for {
		conn, err := s.l.Accept()
		if err != nil {
			return
		}
		func() {
			defer conn.Close()
			ad := alert.Data{}
			json.NewDecoder(conn).Decode(&ad)
			s.data = append(s.data, ad)
		}()
	}
}

type PostServer struct {
	ts     *httptest.Server
	URL    string
	data   []alert.Data
	closed bool
}

func NewPostServer() *PostServer {
	s := new(PostServer)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ad := alert.Data{}
		dec := json.NewDecoder(r.Body)
		dec.Decode(&ad)
		s.data = append(s.data, ad)
	}))
	s.ts = ts
	s.URL = ts.URL
	return s
}

func (s *PostServer) Data() []alert.Data {
	return s.data
}

func (s *PostServer) Close() {
	if s.closed {
		return
	}
	s.closed = true
	s.ts.Close()
}
