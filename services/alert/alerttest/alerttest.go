package alerttest

import (
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"

	"github.com/influxdata/kapacitor/command"
	"github.com/influxdata/kapacitor/command/commandtest"
	alertservice "github.com/influxdata/kapacitor/services/alert"
)

type Log struct {
	path string
}

func NewLog(p string) *Log {
	return &Log{
		path: p,
	}
}

func (l *Log) Data() ([]alertservice.AlertData, error) {
	f, err := os.Open(l.path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	dec := json.NewDecoder(f)
	var data []alertservice.AlertData
	for dec.More() {
		ad := alertservice.AlertData{}
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

	data []alertservice.AlertData

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

func (s *TCPServer) Data() []alertservice.AlertData {
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
			ad := alertservice.AlertData{}
			json.NewDecoder(conn).Decode(&ad)
			s.data = append(s.data, ad)
		}()
	}
}

type PostServer struct {
	ts     *httptest.Server
	URL    string
	data   []alertservice.AlertData
	closed bool
}

func NewPostServer() *PostServer {
	s := new(PostServer)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ad := alertservice.AlertData{}
		dec := json.NewDecoder(r.Body)
		dec.Decode(&ad)
		s.data = append(s.data, ad)
	}))
	s.ts = ts
	s.URL = ts.URL
	return s
}

func (s *PostServer) Data() []alertservice.AlertData {
	return s.data
}

func (s *PostServer) Close() {
	if s.closed {
		return
	}
	s.closed = true
	s.ts.Close()
}
