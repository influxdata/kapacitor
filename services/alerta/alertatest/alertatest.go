package alertatest

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
)

type Server struct {
	mu       sync.Mutex
	ts       *httptest.Server
	URL      string
	requests []Request
	closed   bool
}

func NewServer() *Server {
	s := new(Server)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ar := Request{
			URL:           r.URL.String(),
			Authorization: r.Header.Get("Authorization"),
		}
		dec := json.NewDecoder(r.Body)
		dec.Decode(&ar.PostData)
		s.mu.Lock()
		s.requests = append(s.requests, ar)
		s.mu.Unlock()
		w.WriteHeader(http.StatusCreated)
	}))
	s.ts = ts
	s.URL = ts.URL
	return s
}
func (s *Server) Requests() []Request {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.requests
}
func (s *Server) Close() {
	if s.closed {
		return
	}
	s.closed = true
	s.ts.Close()
}

type Request struct {
	URL           string
	Authorization string
	PostData      PostData
}

type PostData struct {
	Resource    string   `json:"resource"`
	Event       string   `json:"event"`
	Group       string   `json:"group"`
	Environment string   `json:"environment"`
	Text        string   `json:"text"`
	Origin      string   `json:"origin"`
	Service     []string `json:"service"`
	Value       string   `json:"value"`
	Timeout     int64    `json:"timeout"`
}
