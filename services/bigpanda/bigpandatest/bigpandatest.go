package bigpandatest

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
		pr := Request{
			URL: r.URL.String(),
		}
		dec := json.NewDecoder(r.Body)
		dec.Decode(&pr.PostData)
		s.mu.Lock()
		s.requests = append(s.requests, pr)
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
	URL      string
	PostData PostData
}

// PostData is the default struct to send an element through to BigPanda
type PostData struct {
	AppKey      string `json:"app_key"`
	Status      string `json:"status"`
	Host        string `json:"host"`
	Timestamp   int64  `json:"timestamp"`
	Check       string `json:"check"`
	Description string `json:"description"`
	Cluster     string `json:"cluster"`
	Task        string `json:"task"`
	Details     string `json:"details"`
}
