package discordtest

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
		sr := Request{
			URL: r.URL.String(),
		}
		dec := json.NewDecoder(r.Body)
		dec.Decode(&sr.PostData)
		s.mu.Lock()
		s.requests = append(s.requests, sr)
		s.mu.Unlock()
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

type PostData struct {
	Username  string  `json:"username"`
	AvatarURL string  `json:"avatar_url"`
	Embeds    []Embed `json:"embeds"`
}

type Embed struct {
	Color       int    `json:"color"`
	Title       string `json:"title"`
	Description string `json:"description"`
	Timestamp   string `json:"timestamp"`
}
