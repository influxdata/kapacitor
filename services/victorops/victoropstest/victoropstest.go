package victoropstest

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
		vr := Request{
			URL: r.URL.String(),
		}
		dec := json.NewDecoder(r.Body)
		dec.Decode(&vr.PostData)
		s.mu.Lock()
		s.requests = append(s.requests, vr)
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
	MessageType    string      `json:"message_type"`
	EntityID       string      `json:"entity_id"`
	StateMessage   string      `json:"state_message"`
	Timestamp      int         `json:"timestamp"`
	MonitoringTool string      `json:"monitoring_tool"`
	Data           interface{} `json:"data"`
}
