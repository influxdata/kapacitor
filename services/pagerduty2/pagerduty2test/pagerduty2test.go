package pagerduty2test

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

type PDCEF struct {
	Summary       string                 `json:"summary"`
	Source        string                 `json:"source"`
	Severity      string                 `json:"severity"`
	Timestamp     string                 `json:"timestamp"`
	Class         string                 `json:"class"`
	Component     string                 `json:"component"`
	Group         string                 `json:"group"`
	CustomDetails map[string]interface{} `json:"custom_details"`
}

// Image is the struct of elements for an image in the payload
type Image struct {
	Src  string `json:"src"`
	Href string `json:"href"`
	Alt  string `json:"alt"`
}

// Link is the struct of elements for a link in the payload
type Link struct {
	Href string `json:"href"`
	Text string `json:"text"`
}

// PostData is the default struct to send an element through to PagerDuty
type PostData struct {
	RoutingKey  string  `json:"routing_key"`
	EventAction string  `json:"event_action"`
	DedupKey    string  `json:"dedup_key"`
	Payload     *PDCEF  `json:"payload"`
	Images      []Image `json:"images"`
	Links       []Link  `json:"links"`
	Client      string  `json:"client"`
	ClientURL   string  `json:"client_url"`
}
