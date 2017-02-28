package pushovertest

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
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
		fr := Request{}
		data, _ := ioutil.ReadAll(r.Body)
		v, _ := url.ParseQuery(string(data))
		fr.PostData, _ = NewPostData(v)
		s.mu.Lock()
		s.requests = append(s.requests, fr)
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
	PostData PostData
}

type PostData struct {
	Token    string
	UserKey  string
	Message  string
	Device   string
	Title    string
	URL      string
	URLTitle string
	Sound    string
	Priority int
}

func NewPostData(v url.Values) (PostData, error) {
	p := PostData{}

	p.Token = v.Get("token")
	p.UserKey = v.Get("user")
	p.Message = v.Get("message")
	p.Device = v.Get("device")
	p.Title = v.Get("title")
	p.URL = v.Get("url")
	p.URLTitle = v.Get("url_title")
	p.Sound = v.Get("sound")
	priority, err := strconv.Atoi(v.Get("priority"))
	if err != nil {
		return p, err
	}
	p.Priority = priority

	return p, nil
}
