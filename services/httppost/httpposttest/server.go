package httpposttest

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"

	"github.com/influxdata/kapacitor/alert"
)

type AlertServer struct {
	ts     *httptest.Server
	URL    string
	data   []AlertRequest
	closed bool
}

func NewAlertServer(headers map[string]string) *AlertServer {
	s := new(AlertServer)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := AlertRequest{MatchingHeaders: true}
		for k, v := range headers {
			nv := r.Header.Get(k)
			if nv != v {
				req.MatchingHeaders = false
			}
		}
		req.Data = alert.Data{}
		dec := json.NewDecoder(r.Body)
		dec.Decode(&req.Data)
		s.data = append(s.data, req)
	}))
	s.ts = ts
	s.URL = ts.URL
	return s
}

type AlertRequest struct {
	MatchingHeaders bool
	Data            alert.Data
}

func (s *AlertServer) Data() []AlertRequest {
	return s.data
}

func (s *AlertServer) Close() {
	if s.closed {
		return
	}
	s.closed = true
	s.ts.Close()
}
