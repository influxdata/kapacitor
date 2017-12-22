package httpposttest

import (
	"encoding/json"
	"io/ioutil"
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

func NewAlertServer(headers map[string]string, raw bool) *AlertServer {
	s := new(AlertServer)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := AlertRequest{MatchingHeaders: true}
		for k, v := range headers {
			nv := r.Header.Get(k)
			if nv != v {
				req.MatchingHeaders = false
			}
		}
		if raw {
			req.Raw, _ = ioutil.ReadAll(r.Body)
			json.Unmarshal(req.Raw, &req.Data)
		} else {
			json.NewDecoder(r.Body).Decode(&req.Data)
		}
		s.data = append(s.data, req)
	}))
	s.ts = ts
	s.URL = ts.URL
	return s
}

type AlertRequest struct {
	MatchingHeaders bool
	Data            alert.Data
	Raw             []byte
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
