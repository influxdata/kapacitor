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
	AppKey            string `json:"app_key"`
	Status            string `json:"status"`
	Host              string `json:"host"`
	Timestamp         int64  `json:"timestamp"`
	Check             string `json:"check"`
	Description       string `json:"description"`
	Cluster           string `json:"cluster"`
	Task              string `json:"task"`
	Details           string `json:"details"`
	PrimaryProperty   string `json:"primary_property"`
	SecondaryProperty string `json:"secondary_property"`
	Attributes        map[string]string `json:"-,omitempty"`
}

func (pd *PostData) UnmarshalJSON(data []byte) error {
	var x map[string]interface{}
	if err := json.Unmarshal(data, &x); err != nil {
		return nil
	}
	if appKey, ok := x["app_key"]; ok {
		pd.AppKey = appKey.(string)
		delete(x, "app_key")
	}
	if status, ok := x["status"]; ok {
		pd.Status = status.(string)
		delete(x, "status")
	}
	if host, ok := x["host"]; ok {
		pd.Host = host.(string)
		delete(x, "host")
	}
	if timestamp, ok := x["timestamp"]; ok {
		pd.Timestamp = int64(timestamp.(float64))
		delete(x, "timestamp")
	}
	if check, ok := x["check"]; ok {
		pd.Check = check.(string)
		delete(x, "check")
	}
	if description, ok := x["description"]; ok {
		pd.Description = description.(string)
		delete(x, "description")
	}
	if cluster, ok := x["cluster"]; ok {
		pd.Cluster = cluster.(string)
		delete(x, "cluster")
	}
	if task, ok := x["task"]; ok {
		pd.Task = task.(string)
		delete(x, "task")
	}
	if details, ok := x["details"]; ok {
		pd.Details = details.(string)
		delete(x, "details")
	}
	if primary, ok := x["primary_property"]; ok {
		pd.PrimaryProperty = primary.(string)
		delete(x, "primary_property")
	}
	if secondary, ok := x["secondary_property"]; ok {
		pd.SecondaryProperty = secondary.(string)
		delete(x, "secondary_property")
	}
	if len(x) > 0 {
		pd.Attributes = make(map[string]string, len(x))
		for k, v := range x {
			switch value := v.(type) {
			case string:
				pd.Attributes[k] = value
			default:
				b, err := json.Marshal(value)
				if err != nil {
					return err
				}
				pd.Attributes[k] = string(b)
			}
		}
	}
	return nil
}