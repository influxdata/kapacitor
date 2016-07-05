// This package is a set of convenience helpers and structs to make integration testing easier
package server_test

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	iclient "github.com/influxdata/influxdb/client/v2"
	"github.com/influxdata/kapacitor/client/v1"
	"github.com/influxdata/kapacitor/server"
	"github.com/influxdata/wlog"
)

// Server represents a test wrapper for server.Server.
type Server struct {
	*server.Server
	Config *server.Config
}

// NewServer returns a new instance of Server.
func NewServer(c *server.Config) *Server {
	configureLogging()
	buildInfo := server.BuildInfo{
		Version: "testServer",
		Commit:  "testCommit",
		Branch:  "testBranch",
	}
	c.HTTP.LogEnabled = testing.Verbose()
	ls := &LogService{}
	srv, err := server.New(c, buildInfo, ls)
	if err != nil {
		panic(err)
	}
	s := Server{
		Server: srv,
		Config: c,
	}
	return &s
}

// OpenServer opens a test server.
func OpenDefaultServer() (*Server, *client.Client) {
	c := NewConfig()
	s := OpenServer(c)
	client := Client(s)
	return s, client
}

// OpenServer opens a test server.
func OpenServer(c *server.Config) *Server {
	s := NewServer(c)
	if err := s.Open(); err != nil {
		panic(err.Error())
	}
	return s
}

func Client(s *Server) *client.Client {
	// Create client
	client, err := client.New(client.Config{
		URL: s.URL(),
	})
	if err != nil {
		panic(err)
	}
	return client
}

// Close shuts down the server and removes all temporary paths.
func (s *Server) Close() {
	s.Server.Close()
	os.RemoveAll(s.Config.Replay.Dir)
	os.Remove(s.Config.Storage.BoltDBPath)
	os.RemoveAll(s.Config.DataDir)
}

// URL returns the base URL for the httpd endpoint.
func (s *Server) URL() string {
	if s.HTTPDService != nil {
		return s.HTTPDService.URL()
	}
	panic("httpd server not found in services")
}

// Write executes a write against the server and returns the results.
func (s *Server) Write(db, rp, body string, params url.Values) (results string, err error) {
	if params == nil {
		params = url.Values{}
	}
	if params.Get("db") == "" {
		params.Set("db", db)
	}
	if params.Get("rp") == "" {
		params.Set("rp", rp)
	}
	resp, err := http.Post(s.URL()+"/write?"+params.Encode(), "", strings.NewReader(body))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return "", fmt.Errorf("invalid status code: code=%d, body=%s", resp.StatusCode, MustReadAll(resp.Body))
	}
	return string(MustReadAll(resp.Body)), nil
}

func (s *Server) HTTPGetRetry(url, exp string, retries int, sleep time.Duration) error {
	var r string
	for retries > 0 {
		resp, err := http.Get(url)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		r = string(MustReadAll(resp.Body))
		if r == exp {
			break
		} else {
			retries--
			time.Sleep(sleep)
		}
	}
	if r != exp {
		return fmt.Errorf("unexpected result\ngot %s\nexp %s", r, exp)
	}
	return nil
}

// MustReadAll reads r. Panic on error.
func MustReadAll(r io.Reader) []byte {
	b, err := ioutil.ReadAll(r)
	if err != nil {
		panic(err)
	}
	return b
}

// MustWrite executes a write to the server. Panic on error.
func (s *Server) MustWrite(db, rp, body string, params url.Values) string {
	results, err := s.Write(db, rp, body, params)
	if err != nil {
		panic(err)
	}
	return results
}

type stats struct {
	NumEnabledTasks int `json:"num_enabled_tasks"`
	NumTasks        int `json:"num_tasks"`
}

func (s *Server) Stats() (stats, error) {
	stats := stats{}
	resp, err := http.Get(s.URL() + "/debug/vars")
	if err != nil {
		return stats, err
	}
	dec := json.NewDecoder(resp.Body)
	err = dec.Decode(&stats)
	return stats, err
}

// NewConfig returns the default config with temporary paths.
func NewConfig() *server.Config {
	c := server.NewConfig()
	c.PostInit()
	c.Reporting.Enabled = false
	c.Replay.Dir = MustTempFile()
	c.Storage.BoltDBPath = MustTempFile()
	c.DataDir = MustTempFile()
	c.HTTP.BindAddress = "127.0.0.1:0"
	//c.HTTP.BindAddress = "127.0.0.1:9092"
	//c.HTTP.GZIP = false
	c.InfluxDB[0].Enabled = false
	return c
}

// MustTempFile returns a path to a temporary file.
func MustTempFile() string {
	f, err := ioutil.TempFile("", "kapacitord-")
	if err != nil {
		panic(err)
	}
	f.Close()
	os.Remove(f.Name())
	return f.Name()
}

func configureLogging() {
	if testing.Verbose() {
		wlog.SetLevel(wlog.DEBUG)
	} else {
		wlog.SetLevel(wlog.OFF)
	}
}

type LogService struct{}

func (l *LogService) NewLogger(prefix string, flag int) *log.Logger {
	return wlog.New(os.Stderr, prefix, flag)
}
func (l *LogService) NewRawLogger(prefix string, flag int) *log.Logger {
	return log.New(os.Stderr, prefix, flag)
}

func (l *LogService) NewStaticLevelLogger(prefix string, flag int, level wlog.Level) *log.Logger {
	return log.New(wlog.NewStaticLevelWriter(os.Stderr, level), prefix, flag)
}

func (l *LogService) NewStaticLevelWriter(level wlog.Level) io.Writer {
	return wlog.NewStaticLevelWriter(os.Stderr, level)
}

type queryFunc func(q string) *iclient.Response

type InfluxDB struct {
	server *httptest.Server
}

func NewInfluxDB(callback queryFunc) *InfluxDB {
	handler := func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ping" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		q := r.URL.Query().Get("q")
		res := callback(q)
		enc := json.NewEncoder(w)
		enc.Encode(res)
	}
	return &InfluxDB{
		server: httptest.NewServer(http.HandlerFunc(handler)),
	}
}

func (i *InfluxDB) URL() string {
	return i.server.URL
}

func (i *InfluxDB) Close() {
	i.server.Close()
}
