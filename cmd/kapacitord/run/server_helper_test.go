// This package is a set of convenience helpers and structs to make integration testing easier
package run_test

import (
	"bytes"
	"encoding/json"
	"errors"
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

	"github.com/influxdb/influxdb/client"
	"github.com/influxdb/kapacitor"
	"github.com/influxdb/kapacitor/cmd/kapacitord/run"
	"github.com/influxdb/kapacitor/services/task_store"
	"github.com/influxdb/kapacitor/wlog"
)

// Server represents a test wrapper for run.Server.
type Server struct {
	*run.Server
	Config *run.Config
}

// NewServer returns a new instance of Server.
func NewServer(c *run.Config) *Server {
	buildInfo := &run.BuildInfo{
		Version: "testServer",
		Commit:  "testCommit",
		Branch:  "testBranch",
	}
	ls := &LogService{}
	srv, _ := run.NewServer(c, buildInfo, ls)
	s := Server{
		Server: srv,
		Config: c,
	}
	configureLogging()
	return &s
}

// OpenServer opens a test server.
func OpenDefaultServer() *Server {
	c := NewConfig()
	return OpenServer(c)
}

// OpenServer opens a test server.
func OpenServer(c *run.Config) *Server {
	s := NewServer(c)
	if err := s.Open(); err != nil {
		panic(err.Error())
	}
	return s
}

// Close shuts down the server and removes all temporary paths.
func (s *Server) Close() {
	s.Server.Close()
	os.RemoveAll(s.Config.Replay.Dir)
	os.RemoveAll(s.Config.Task.Dir)
	os.RemoveAll(s.Config.DataDir)
}

// URL returns the base URL for the httpd endpoint.
func (s *Server) URL() string {
	if s.HTTPDService != nil {
		return "http://" + s.HTTPDService.Addr().String()
	}
	panic("httpd server not found in services")
}

// HTTPGet makes an HTTP GET request to the server and returns the response.
func (s *Server) HTTPGet(url string) (results string, err error) {
	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}
	body := string(MustReadAll(resp.Body))
	switch resp.StatusCode {
	case http.StatusBadRequest:
		return "", fmt.Errorf("unexpected status code: code=%d, body=%s", resp.StatusCode, body)
	case http.StatusOK, http.StatusNoContent:
		return body, nil
	default:
		return "", fmt.Errorf("unexpected status code: code=%d, body=%s", resp.StatusCode, body)
	}
}

// HTTPPost makes an HTTP POST request to the server and returns the response.
func (s *Server) HTTPPost(url string, content []byte) (results string, err error) {
	buf := bytes.NewBuffer(content)
	resp, err := http.Post(url, "application/json", buf)
	if err != nil {
		return "", err
	}
	body := string(MustReadAll(resp.Body))
	switch resp.StatusCode {
	case http.StatusBadRequest:
		return "", fmt.Errorf("unexpected status code: code=%d, body=%s", resp.StatusCode, body)
	case http.StatusOK, http.StatusNoContent:
		return body, nil
	default:
		return "", fmt.Errorf("unexpected status code: code=%d, body=%s", resp.StatusCode, body)
	}
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
	resp, err := http.Post(s.URL()+"/api/v1/write?"+params.Encode(), "", strings.NewReader(body))
	if err != nil {
		return "", err
	} else if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return "", fmt.Errorf("invalid status code: code=%d, body=%s", resp.StatusCode, MustReadAll(resp.Body))
	}
	return string(MustReadAll(resp.Body)), nil
}

func (s *Server) DefineTask(name, ttype, tick string, dbrps []kapacitor.DBRP) (results string, err error) {
	dbrpsStr, err := json.Marshal(dbrps)
	if err != nil {
		return
	}
	v := url.Values{}
	v.Add("name", name)
	v.Add("type", ttype)
	v.Add("dbrps", string(dbrpsStr))
	results, err = s.HTTPPost(s.URL()+"/api/v1/task?"+v.Encode(), []byte(tick))
	return
}

func (s *Server) EnableTask(name string) (string, error) {
	v := url.Values{}
	v.Add("name", name)
	return s.HTTPPost(s.URL()+"/api/v1/enable?"+v.Encode(), nil)
}

func (s *Server) DisableTask(name string) (string, error) {
	v := url.Values{}
	v.Add("name", name)
	return s.HTTPPost(s.URL()+"/api/v1/disable?"+v.Encode(), nil)
}

func (s *Server) DeleteTask(name string) error {
	v := url.Values{}
	v.Add("name", name)
	req, err := http.NewRequest("DELETE", s.URL()+"/api/v1/task?"+v.Encode(), nil)
	if err != nil {
		return err
	}
	client := &http.Client{}
	r, err := client.Do(req)
	if err != nil {
		return err
	}
	defer r.Body.Close()
	if r.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code got %d exp %d", r.StatusCode, http.StatusOK)
	}
	// Decode valid response
	type resp struct {
		Error string `json:"Error"`
	}
	d := json.NewDecoder(r.Body)
	rp := resp{}
	d.Decode(&rp)
	if rp.Error != "" {
		return errors.New(rp.Error)
	}
	return nil
}

func (s *Server) GetTask(name string) (ti task_store.TaskInfo, err error) {
	v := url.Values{}
	v.Add("name", name)
	resp, err := http.Get(s.URL() + "/api/v1/task?" + v.Encode())
	if err != nil {
		return
	}
	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("unexpected status code got %d exp %d", resp.StatusCode, http.StatusOK)
	}
	defer resp.Body.Close()
	d := json.NewDecoder(resp.Body)
	d.Decode(&ti)
	return
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

// NewConfig returns the default config with temporary paths.
func NewConfig() *run.Config {
	c := run.NewConfig()
	c.Reporting.Enabled = false
	c.Replay.Dir = MustTempFile()
	c.Task.Dir = MustTempFile()
	c.DataDir = MustTempFile()
	c.HTTP.BindAddress = "127.0.0.1:0"
	c.InfluxDB.Enabled = false
	return c
}

// MustTempFile returns a path to a temporary file.
func MustTempFile() string {
	f, err := ioutil.TempFile("", "influxd-")
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

func (l *LogService) NewStaticLevelLogger(prefix string, flag int, level wlog.Level) *log.Logger {
	return log.New(wlog.NewStaticLevelWriter(os.Stderr, level), prefix, flag)
}

type queryFunc func(q string) *client.Response

type InfluxDB struct {
	server *httptest.Server
}

func NewInfluxDB(callback queryFunc) *InfluxDB {
	handler := func(w http.ResponseWriter, r *http.Request) {
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
