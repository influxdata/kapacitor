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
	"time"

	"github.com/influxdata/kapacitor"
	"github.com/influxdata/kapacitor/cmd/kapacitord/run"
	"github.com/influxdata/kapacitor/services/task_store"
	"github.com/influxdata/kapacitor/wlog"
	client "github.com/influxdb/influxdb/client/v2"
)

// Server represents a test wrapper for run.Server.
type Server struct {
	*run.Server
	Config *run.Config
}

// NewServer returns a new instance of Server.
func NewServer(c *run.Config) *Server {
	configureLogging()
	buildInfo := &run.BuildInfo{
		Version: "testServer",
		Commit:  "testCommit",
		Branch:  "testBranch",
	}
	ls := &LogService{}
	srv, err := run.NewServer(c, buildInfo, ls)
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
		return s.HTTPDService.URL()
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

func (s *Server) HTTPGetRetry(url, exp string, retries int, sleep time.Duration) error {
	var r string
	for retries > 0 {
		resp, err := http.Get(url)
		if err != nil {
			return err
		}
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

// HTTPPost makes an HTTP POST request to the server and returns the response.
func (s *Server) HTTPPost(url string, content []byte) (results string, err error) {
	buf := bytes.NewBuffer(content)
	resp, err := http.Post(url, "application/json", buf)
	if err != nil {
		return "", err
	}
	body := MustReadAll(resp.Body)
	type response struct {
		Error string `json:"Error"`
	}
	d := json.NewDecoder(bytes.NewReader(body))
	rp := response{}
	d.Decode(&rp)
	if rp.Error != "" {
		return "", errors.New(rp.Error)
	}
	switch resp.StatusCode {
	case http.StatusBadRequest:
		return "", fmt.Errorf("unexpected status code: code=%d, body=%s", resp.StatusCode, string(body))
	case http.StatusOK, http.StatusNoContent:
		return string(body), nil
	default:
		return "", fmt.Errorf("unexpected status code: code=%d, body=%s", resp.StatusCode, string(body))
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
	resp, err := http.Post(s.URL()+"/write?"+params.Encode(), "", strings.NewReader(body))
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
	results, err = s.HTTPPost(s.URL()+"/task?"+v.Encode(), []byte(tick))
	return
}

func (s *Server) EnableTask(name string) (string, error) {
	v := url.Values{}
	v.Add("name", name)
	return s.HTTPPost(s.URL()+"/enable?"+v.Encode(), nil)
}

func (s *Server) DisableTask(name string) (string, error) {
	v := url.Values{}
	v.Add("name", name)
	return s.HTTPPost(s.URL()+"/disable?"+v.Encode(), nil)
}

func (s *Server) DeleteTask(name string) error {
	v := url.Values{}
	v.Add("name", name)
	req, err := http.NewRequest("DELETE", s.URL()+"/task?"+v.Encode(), nil)
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
	resp, err := http.Get(s.URL() + "/task?" + v.Encode())
	if err != nil {
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("unexpected status code got %d exp %d", resp.StatusCode, http.StatusOK)
		return
	}
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

func (s *Server) DoStreamRecording(name string, duration time.Duration, started chan struct{}) (id string, err error) {
	v := url.Values{}
	v.Add("type", "stream")
	v.Add("name", name)
	v.Add("duration", duration.String())
	r, err := http.Post(s.URL()+"/record?"+v.Encode(), "", nil)
	if err != nil {
		return
	}
	defer r.Body.Close()
	if r.StatusCode != http.StatusOK {
		err = fmt.Errorf("unexpected status code got %d exp %d", r.StatusCode, http.StatusOK)
		return
	}

	// Decode valid response
	type resp struct {
		RecordingID string `json:"RecordingID"`
		Error       string `json:"Error"`
	}
	rp := resp{}
	d := json.NewDecoder(r.Body)
	d.Decode(&rp)
	if rp.Error != "" {
		err = errors.New(rp.Error)
		return
	}
	id = rp.RecordingID
	close(started)
	v = url.Values{}
	v.Add("id", id)
	_, err = s.HTTPGet(s.URL() + "/record?" + v.Encode())
	return
}

func (s *Server) DoBatchRecording(name string, past time.Duration) (id string, err error) {
	v := url.Values{}
	v.Add("type", "batch")
	v.Add("name", name)
	v.Add("past", past.String())
	r, err := http.Post(s.URL()+"/record?"+v.Encode(), "", nil)
	if err != nil {
		return
	}
	defer r.Body.Close()
	if r.StatusCode != http.StatusOK {
		err = fmt.Errorf("unexpected status code got %d exp %d", r.StatusCode, http.StatusOK)
		return
	}

	// Decode valid response
	type resp struct {
		RecordingID string `json:"RecordingID"`
		Error       string `json:"Error"`
	}
	rp := resp{}
	d := json.NewDecoder(r.Body)
	d.Decode(&rp)
	if rp.Error != "" {
		err = errors.New(rp.Error)
		return
	}
	id = rp.RecordingID
	v = url.Values{}
	v.Add("id", id)
	_, err = s.HTTPGet(s.URL() + "/record?" + v.Encode())
	return
}

func (s *Server) DoReplay(name, id string) (string, error) {
	v := url.Values{}
	v.Add("name", name)
	v.Add("id", id)
	v.Add("clock", "fast")
	v.Add("rec-time", "true")
	return s.HTTPPost(s.URL()+"/replay?"+v.Encode(), nil)
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
