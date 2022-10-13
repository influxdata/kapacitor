// This package is a set of convenience helpers and structs to make integration testing easier
package server_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	iclient "github.com/influxdata/influxdb/client/v2"
	"github.com/influxdata/kapacitor/client/v1"
	"github.com/influxdata/kapacitor/server"
	"github.com/influxdata/kapacitor/services/diagnostic"
	"github.com/influxdata/kapacitor/services/mqtt"
	"github.com/influxdata/kapacitor/services/slack"
	"github.com/influxdata/wlog"
)

// Server represents a test wrapper for server.Server.
type Server struct {
	*server.Server
	Config    *server.Config
	buildInfo server.BuildInfo
	ds        *diagnostic.Service
}

// NewServer returns a new instance of Server.
func NewServer(c *server.Config, disabled map[string]struct{}) *Server {
	configureLogging()
	buildInfo := server.BuildInfo{
		Version: "testServer",
		Commit:  "testCommit",
		Branch:  "testBranch",
	}
	c.HTTP.LogEnabled = testing.Verbose()
	ds := diagnostic.NewService(diagnostic.NewConfig(), io.Discard, io.Discard)
	ds.Open()
	srv, err := server.New(c, buildInfo, ds, disabled)
	if err != nil {
		panic(err)
	}
	s := Server{
		Server:    srv,
		Config:    c,
		buildInfo: buildInfo,
		ds:        ds,
	}
	return &s
}

func (s *Server) Stop() {
	s.Server.Close()
}

func (s *Server) Start() {
	srv, err := server.New(s.Config, s.buildInfo, s.ds, s.Server.DisabledHandlers)
	if err != nil {
		panic(err.Error())
	}
	if err := srv.Open(); err != nil {
		panic(err.Error())
	}
	s.Server = srv
}

func (s *Server) Restart() {
	s.Stop()
	s.Start()
}

// OpenServer opens a test server.
func OpenDefaultServer(t *testing.T) (*Server, *client.Client) {
	c := NewConfig(t)
	s := OpenServer(c)
	client := Client(s)
	return s, client
}

func OpenLoadServer(t *testing.T) (*Server, *server.Config, *client.Client) {
	c := NewConfig(t)
	if err := copyFiles("testdata/load", c.Load.Dir); err != nil {
		panic(err)
	}
	s := OpenServer(c)
	client := Client(s)
	return s, c, client
}

// OpenServer opens a test server.
func OpenServer(c *server.Config) *Server {
	s := NewServer(c, nil)
	if err := s.Open(); err != nil {
		panic(err.Error())
	}
	return s
}

// OpenServer opens a test server.
func OpenServerWithDisabledHandlers(c *server.Config, disabledAlertHandlers map[string]struct{}) *Server {
	s := NewServer(c, disabledAlertHandlers)
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

func (s *Server) Open() error {
	err := s.Server.Open()
	if err != nil {
		return err
	}
	u, _ := url.Parse(s.URL())
	s.Config.HTTP.BindAddress = u.Host
	return nil
}

// Close shuts down the server and removes all temporary paths.
func (s *Server) Close() {
	s.Server.Close()
	os.RemoveAll(s.Config.Replay.Dir)
	os.RemoveAll(filepath.Dir(s.Config.Storage.BoltDBPath))
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
	b, err := io.ReadAll(r)
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

func (s *Server) PingAsUser(username, password string) error {
	cli, err := client.New(client.Config{
		URL: s.URL(),
		Credentials: &client.Credentials{
			Method:   client.UserAuthentication,
			Username: username,
			Password: password,
		},
	})
	if err != nil {
		return err
	}
	_, _, err = cli.Ping()
	return err
}

func (s *Server) PingWithToken(token string) error {
	cli, err := client.New(client.Config{
		URL: s.URL(),
		Credentials: &client.Credentials{
			Method: client.BearerAuthentication,
			Token:  token,
		},
	})
	if err != nil {
		return err
	}
	_, _, err = cli.Ping()
	return err
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
func NewConfig(t *testing.T) *server.Config {
	c := server.NewConfig()
	c.MQTT = mqtt.Configs{}
	c.Slack = slack.Configs{slack.NewConfig()}
	c.Reporting.Enabled = false
	c.Replay.Dir = t.TempDir()
	c.Storage.BoltDBPath = filepath.Join(t.TempDir(), "bolt.db")
	c.DataDir = t.TempDir()
	c.HTTP.BindAddress = "127.0.0.1:0"
	//c.HTTP.BindAddress = "127.0.0.1:9092"
	//c.HTTP.GZIP = false
	c.InfluxDB[0].Enabled = false
	c.Load.Enabled = true
	c.Load.Dir = t.TempDir()
	return c
}

func configureLogging() {
	if testing.Verbose() {
		wlog.SetLevel(wlog.DEBUG)
	} else {
		wlog.SetLevel(wlog.OFF)
	}
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

func copyFiles(src, dst string) error {
	fs, err := os.ReadDir(src)
	if err != nil {
		return err
	}

	for _, f := range fs {
		if f.IsDir() {
			if err := os.Mkdir(path.Join(dst, f.Name()), os.ModePerm); err != nil {
				return err
			}
			// copy deeper files
			if err := copyFiles(path.Join(src, f.Name()), path.Join(dst, f.Name())); err != nil {
				return err
			}
		} else {
			copyFile(path.Join(src, f.Name()), path.Join(dst, f.Name()))
		}
	}

	return nil

}

func copyFile(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	dstFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	if _, err := io.Copy(dstFile, srcFile); err != nil {
		return err
	}

	if err := dstFile.Sync(); err != nil {
		return err
	}

	return nil
}
