// Kapacitor HTTP API client written in Go
package client

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

const DefaultUserAgent = "KapacitorClient"

// HTTP configuration for connecting to Kapacitor
type Config struct {
	// The URL of the Kapacitor server.
	URL string

	// Timeout for API requests, defaults to no timeout.
	Timeout time.Duration

	// UserAgent is the http User Agent, defaults to "KapacitorClient".
	UserAgent string

	// InsecureSkipVerify gets passed to the http client, if true, it will
	// skip https certificate verification. Defaults to false.
	InsecureSkipVerify bool

	// TLSConfig allows the user to set their own TLS config for the HTTP
	// Client. If set, this option overrides InsecureSkipVerify.
	TLSConfig *tls.Config
}

// Basic HTTP client
type Client struct {
	url        *url.URL
	userAgent  string
	httpClient *http.Client
}

// Create a new client.
func New(conf Config) (*Client, error) {
	if conf.UserAgent == "" {
		conf.UserAgent = DefaultUserAgent
	}

	u, err := url.Parse(conf.URL)
	if err != nil {
		return nil, err
	} else if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf(
			"Unsupported protocol scheme: %s, your address must start with http:// or https://",
			u.Scheme,
		)
	}

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: conf.InsecureSkipVerify,
		},
	}
	if conf.TLSConfig != nil {
		tr.TLSClientConfig = conf.TLSConfig
	}
	return &Client{
		url:       u,
		userAgent: conf.UserAgent,
		httpClient: &http.Client{
			Timeout:   conf.Timeout,
			Transport: tr,
		},
	}, nil
}

type DBRP struct {
	Database        string `json:"db"`
	RetentionPolicy string `json:"rp"`
}

func (d DBRP) String() string {
	return fmt.Sprintf("%q.%q", d.Database, d.RetentionPolicy)
}

// Statistics about the execution of a task.
type ExecutionStats struct {
	// Summary stats about the entire task
	TaskStats map[string]float64
	// Stats for each node in the task
	NodeStats map[string]map[string]float64
}

// Summary information about a task
type TaskSummary struct {
	Name           string
	Type           string
	DBRPs          []DBRP
	Enabled        bool
	Executing      bool
	ExecutionStats ExecutionStats
}

// Complete information about a task
type Task struct {
	Name           string
	Type           string
	DBRPs          []DBRP
	TICKscript     string
	Dot            string
	Enabled        bool
	Executing      bool
	Error          string
	ExecutionStats ExecutionStats
}

// Infomation about a recording
type Recording struct {
	ID      string
	Type    string
	Size    int64
	Created time.Time
	Error   string
}

// Set of recordings sorted by created date.
type Recordings []Recording

func (r Recordings) Len() int           { return len(r) }
func (r Recordings) Swap(i, j int)      { r[i], r[j] = r[j], r[i] }
func (r Recordings) Less(i, j int) bool { return r[i].Created.Before(r[j].Created) }

// Perform the request.
// If result is not nil the response body is JSON decoded into result.
// Codes is a list of valid response codes.
func (c *Client) do(req *http.Request, result interface{}, codes ...int) (*http.Response, error) {
	req.Header.Set("User-Agent", c.userAgent)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	valid := false
	for _, code := range codes {
		if resp.StatusCode == code {
			valid = true
			break
		}
	}
	if !valid {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		type errResp struct {
			Error string `json:"Error"`
		}
		d := json.NewDecoder(bytes.NewReader(body))
		rp := errResp{}
		d.Decode(&rp)
		if rp.Error != "" {
			return nil, errors.New(rp.Error)
		}
		return nil, fmt.Errorf("invalid repsonse: code %d: body: %s", resp.StatusCode, string(body))
	}
	if result != nil {
		d := json.NewDecoder(resp.Body)
		d.Decode(result)
	}
	return resp, nil
}

// Ping the server for a response.
// Ping returns how long the request took, the version of the server it connected to, and an error if one occurred.
func (c *Client) Ping() (time.Duration, string, error) {
	now := time.Now()
	u := *c.url
	u.Path = "ping"

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return 0, "", err
	}

	resp, err := c.do(req, nil, http.StatusNoContent)
	if err != nil {
		return 0, "", err
	}
	version := resp.Header.Get("X-Kapacitor-Version")
	return time.Since(now), version, nil
}

// Get summary information about tasks.
// If names list is empty all tasks are returned.
func (c *Client) ListTasks(names []string) ([]TaskSummary, error) {
	tasks := strings.Join(names, ",")
	v := url.Values{}
	v.Add("tasks", tasks)

	u := *c.url
	u.Path = "tasks"
	u.RawQuery = v.Encode()

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, err
	}

	// Response type
	type response struct {
		Error string        `json:"Error"`
		Tasks []TaskSummary `json:"Tasks"`
	}

	r := &response{}

	_, err = c.do(req, r, http.StatusOK)
	if err != nil {
		return nil, err
	}
	return r.Tasks, nil
}

// Get detailed information about a task.
// If dotLabels is true then the DOT string returned
// will use label attributes for the stats on the nodes and edges
// making it more useful to graph.
func (c *Client) Task(name string, dotLabels bool) (Task, error) {
	task := Task{}

	v := url.Values{}
	v.Add("name", name)
	if dotLabels {
		v.Add("labels", "true")
	}

	u := *c.url
	u.Path = "task"
	u.RawQuery = v.Encode()

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return task, err
	}

	_, err = c.do(req, &task, http.StatusOK)
	if err != nil {
		return task, err
	}
	return task, nil
}

// Get information about recordings.
// If rids is empty than all recordings are returned.
func (c *Client) ListRecordings(rids []string) (Recordings, error) {
	ids := strings.Join(rids, ",")
	v := url.Values{}
	v.Add("rids", ids)

	u := *c.url
	u.Path = "recordings"
	u.RawQuery = v.Encode()

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, err
	}
	// Decode valid response
	type response struct {
		Error      string     `json:"Error"`
		Recordings Recordings `json:"Recordings"`
	}

	r := &response{}

	_, err = c.do(req, r, http.StatusOK)
	if err != nil {
		return nil, err
	}
	sort.Sort(r.Recordings)
	return r.Recordings, nil
}

// Perform the record requests.
func (c *Client) doRecord(v url.Values) (string, error) {
	u := *c.url
	u.Path = "record"
	u.RawQuery = v.Encode()

	req, err := http.NewRequest("POST", u.String(), nil)
	if err != nil {
		return "", err
	}

	// Decode valid response
	type response struct {
		RecordingID string `json:"RecordingID"`
		Error       string `json:"Error"`
	}

	r := &response{}

	_, err = c.do(req, r, http.StatusOK)
	if err != nil {
		return "", err
	}
	return r.RecordingID, nil

}

// Record the stream for a task.
// Returns once the recording is started.
func (c *Client) RecordStream(name string, duration time.Duration) (string, error) {
	v := url.Values{}
	v.Add("type", "stream")
	v.Add("name", name)
	v.Add("duration", duration.String())

	return c.doRecord(v)
}

// Record the batch queries for a task.
// Returns once the recording is started.
func (c *Client) RecordBatch(name, cluster string, start, stop time.Time, past time.Duration) (string, error) {
	v := url.Values{}
	v.Add("type", "batch")
	v.Add("name", name)
	v.Add("cluster", cluster)
	if !start.IsZero() {
		v.Add("start", start.Format(time.RFC3339Nano))
	}
	if !stop.IsZero() {
		v.Add("stop", stop.Format(time.RFC3339Nano))
	}
	v.Add("past", past.String())

	return c.doRecord(v)
}

// Record the results of a query.
// The recordingType must be one of "stream", or "batch".
// Returns once the recording is started.
func (c *Client) RecordQuery(query, recordingType, cluster string) (string, error) {
	v := url.Values{}
	v.Add("type", "query")
	v.Add("query", query)
	v.Add("cluster", cluster)
	v.Add("ttype", recordingType)

	return c.doRecord(v)
}

// Get information about a recording.
// If the recording is currently being recorded then
// this method blocks until it is finished.
func (c *Client) Recording(rid string) (Recording, error) {
	r := Recording{}

	v := url.Values{}
	v.Add("id", rid)

	u := *c.url
	u.Path = "record"
	u.RawQuery = v.Encode()

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return r, err
	}

	_, err = c.do(req, &r, http.StatusOK)
	if err != nil {
		return r, err
	}
	return r, nil
}

// Replay a recording for a task.
func (c *Client) Replay(name, rid string, recordingTime, fast bool) error {
	v := url.Values{}
	v.Add("name", name)
	v.Add("id", rid)
	v.Add("rec-time", strconv.FormatBool(recordingTime))
	if fast {
		v.Add("clock", "fast")
	}

	u := *c.url
	u.Path = "replay"
	u.RawQuery = v.Encode()

	req, err := http.NewRequest("POST", u.String(), nil)
	if err != nil {
		return err
	}

	_, err = c.do(req, nil, http.StatusNoContent)
	if err != nil {
		return err
	}
	return nil
}

// Define a task.
// Name is always required.
// The other options are only modified if not empty or nil.
func (c *Client) Define(name, taskType string, dbrps []DBRP, tickScript io.Reader, reload bool) error {
	v := url.Values{}
	v.Add("name", name)
	v.Add("type", taskType)
	if len(dbrps) > 0 {
		b, err := json.Marshal(dbrps)
		if err != nil {
			return err
		}
		v.Add("dbrps", string(b))
	}

	u := *c.url
	u.Path = "task"
	u.RawQuery = v.Encode()

	req, err := http.NewRequest("POST", u.String(), tickScript)
	if err != nil {
		return err
	}

	_, err = c.do(req, nil, http.StatusNoContent)
	if err != nil {
		return err
	}
	if reload {
		tasks, err := c.ListTasks([]string{name})
		if err != nil {
			return err
		}
		if len(tasks) == 1 && tasks[0].Enabled {
			return c.Reload(name)
		}
	}
	return nil
}

// Enable a task.
func (c *Client) Enable(name string) error {
	v := url.Values{}
	v.Add("name", name)

	u := *c.url
	u.Path = "enable"
	u.RawQuery = v.Encode()

	req, err := http.NewRequest("POST", u.String(), nil)
	if err != nil {
		return err
	}

	_, err = c.do(req, nil, http.StatusNoContent)
	return err
}

// Disable a task.
func (c *Client) Disable(name string) error {
	v := url.Values{}
	v.Add("name", name)

	u := *c.url
	u.Path = "disable"
	u.RawQuery = v.Encode()

	req, err := http.NewRequest("POST", u.String(), nil)
	if err != nil {
		return err
	}

	_, err = c.do(req, nil, http.StatusNoContent)
	return err
}

// Reload a task, aka disable/enable.
func (c *Client) Reload(name string) error {
	err := c.Disable(name)
	if err != nil {
		return err
	}
	return c.Enable(name)
}

// Delete a task.
func (c *Client) DeleteTask(name string) error {
	v := url.Values{}
	v.Add("name", name)

	u := *c.url
	u.Path = "task"
	u.RawQuery = v.Encode()

	req, err := http.NewRequest("DELETE", u.String(), nil)
	if err != nil {
		return err
	}

	_, err = c.do(req, nil, http.StatusNoContent)
	return err
}

// Delete a recording.
func (c *Client) DeleteRecording(rid string) error {
	v := url.Values{}
	v.Add("rid", rid)

	u := *c.url
	u.Path = "recording"
	u.RawQuery = v.Encode()

	req, err := http.NewRequest("DELETE", u.String(), nil)
	if err != nil {
		return err
	}

	_, err = c.do(req, nil, http.StatusNoContent)
	return err
}

// Set the logging level.
// Level must be one of DEBUG, INFO, WARN, ERROR, or OFF
func (c *Client) LogLevel(level string) error {
	v := url.Values{}
	v.Add("level", level)

	u := *c.url
	u.Path = "loglevel"
	u.RawQuery = v.Encode()

	req, err := http.NewRequest("POST", u.String(), nil)
	if err != nil {
		return err
	}

	_, err = c.do(req, nil, http.StatusNoContent)
	return err
}
