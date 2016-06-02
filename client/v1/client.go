// Kapacitor HTTP API client written in Go
package client

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"time"

	"github.com/influxdata/influxdb/influxql"
	"github.com/pkg/errors"
)

const DefaultUserAgent = "KapacitorClient"

// These are the constant enpoints for the API.
// The server will always return a `link` to resources,
// so path manipulation should not be necessary.
// The only exception is if you only have an ID for a resource
// then use the appropriate *Link methods.

const basePath = "/kapacitor/v1"
const pingPath = basePath + "/ping"
const logLevelPath = basePath + "/loglevel"
const debugVarsPath = basePath + "/debug/vars"
const tasksPath = basePath + "/tasks"
const templatesPath = basePath + "/templates"
const recordingsPath = basePath + "/recordings"
const recordStreamPath = basePath + "/recordings/stream"
const recordBatchPath = basePath + "/recordings/batch"
const recordQueryPath = basePath + "/recordings/query"
const replaysPath = basePath + "/replays"
const replayBatchPath = basePath + "/replays/batch"
const replayQueryPath = basePath + "/replays/query"

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

type Relation int

const (
	Self Relation = iota
	Next
	Previous
)

func (r Relation) MarshalText() ([]byte, error) {
	switch r {
	case Self:
		return []byte("self"), nil
	case Next:
		return []byte("next"), nil
	case Previous:
		return []byte("prev"), nil
	default:
		return nil, fmt.Errorf("unknown Relation %d", r)
	}
}

func (r *Relation) UnmarshalText(text []byte) error {
	switch s := string(text); s {
	case "self":
		*r = Self
	case "next":
		*r = Next
	case "prev":
		*r = Previous
	default:
		return fmt.Errorf("unknown Relation %s", s)
	}
	return nil
}

func (r Relation) String() string {
	s, err := r.MarshalText()
	if err != nil {
		return err.Error()
	}
	return string(s)
}

type Link struct {
	Relation Relation `json:"rel"`
	Href     string   `json:"href"`
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
	TaskStats map[string]interface{} `json:"task-stats"`
	// Stats for each node in the task
	NodeStats map[string]map[string]interface{} `json:"node-stats"`
}

type TaskType int

const (
	StreamTask TaskType = 1
	BatchTask  TaskType = 2
)

func (tt TaskType) MarshalText() ([]byte, error) {
	switch tt {
	case StreamTask:
		return []byte("stream"), nil
	case BatchTask:
		return []byte("batch"), nil
	default:
		return nil, fmt.Errorf("unknown TaskType %d", tt)
	}
}

func (tt *TaskType) UnmarshalText(text []byte) error {
	switch s := string(text); s {
	case "stream":
		*tt = StreamTask
	case "batch":
		*tt = BatchTask
	default:
		return fmt.Errorf("unknown TaskType %s", s)
	}
	return nil
}
func (tt TaskType) String() string {
	s, err := tt.MarshalText()
	if err != nil {
		return err.Error()
	}
	return string(s)
}

type TaskStatus int

const (
	Disabled TaskStatus = 1
	Enabled  TaskStatus = 2
)

func (ts TaskStatus) MarshalText() ([]byte, error) {
	switch ts {
	case Disabled:
		return []byte("disabled"), nil
	case Enabled:
		return []byte("enabled"), nil
	default:
		return nil, fmt.Errorf("unknown TaskStatus %d", ts)
	}
}

func (ts *TaskStatus) UnmarshalText(text []byte) error {
	switch s := string(text); s {
	case "enabled":
		*ts = Enabled
	case "disabled":
		*ts = Disabled
	default:
		return fmt.Errorf("unknown TaskStatus %s", s)
	}
	return nil
}

func (ts TaskStatus) String() string {
	s, err := ts.MarshalText()
	if err != nil {
		return err.Error()
	}
	return string(s)
}

type Status int

const (
	Failed Status = iota
	Running
	Finished
)

func (s Status) MarshalText() ([]byte, error) {
	switch s {
	case Failed:
		return []byte("failed"), nil
	case Running:
		return []byte("running"), nil
	case Finished:
		return []byte("finished"), nil
	default:
		return nil, fmt.Errorf("unknown Status %d", s)
	}
}

func (s *Status) UnmarshalText(text []byte) error {
	switch t := string(text); t {
	case "failed":
		*s = Failed
	case "running":
		*s = Running
	case "finished":
		*s = Finished
	default:
		return fmt.Errorf("unknown Status %s", t)
	}
	return nil
}

func (s Status) String() string {
	t, err := s.MarshalText()
	if err != nil {
		return err.Error()
	}
	return string(t)
}

type Clock int

const (
	Fast Clock = iota
	Real
)

func (c Clock) MarshalText() ([]byte, error) {
	switch c {
	case Fast:
		return []byte("fast"), nil
	case Real:
		return []byte("real"), nil
	default:
		return nil, fmt.Errorf("unknown Clock %d", c)
	}
}

func (c *Clock) UnmarshalText(text []byte) error {
	switch s := string(text); s {
	case "fast":
		*c = Fast
	case "real":
		*c = Real
	default:
		return fmt.Errorf("unknown Clock %s", s)
	}
	return nil
}

func (c Clock) String() string {
	s, err := c.MarshalText()
	if err != nil {
		return err.Error()
	}
	return string(s)
}

type VarType int

const (
	VarUnknown VarType = iota
	VarBool
	VarInt
	VarFloat
	VarString
	VarRegex
	VarDuration
	VarLambda
	VarList
	VarStar
)

func (vt VarType) MarshalText() ([]byte, error) {
	switch vt {
	case VarBool:
		return []byte("bool"), nil
	case VarInt:
		return []byte("int"), nil
	case VarFloat:
		return []byte("float"), nil
	case VarString:
		return []byte("string"), nil
	case VarRegex:
		return []byte("regex"), nil
	case VarDuration:
		return []byte("duration"), nil
	case VarLambda:
		return []byte("lambda"), nil
	case VarList:
		return []byte("list"), nil
	case VarStar:
		return []byte("star"), nil
	default:
		return nil, fmt.Errorf("unknown VarType %d", vt)
	}
}

func (vt *VarType) UnmarshalText(text []byte) error {
	switch s := string(text); s {
	case "bool":
		*vt = VarBool
	case "int":
		*vt = VarInt
	case "float":
		*vt = VarFloat
	case "string":
		*vt = VarString
	case "regex":
		*vt = VarRegex
	case "duration":
		*vt = VarDuration
	case "lambda":
		*vt = VarLambda
	case "list":
		*vt = VarList
	case "star":
		*vt = VarStar
	default:
		return fmt.Errorf("unknown VarType %s", s)
	}
	return nil
}

func (vt VarType) String() string {
	s, err := vt.MarshalText()
	if err != nil {
		return err.Error()
	}
	return string(s)
}

type Vars map[string]Var

func (vs *Vars) UnmarshalJSON(b []byte) error {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	data := make(map[string]Var)
	err := dec.Decode(&data)
	if err != nil {
		return err
	}
	*vs = make(Vars)
	for name, v := range data {
		if v.Value != nil {
			switch v.Type {
			case VarDuration:
				switch value := v.Value.(type) {
				case json.Number:
					i, err := value.Int64()
					if err != nil {
						return errors.Wrapf(err, "invalid var %v", v)
					}
					v.Value = time.Duration(i)
				case string:
					d, err := influxql.ParseDuration(value)
					if err != nil {
						return errors.Wrapf(err, "invalid duration string for var %s", v)
					}
					v.Value = d
				default:
					return fmt.Errorf("invalid var %v: expected int or string value", v)
				}
			case VarInt:
				n, ok := v.Value.(json.Number)
				if !ok {
					return fmt.Errorf("invalid var %v: expected int value", v)
				}
				v.Value, err = n.Int64()
				if err != nil {
					return errors.Wrapf(err, "invalid var %v", v)
				}
			case VarFloat:
				n, ok := v.Value.(json.Number)
				if !ok {
					return fmt.Errorf("invalid var %v: expected float value", v)
				}
				v.Value, err = n.Float64()
				if err != nil {
					return errors.Wrapf(err, "invalid var %v", v)
				}
			case VarList:
				values, ok := v.Value.([]interface{})
				if !ok {
					return fmt.Errorf("invalid var %v: expected list of vars", v)
				}
				vars := make([]Var, len(values))
				for i := range values {
					m, ok := values[i].(map[string]interface{})
					if !ok {
						return fmt.Errorf("invalid var %v: expected list of vars", v)
					}
					if typeText, ok := m["type"]; ok {
						err := vars[i].Type.UnmarshalText([]byte(typeText.(string)))
						if err != nil {
							return err
						}
					} else {
						return fmt.Errorf("invalid var %v: expected list type key in object", v)
					}
					if value, ok := m["value"]; ok {
						vars[i].Value = value
					} else {
						return fmt.Errorf("invalid var %v: expected list value key in object", v)
					}
				}
				v.Value = vars
			}
		}
		(*vs)[name] = v
	}
	return nil
}

type Var struct {
	Type        VarType     `json:"type"`
	Value       interface{} `json:"value"`
	Description string      `json:"description"`
}

// A Task plus its read-only attributes.
type Task struct {
	Link           Link           `json:"link"`
	ID             string         `json:"id"`
	TemplateID     string         `json:"template-id"`
	Type           TaskType       `json:"type"`
	DBRPs          []DBRP         `json:"dbrps"`
	TICKscript     string         `json:"script"`
	Vars           Vars           `json:"vars"`
	Dot            string         `json:"dot"`
	Status         TaskStatus     `json:"status"`
	Executing      bool           `json:"executing"`
	Error          string         `json:"error"`
	ExecutionStats ExecutionStats `json:"stats"`
	Created        time.Time      `json:"created"`
	Modified       time.Time      `json:"modified"`
	LastEnabled    time.Time      `json:"last-enabled,omitempty"`
}

// A Template plus its read-only attributes.
type Template struct {
	Link       Link      `json:"link"`
	ID         string    `json:"id"`
	Type       TaskType  `json:"type"`
	TICKscript string    `json:"script"`
	Vars       Vars      `json:"vars"`
	Dot        string    `json:"dot"`
	Error      string    `json:"error"`
	Created    time.Time `json:"created"`
	Modified   time.Time `json:"modified"`
}

// Information about a recording.
type Recording struct {
	Link     Link      `json:"link"`
	ID       string    `json:"id"`
	Type     TaskType  `json:"type"`
	Size     int64     `json:"size"`
	Date     time.Time `json:"date"`
	Error    string    `json:"error"`
	Status   Status    `json:"status"`
	Progress float64   `json:"progress"`
}

// Information about a replay.
type Replay struct {
	Link          Link      `json:"link"`
	ID            string    `json:"id"`
	Task          string    `json:"task"`
	Recording     string    `json:"recording"`
	RecordingTime bool      `json:"recording-time"`
	Clock         Clock     `json:"clock"`
	Date          time.Time `json:"date"`
	Error         string    `json:"error"`
	Status        Status    `json:"status"`
	Progress      float64   `json:"progress"`
}

func (c *Client) URL() string {
	return c.url.String()
}

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
			Error string `json:"error"`
		}
		d := json.NewDecoder(bytes.NewReader(body))
		rp := errResp{}
		d.Decode(&rp)
		if rp.Error != "" {
			return nil, errors.New(rp.Error)
		}
		return nil, fmt.Errorf("invalid response: code %d: body: %s", resp.StatusCode, string(body))
	}
	if result != nil {
		d := json.NewDecoder(resp.Body)
		err := d.Decode(result)
		if err != nil {
			return nil, fmt.Errorf("failed to decode JSON: %v", err)
		}
	}
	return resp, nil
}

// Ping the server for a response.
// Ping returns how long the request took, the version of the server it connected to, and an error if one occurred.
func (c *Client) Ping() (time.Duration, string, error) {
	now := time.Now()
	u := *c.url
	u.Path = pingPath

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

func (c *Client) TaskLink(id string) Link {
	return Link{Relation: Self, Href: path.Join(tasksPath, id)}
}

func (c *Client) TemplateLink(id string) Link {
	return Link{Relation: Self, Href: path.Join(templatesPath, id)}
}

type CreateTaskOptions struct {
	ID         string     `json:"id,omitempty"`
	TemplateID string     `json:"template-id,omitempty"`
	Type       TaskType   `json:"type,omitempty"`
	DBRPs      []DBRP     `json:"dbrps,omitempty"`
	TICKscript string     `json:"script,omitempty"`
	Status     TaskStatus `json:"status,omitempty"`
	Vars       Vars       `json:"vars,omitempty"`
}

// Create a new task.
// Errors if the task already exists.
func (c *Client) CreateTask(opt CreateTaskOptions) (Task, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	err := enc.Encode(opt)
	if err != nil {
		return Task{}, err
	}

	u := *c.url
	u.Path = tasksPath

	req, err := http.NewRequest("POST", u.String(), &buf)
	if err != nil {
		return Task{}, err
	}

	t := Task{}
	_, err = c.do(req, &t, http.StatusOK)
	return t, err
}

type UpdateTaskOptions struct {
	TemplateID string     `json:"template-id,omitempty"`
	Type       TaskType   `json:"type,omitempty"`
	DBRPs      []DBRP     `json:"dbrps,omitempty"`
	TICKscript string     `json:"script,omitempty"`
	Status     TaskStatus `json:"status,omitempty"`
	Vars       Vars       `json:"vars,omitempty"`
}

// Update an existing task.
// Only fields that are not their default value will be updated.
func (c *Client) UpdateTask(link Link, opt UpdateTaskOptions) error {
	if link.Href == "" {
		return fmt.Errorf("invalid link %v", link)
	}

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	err := enc.Encode(opt)
	if err != nil {
		return err
	}

	u := *c.url
	u.Path = link.Href

	req, err := http.NewRequest("PATCH", u.String(), &buf)
	if err != nil {
		return err
	}

	_, err = c.do(req, nil, http.StatusNoContent)
	if err != nil {
		return err
	}
	return nil
}

type TaskOptions struct {
	DotView      string
	ScriptFormat string
}

func (o *TaskOptions) Default() {
	if o.DotView == "" {
		o.DotView = "attributes"
	}
	if o.ScriptFormat == "" {
		o.ScriptFormat = "formatted"
	}
}

func (o *TaskOptions) Values() *url.Values {
	v := &url.Values{}
	v.Set("dot-view", o.DotView)
	v.Set("script-format", o.ScriptFormat)
	return v
}

// Get information about a task.
// Options can be nil and the default options will be used.
// By default the DOT content will use attributes for stats. Use DotView="labels" to generate a purley labels based DOT content, which can accurately be rendered but is less readable.
// By default the TICKscript contents are formatted, use ScriptFormat="raw" to return the TICKscript unmodified.
func (c *Client) Task(link Link, opt *TaskOptions) (Task, error) {
	task := Task{}
	if link.Href == "" {
		return task, fmt.Errorf("invalid link %v", link)
	}

	if opt == nil {
		opt = new(TaskOptions)
	}
	opt.Default()

	u := *c.url
	u.Path = link.Href
	u.RawQuery = opt.Values().Encode()

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

// Delete a task.
func (c *Client) DeleteTask(link Link) error {
	if link.Href == "" {
		return fmt.Errorf("invalid link %v", link)
	}

	u := *c.url
	u.Path = link.Href

	req, err := http.NewRequest("DELETE", u.String(), nil)
	if err != nil {
		return err
	}

	_, err = c.do(req, nil, http.StatusNoContent)
	return err
}

type ListTasksOptions struct {
	TaskOptions
	Pattern string
	Fields  []string
	Offset  int
	Limit   int
}

func (o *ListTasksOptions) Default() {
	o.TaskOptions.Default()
	if o.Limit == 0 {
		o.Limit = 100
	}
}

func (o *ListTasksOptions) Values() *url.Values {
	v := o.TaskOptions.Values()
	v.Set("pattern", o.Pattern)
	for _, field := range o.Fields {
		v.Add("fields", field)
	}
	v.Set("offset", strconv.FormatInt(int64(o.Offset), 10))
	v.Set("limit", strconv.FormatInt(int64(o.Limit), 10))
	return v
}

// Get tasks.
func (c *Client) ListTasks(opt *ListTasksOptions) ([]Task, error) {
	if opt == nil {
		opt = new(ListTasksOptions)
	}
	opt.Default()

	u := *c.url
	u.Path = tasksPath
	u.RawQuery = opt.Values().Encode()

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, err
	}

	// Response type
	type response struct {
		Tasks []Task `json:"tasks"`
	}

	r := &response{}

	_, err = c.do(req, r, http.StatusOK)
	if err != nil {
		return nil, err
	}
	return r.Tasks, nil
}

func (c *Client) TaskOutput(link Link, name string) (*influxql.Result, error) {
	u := *c.url
	u.Path = path.Join(link.Href, name)

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, err
	}
	r := &influxql.Result{}
	_, err = c.do(req, r, http.StatusOK)
	if err != nil {
		return nil, err
	}
	return r, nil
}

type CreateTemplateOptions struct {
	ID         string   `json:"id,omitempty"`
	Type       TaskType `json:"type,omitempty"`
	TICKscript string   `json:"script,omitempty"`
}

// Create a new template.
// Errors if the template already exists.
func (c *Client) CreateTemplate(opt CreateTemplateOptions) (Template, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	err := enc.Encode(opt)
	if err != nil {
		return Template{}, err
	}

	u := *c.url
	u.Path = templatesPath

	req, err := http.NewRequest("POST", u.String(), &buf)
	if err != nil {
		return Template{}, err
	}

	t := Template{}
	_, err = c.do(req, &t, http.StatusOK)
	return t, err
}

type UpdateTemplateOptions struct {
	Type       TaskType `json:"type,omitempty"`
	TICKscript string   `json:"script,omitempty"`
}

// Update an existing template.
// Only fields that are not their default value will be updated.
func (c *Client) UpdateTemplate(link Link, opt UpdateTemplateOptions) error {
	if link.Href == "" {
		return fmt.Errorf("invalid link %v", link)
	}

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	err := enc.Encode(opt)
	if err != nil {
		return err
	}

	u := *c.url
	u.Path = link.Href

	req, err := http.NewRequest("PATCH", u.String(), &buf)
	if err != nil {
		return err
	}

	_, err = c.do(req, nil, http.StatusNoContent)
	if err != nil {
		return err
	}
	return nil
}

type TemplateOptions struct {
	ScriptFormat string
}

func (o *TemplateOptions) Default() {
	if o.ScriptFormat == "" {
		o.ScriptFormat = "formatted"
	}
}

func (o *TemplateOptions) Values() *url.Values {
	v := &url.Values{}
	v.Set("script-format", o.ScriptFormat)
	return v
}

// Get information about a template.
// Options can be nil and the default options will be used.
// By default the TICKscript contents are formatted, use ScriptFormat="raw" to return the TICKscript unmodified.
func (c *Client) Template(link Link, opt *TemplateOptions) (Template, error) {
	template := Template{}
	if link.Href == "" {
		return template, fmt.Errorf("invalid link %v", link)
	}

	if opt == nil {
		opt = new(TemplateOptions)
	}
	opt.Default()

	u := *c.url
	u.Path = link.Href
	u.RawQuery = opt.Values().Encode()

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return template, err
	}

	_, err = c.do(req, &template, http.StatusOK)
	if err != nil {
		return template, err
	}
	return template, nil
}

// Delete a template.
func (c *Client) DeleteTemplate(link Link) error {
	if link.Href == "" {
		return fmt.Errorf("invalid link %v", link)
	}

	u := *c.url
	u.Path = link.Href

	req, err := http.NewRequest("DELETE", u.String(), nil)
	if err != nil {
		return err
	}

	_, err = c.do(req, nil, http.StatusNoContent)
	return err
}

type ListTemplatesOptions struct {
	TemplateOptions
	Pattern string
	Fields  []string
	Offset  int
	Limit   int
}

func (o *ListTemplatesOptions) Default() {
	o.TemplateOptions.Default()
	if o.Limit == 0 {
		o.Limit = 100
	}
}

func (o *ListTemplatesOptions) Values() *url.Values {
	v := o.TemplateOptions.Values()
	v.Set("pattern", o.Pattern)
	for _, field := range o.Fields {
		v.Add("fields", field)
	}
	v.Set("offset", strconv.FormatInt(int64(o.Offset), 10))
	v.Set("limit", strconv.FormatInt(int64(o.Limit), 10))
	return v
}

// Get templates.
func (c *Client) ListTemplates(opt *ListTemplatesOptions) ([]Template, error) {
	if opt == nil {
		opt = new(ListTemplatesOptions)
	}
	opt.Default()

	u := *c.url
	u.Path = templatesPath
	u.RawQuery = opt.Values().Encode()

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, err
	}

	// Response type
	type response struct {
		Templates []Template `json:"templates"`
	}

	r := &response{}

	_, err = c.do(req, r, http.StatusOK)
	if err != nil {
		return nil, err
	}
	return r.Templates, nil
}

// Get information about a recording.
func (c *Client) Recording(link Link) (Recording, error) {
	r := Recording{}
	if link.Href == "" {
		return r, fmt.Errorf("invalid link %v", link)
	}

	u := *c.url
	u.Path = link.Href

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return r, err
	}

	_, err = c.do(req, &r, http.StatusOK, http.StatusAccepted)
	if err != nil {
		return r, err
	}
	return r, nil
}

func (c *Client) RecordingLink(id string) Link {
	return Link{Relation: Self, Href: path.Join(recordingsPath, id)}
}

type RecordStreamOptions struct {
	ID   string    `json:"id,omitempty"`
	Task string    `json:"task"`
	Stop time.Time `json:"stop"`
}

// Record the stream for a task.
// Returns once the recording is started.
func (c *Client) RecordStream(opt RecordStreamOptions) (Recording, error) {
	r := Recording{}

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	err := enc.Encode(opt)
	if err != nil {
		return r, err
	}

	u := *c.url
	u.Path = recordStreamPath

	req, err := http.NewRequest("POST", u.String(), &buf)
	if err != nil {
		return r, err
	}

	_, err = c.do(req, &r, http.StatusCreated)
	if err != nil {
		return r, err
	}
	return r, nil
}

type RecordBatchOptions struct {
	ID      string    `json:"id,omitempty"`
	Task    string    `json:"task"`
	Start   time.Time `json:"start"`
	Stop    time.Time `json:"stop"`
	Cluster string    `json:"cluster,omitempty"`
}

// Record the batch queries for a task.
// Returns once the recording is started.
func (c *Client) RecordBatch(opt RecordBatchOptions) (Recording, error) {
	r := Recording{}

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	err := enc.Encode(opt)
	if err != nil {
		return r, err
	}

	u := *c.url
	u.Path = recordBatchPath

	req, err := http.NewRequest("POST", u.String(), &buf)
	if err != nil {
		return r, err
	}

	_, err = c.do(req, &r, http.StatusCreated)
	if err != nil {
		return r, err
	}
	return r, nil
}

type RecordQueryOptions struct {
	ID      string   `json:"id,omitempty"`
	Query   string   `json:"query"`
	Type    TaskType `json:"type"`
	Cluster string   `json:"cluster,omitempty"`
}

// Record the results of a query.
// The recordingType must be one of "stream", or "batch".
// Returns once the recording is started.
func (c *Client) RecordQuery(opt RecordQueryOptions) (Recording, error) {
	r := Recording{}

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	err := enc.Encode(opt)
	if err != nil {
		return r, err
	}

	u := *c.url
	u.Path = recordQueryPath

	req, err := http.NewRequest("POST", u.String(), &buf)
	if err != nil {
		return r, err
	}

	_, err = c.do(req, &r, http.StatusCreated)
	if err != nil {
		return r, err
	}
	return r, nil
}

// Delete a recording.
func (c *Client) DeleteRecording(link Link) error {
	if link.Href == "" {
		return fmt.Errorf("invalid link %v", link)
	}
	u := *c.url
	u.Path = link.Href

	req, err := http.NewRequest("DELETE", u.String(), nil)
	if err != nil {
		return err
	}

	_, err = c.do(req, nil, http.StatusNoContent)
	return err
}

type ListRecordingsOptions struct {
	Pattern string
	Fields  []string
	Offset  int
	Limit   int
}

func (o *ListRecordingsOptions) Default() {
	if o.Limit == 0 {
		o.Limit = 100
	}
}

func (o *ListRecordingsOptions) Values() *url.Values {
	v := &url.Values{}
	v.Set("pattern", o.Pattern)
	for _, field := range o.Fields {
		v.Add("fields", field)
	}
	v.Set("offset", strconv.FormatInt(int64(o.Offset), 10))
	v.Set("limit", strconv.FormatInt(int64(o.Limit), 10))
	return v
}

// Get information about recordings.
// If rids is empty than all recordings are returned.
func (c *Client) ListRecordings(opt *ListRecordingsOptions) ([]Recording, error) {
	if opt == nil {
		opt = new(ListRecordingsOptions)
	}
	opt.Default()
	u := *c.url
	u.Path = recordingsPath
	u.RawQuery = opt.Values().Encode()

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, err
	}
	// Decode valid response
	type response struct {
		Recordings []Recording `json:"recordings"`
	}

	r := &response{}

	_, err = c.do(req, r, http.StatusOK)
	if err != nil {
		return nil, err
	}
	return r.Recordings, nil
}

func (c *Client) ReplayLink(id string) Link {
	return Link{Relation: Self, Href: path.Join(replaysPath, id)}
}

type CreateReplayOptions struct {
	ID            string `json:"id"`
	Recording     string `json:"recording"`
	Task          string `json:"task"`
	RecordingTime bool   `json:"recording-time"`
	Clock         Clock  `json:"clock"`
}

func (o *CreateReplayOptions) Default() {
}

// Replay a recording for a task.
func (c *Client) CreateReplay(opt CreateReplayOptions) (Replay, error) {
	r := Replay{}

	opt.Default()

	u := *c.url
	u.Path = replaysPath

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	err := enc.Encode(opt)
	if err != nil {
		return r, err
	}

	req, err := http.NewRequest("POST", u.String(), &buf)
	if err != nil {
		return r, err
	}

	_, err = c.do(req, &r, http.StatusCreated)
	if err != nil {
		return r, err
	}
	return r, nil
}

type ReplayBatchOptions struct {
	ID            string    `json:"id,omitempty"`
	Task          string    `json:"task"`
	Start         time.Time `json:"start"`
	Stop          time.Time `json:"stop"`
	Cluster       string    `json:"cluster,omitempty"`
	RecordingTime bool      `json:"recording-time"`
	Clock         Clock     `json:"clock"`
}

// Replay a query against a task.
func (c *Client) ReplayBatch(opt ReplayBatchOptions) (Replay, error) {
	r := Replay{}

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	err := enc.Encode(opt)
	if err != nil {
		return r, err
	}

	u := *c.url
	u.Path = replayBatchPath

	req, err := http.NewRequest("POST", u.String(), &buf)
	if err != nil {
		return r, err
	}

	_, err = c.do(req, &r, http.StatusCreated)
	if err != nil {
		return r, err
	}
	return r, nil
}

type ReplayQueryOptions struct {
	ID            string `json:"id,omitempty"`
	Task          string `json:"task"`
	Query         string `json:"query"`
	Cluster       string `json:"cluster,omitempty"`
	RecordingTime bool   `json:"recording-time"`
	Clock         Clock  `json:"clock"`
}

// Replay a query against a task.
func (c *Client) ReplayQuery(opt ReplayQueryOptions) (Replay, error) {
	r := Replay{}

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	err := enc.Encode(opt)
	if err != nil {
		return r, err
	}

	u := *c.url
	u.Path = replayQueryPath

	req, err := http.NewRequest("POST", u.String(), &buf)
	if err != nil {
		return r, err
	}

	_, err = c.do(req, &r, http.StatusCreated)
	if err != nil {
		return r, err
	}
	return r, nil
}

// Return the replay information
func (c *Client) Replay(link Link) (Replay, error) {
	r := Replay{}
	if link.Href == "" {
		return r, fmt.Errorf("invalid link %v", link)
	}

	u := *c.url
	u.Path = link.Href

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return r, err
	}

	_, err = c.do(req, &r, http.StatusOK, http.StatusAccepted)
	if err != nil {
		return r, err
	}
	return r, nil
}

// Delete a replay. This will cancel a running replay.
func (c *Client) DeleteReplay(link Link) error {
	if link.Href == "" {
		return fmt.Errorf("invalid link %v", link)
	}
	u := *c.url
	u.Path = link.Href

	req, err := http.NewRequest("DELETE", u.String(), nil)
	if err != nil {
		return err
	}

	_, err = c.do(req, nil, http.StatusNoContent)
	if err != nil {
		return err
	}
	return nil
}

type ListReplaysOptions struct {
	Pattern string
	Fields  []string
	Offset  int
	Limit   int
}

func (o *ListReplaysOptions) Default() {
	if o.Limit == 0 {
		o.Limit = 100
	}
}

func (o *ListReplaysOptions) Values() *url.Values {
	v := &url.Values{}
	v.Set("pattern", o.Pattern)
	for _, field := range o.Fields {
		v.Add("fields", field)
	}
	v.Set("offset", strconv.FormatInt(int64(o.Offset), 10))
	v.Set("limit", strconv.FormatInt(int64(o.Limit), 10))
	return v
}

// Get information about replays.
// If rids is empty than all replays are returned.
func (c *Client) ListReplays(opt *ListReplaysOptions) ([]Replay, error) {
	if opt == nil {
		opt = new(ListReplaysOptions)
	}
	opt.Default()
	u := *c.url
	u.Path = replaysPath
	u.RawQuery = opt.Values().Encode()

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, err
	}
	// Decode valid response
	type response struct {
		Replays []Replay `json:"replays"`
	}

	r := &response{}

	_, err = c.do(req, r, http.StatusOK)
	if err != nil {
		return nil, err
	}
	return r.Replays, nil
}

type LogLevelOptions struct {
	Level string `json:"level"`
}

// Set the logging level.
// Level must be one of DEBUG, INFO, WARN, ERROR, or OFF
func (c *Client) LogLevel(level string) error {
	u := *c.url
	u.Path = logLevelPath

	opt := LogLevelOptions{Level: level}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	err := enc.Encode(opt)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", u.String(), &buf)
	if err != nil {
		return err
	}

	_, err = c.do(req, nil, http.StatusNoContent)
	return err
}

type DebugVars struct {
	ClusterID        string                 `json:"cluster_id"`
	ServerID         string                 `json:"server_id"`
	Host             string                 `json:"host"`
	Stats            map[string]Stat        `json:"kapacitor"`
	Cmdline          []string               `json:"cmdline"`
	NumEnabledTasks  int                    `json:"num_enabled_tasks"`
	NumSubscriptions int                    `json:"num_subscriptions"`
	NumTasks         int                    `json:"num_tasks"`
	Memstats         map[string]interface{} `json:"memstats"`
	Version          string                 `json:"version"`
}

type Stat struct {
	Name   string                 `json:"name"`
	Tags   map[string]string      `json:"tags"`
	Values map[string]interface{} `json:"values"`
}

// Get all Kapacitor vars
func (c *Client) DebugVars() (DebugVars, error) {
	u := *c.url
	u.Path = debugVarsPath

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return DebugVars{}, err
	}

	vars := DebugVars{}
	_, err = c.do(req, &vars, http.StatusOK)
	return vars, err
}
