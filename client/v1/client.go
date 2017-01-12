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

const (
	basePath          = "/kapacitor/v1"
	basePreviewPath   = "/kapacitor/v1preview"
	pingPath          = basePath + "/ping"
	logLevelPath      = basePath + "/loglevel"
	debugVarsPath     = basePath + "/debug/vars"
	tasksPath         = basePath + "/tasks"
	templatesPath     = basePath + "/templates"
	recordingsPath    = basePath + "/recordings"
	recordStreamPath  = basePath + "/recordings/stream"
	recordBatchPath   = basePath + "/recordings/batch"
	recordQueryPath   = basePath + "/recordings/query"
	replaysPath       = basePath + "/replays"
	replayBatchPath   = basePath + "/replays/batch"
	replayQueryPath   = basePath + "/replays/query"
	configPath        = basePath + "/config"
	serviceTestsPath  = basePath + "/service-tests"
	alertsPath        = basePreviewPath + "/alerts"
	handlersPath      = alertsPath + "/handlers"
	topicsPath        = alertsPath + "/topics"
	topicEventsPath   = "events"
	topicHandlersPath = "handlers"
)

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

	// Optional credentials for authenticating with the server.
	Credentials *Credentials
}

// AuthenticationMethod defines the type of authentication used.
type AuthenticationMethod int

// Supported authentication methods.
const (
	_ AuthenticationMethod = iota
	UserAuthentication
	BearerAuthentication
)

// Set of credentials depending on the authentication method
type Credentials struct {
	Method AuthenticationMethod

	// UserAuthentication fields

	Username string
	Password string

	// BearerAuthentication fields

	Token string
}

func (c Credentials) Validate() error {
	switch c.Method {
	case UserAuthentication:
		if c.Username == "" {
			return errors.New("missing username")
		}
		if c.Password == "" {
			return errors.New("missing password")
		}
	case BearerAuthentication:
		if c.Token == "" {
			return errors.New("missing token")
		}
	default:
		return errors.New("missing authentication method")
	}
	return nil
}

// Basic HTTP client
type Client struct {
	url         *url.URL
	userAgent   string
	httpClient  *http.Client
	credentials *Credentials
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

	if conf.Credentials != nil {
		if err := conf.Credentials.Validate(); err != nil {
			return nil, errors.Wrap(err, "invalid credentials")
		}
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
		credentials: conf.Credentials,
	}, nil
}

type Relation string

const (
	Self Relation = "self"
)

func (r Relation) String() string {
	return string(r)
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

type JSONOperation struct {
	Path      string      `json:"path"`
	Operation string      `json:"op"`
	Value     interface{} `json:"value"`
	From      string      `json:"from,omitempty"`
}

type JSONPatch []JSONOperation

func (c *Client) URL() string {
	return c.url.String()
}

func (c *Client) BaseURL() url.URL {
	return *c.url
}

// Perform the request.
// If result is not nil the response body is JSON decoded into result.
// Codes is a list of valid response codes.
func (c *Client) Do(req *http.Request, result interface{}, codes ...int) (*http.Response, error) {
	req.Header.Set("User-Agent", c.userAgent)
	if c.credentials != nil {
		switch c.credentials.Method {
		case UserAuthentication:
			req.SetBasicAuth(c.credentials.Username, c.credentials.Password)
		case BearerAuthentication:
			req.Header.Set("Authorization", "Bearer "+c.credentials.Token)
		default:
			return nil, errors.New("unknown authentication method set")
		}
	}
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

	resp, err := c.Do(req, nil, http.StatusNoContent)
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

func (c *Client) ConfigSectionLink(section string) Link {
	return Link{Relation: Self, Href: path.Join(configPath, section)}
}

func (c *Client) ConfigElementLink(section, element string) Link {
	href := path.Join(configPath, section, element)
	if element == "" {
		href += "/"
	}
	return Link{Relation: Self, Href: href}
}

func (c *Client) ServiceTestLink(service string) Link {
	return Link{Relation: Self, Href: path.Join(serviceTestsPath, service)}
}

func (c *Client) TopicEventsLink(topic string) Link {
	return Link{Relation: Self, Href: path.Join(topicsPath, topic, topicEventsPath)}
}
func (c *Client) TopicEventLink(topic, event string) Link {
	return Link{Relation: Self, Href: path.Join(topicsPath, topic, topicEventsPath, event)}
}

func (c *Client) TopicHandlersLink(topic string) Link {
	return Link{Relation: Self, Href: path.Join(topicsPath, topic, topicHandlersPath)}
}

func (c *Client) HandlerLink(id string) Link {
	return Link{Relation: Self, Href: path.Join(handlersPath, id)}
}
func (c *Client) TopicLink(id string) Link {
	return Link{Relation: Self, Href: path.Join(topicsPath, id)}
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
	req.Header.Set("Content-Type", "application/json")

	t := Task{}
	_, err = c.Do(req, &t, http.StatusOK)
	return t, err
}

type UpdateTaskOptions struct {
	ID         string     `json:"id,omitempty"`
	TemplateID string     `json:"template-id,omitempty"`
	Type       TaskType   `json:"type,omitempty"`
	DBRPs      []DBRP     `json:"dbrps,omitempty"`
	TICKscript string     `json:"script,omitempty"`
	Status     TaskStatus `json:"status,omitempty"`
	Vars       Vars       `json:"vars,omitempty"`
}

// Update an existing task.
// Only fields that are not their default value will be updated.
func (c *Client) UpdateTask(link Link, opt UpdateTaskOptions) (Task, error) {
	t := Task{}
	if link.Href == "" {
		return t, fmt.Errorf("invalid link %v", link)
	}

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	err := enc.Encode(opt)
	if err != nil {
		return t, err
	}

	u := *c.url
	u.Path = link.Href

	req, err := http.NewRequest("PATCH", u.String(), &buf)
	if err != nil {
		return t, err
	}
	req.Header.Set("Content-Type", "application/json")

	_, err = c.Do(req, &t, http.StatusOK)
	if err != nil {
		return t, err
	}
	return t, nil
}

type TaskOptions struct {
	DotView      string
	ScriptFormat string
	ReplayID     string
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
	v.Set("replay-id", o.ReplayID)
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

	_, err = c.Do(req, &task, http.StatusOK)
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

	_, err = c.Do(req, nil, http.StatusNoContent)
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

	_, err = c.Do(req, r, http.StatusOK)
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
	_, err = c.Do(req, r, http.StatusOK)
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
	req.Header.Set("Content-Type", "application/json")

	t := Template{}
	_, err = c.Do(req, &t, http.StatusOK)
	return t, err
}

type UpdateTemplateOptions struct {
	ID         string   `json:"id,omitempty"`
	Type       TaskType `json:"type,omitempty"`
	TICKscript string   `json:"script,omitempty"`
}

// Update an existing template.
// Only fields that are not their default value will be updated.
func (c *Client) UpdateTemplate(link Link, opt UpdateTemplateOptions) (Template, error) {
	t := Template{}
	if link.Href == "" {
		return t, fmt.Errorf("invalid link %v", link)
	}

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	err := enc.Encode(opt)
	if err != nil {
		return t, err
	}

	u := *c.url
	u.Path = link.Href

	req, err := http.NewRequest("PATCH", u.String(), &buf)
	if err != nil {
		return t, err
	}
	req.Header.Set("Content-Type", "application/json")

	_, err = c.Do(req, &t, http.StatusOK)
	if err != nil {
		return t, err
	}
	return t, nil
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

	_, err = c.Do(req, &template, http.StatusOK)
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

	_, err = c.Do(req, nil, http.StatusNoContent)
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

	_, err = c.Do(req, r, http.StatusOK)
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

	_, err = c.Do(req, &r, http.StatusOK, http.StatusAccepted)
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
	req.Header.Set("Content-Type", "application/json")

	_, err = c.Do(req, &r, http.StatusCreated)
	if err != nil {
		return r, err
	}
	return r, nil
}

type RecordBatchOptions struct {
	ID    string    `json:"id,omitempty"`
	Task  string    `json:"task"`
	Start time.Time `json:"start"`
	Stop  time.Time `json:"stop"`
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
	req.Header.Set("Content-Type", "application/json")

	_, err = c.Do(req, &r, http.StatusCreated)
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
	req.Header.Set("Content-Type", "application/json")

	_, err = c.Do(req, &r, http.StatusCreated)
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

	_, err = c.Do(req, nil, http.StatusNoContent)
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

	_, err = c.Do(req, r, http.StatusOK)
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
	req.Header.Set("Content-Type", "application/json")

	_, err = c.Do(req, &r, http.StatusCreated)
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
	req.Header.Set("Content-Type", "application/json")

	_, err = c.Do(req, &r, http.StatusCreated)
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
	req.Header.Set("Content-Type", "application/json")

	_, err = c.Do(req, &r, http.StatusCreated)
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

	_, err = c.Do(req, &r, http.StatusOK, http.StatusAccepted)
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

	_, err = c.Do(req, nil, http.StatusNoContent)
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

	_, err = c.Do(req, r, http.StatusOK)
	if err != nil {
		return nil, err
	}
	return r.Replays, nil
}

type ConfigUpdateAction struct {
	Set    map[string]interface{} `json:"set,omitempty"`
	Delete []string               `json:"delete,omitempty"`
	Add    map[string]interface{} `json:"add,omitempty"`
	Remove []string               `json:"remove,omitempty"`
}

// ConfigUpdate performs a given ConfigUpdateAction against a given section or element.
func (c *Client) ConfigUpdate(link Link, action ConfigUpdateAction) error {
	if link.Href == "" {
		return fmt.Errorf("invalid link %v", link)
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	err := enc.Encode(action)
	if err != nil {
		return err
	}

	u := *c.url
	u.Path = link.Href

	req, err := http.NewRequest("POST", u.String(), &buf)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	_, err = c.Do(req, nil, http.StatusNoContent)
	return err
}

type ConfigSections struct {
	Link     Link                     `json:"link"`
	Sections map[string]ConfigSection `json:"sections"`
}

type ConfigSection struct {
	Link     Link            `json:"link"`
	Elements []ConfigElement `json:"elements"`
}

type ConfigElement struct {
	Link     Link                   `json:"link"`
	Options  map[string]interface{} `json:"options"`
	Redacted []string               `json:"redacted"`
}

// ConfigSections returns all the running configuration sections that can be modified.
func (c *Client) ConfigSections() (ConfigSections, error) {
	u := *c.url
	u.Path = configPath

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return ConfigSections{}, err
	}

	sections := ConfigSections{}
	_, err = c.Do(req, &sections, http.StatusOK)
	if err != nil {
		return ConfigSections{}, err
	}
	return sections, nil
}

// ConfigSection returns the running configuration for a section.
func (c *Client) ConfigSection(link Link) (ConfigSection, error) {
	if link.Href == "" {
		return ConfigSection{}, fmt.Errorf("invalid link %v", link)
	}

	u := *c.url
	u.Path = link.Href

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return ConfigSection{}, err
	}

	section := ConfigSection{}
	_, err = c.Do(req, &section, http.StatusOK)
	if err != nil {
		return ConfigSection{}, err
	}
	return section, nil
}

// ConfigElement returns the running configuration for a given section and element.
func (c *Client) ConfigElement(link Link) (ConfigElement, error) {
	if link.Href == "" {
		return ConfigElement{}, fmt.Errorf("invalid link %v", link)
	}

	u := *c.url
	u.Path = link.Href

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return ConfigElement{}, err
	}

	element := ConfigElement{}
	_, err = c.Do(req, &element, http.StatusOK)
	if err != nil {
		return ConfigElement{}, err
	}
	return element, nil
}

type ServiceTests struct {
	Link     Link          `json:"link"`
	Services []ServiceTest `json:"services"`
}

type ServiceTest struct {
	Link    Link               `json:"link"`
	Name    string             `json:"name"`
	Options ServiceTestOptions `json:"options"`
}

type ServiceTestOptions map[string]interface{}

type ServiceTestResult struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

type ListServiceTestsOptions struct {
	Pattern string
}

func (o *ListServiceTestsOptions) Default() {
	if o.Pattern == "" {
		o.Pattern = "*"
	}
}

func (o *ListServiceTestsOptions) Values() *url.Values {
	v := &url.Values{}
	v.Set("pattern", o.Pattern)
	return v
}

// ServiceTests returns the list of services available for testing.
func (c *Client) ListServiceTests(opt *ListServiceTestsOptions) (ServiceTests, error) {
	if opt == nil {
		opt = new(ListServiceTestsOptions)
	}
	opt.Default()

	u := *c.url
	u.Path = serviceTestsPath
	u.RawQuery = opt.Values().Encode()

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return ServiceTests{}, err
	}

	services := ServiceTests{}
	_, err = c.Do(req, &services, http.StatusOK)
	if err != nil {
		return ServiceTests{}, err
	}
	return services, nil
}

// ServiceTest returns the options available for a service test.
func (c *Client) ServiceTest(link Link) (ServiceTest, error) {
	if link.Href == "" {
		return ServiceTest{}, fmt.Errorf("invalid link %v", link)
	}
	u := *c.url
	u.Path = link.Href

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return ServiceTest{}, err
	}

	service := ServiceTest{}
	_, err = c.Do(req, &service, http.StatusOK)
	if err != nil {
		return ServiceTest{}, err
	}
	return service, nil
}

// DoServiceTest performs a test for a service.
func (c *Client) DoServiceTest(link Link, sto ServiceTestOptions) (ServiceTestResult, error) {
	if link.Href == "" {
		return ServiceTestResult{}, fmt.Errorf("invalid link %v", link)
	}
	u := *c.url
	u.Path = link.Href

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	err := enc.Encode(sto)
	if err != nil {
		return ServiceTestResult{}, err
	}

	req, err := http.NewRequest("POST", u.String(), &buf)
	if err != nil {
		return ServiceTestResult{}, err
	}

	r := ServiceTestResult{}
	_, err = c.Do(req, &r, http.StatusOK)
	if err != nil {
		return ServiceTestResult{}, err
	}
	return r, nil
}

type ListTopicsOptions struct {
	Pattern  string
	MinLevel string
}

func (o *ListTopicsOptions) Default() {
	if o.MinLevel == "" {
		o.MinLevel = "OK"
	}
}

func (o *ListTopicsOptions) Values() *url.Values {
	v := &url.Values{}
	v.Set("pattern", o.Pattern)
	v.Set("min-level", o.MinLevel)
	return v
}

type Topics struct {
	Link   Link    `json:"link"`
	Topics []Topic `json:"topics"`
}

type Topic struct {
	Link         Link   `json:"link"`
	ID           string `json:"id"`
	Level        string `json:"level"`
	Collected    int64  `json:"collected"`
	EventsLink   Link   `json:"events-link"`
	HandlersLink Link   `json:"handlers-link"`
}

func (c *Client) ListTopics(opt *ListTopicsOptions) (Topics, error) {
	topics := Topics{}
	if opt == nil {
		opt = new(ListTopicsOptions)
	}
	opt.Default()

	u := *c.url
	u.Path = topicsPath
	u.RawQuery = opt.Values().Encode()

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return topics, err
	}

	_, err = c.Do(req, &topics, http.StatusOK)
	if err != nil {
		return topics, err
	}
	return topics, nil
}

func (c *Client) Topic(link Link) (Topic, error) {
	var t Topic
	if link.Href == "" {
		return t, fmt.Errorf("invalid link %v", link)
	}
	u := *c.url
	u.Path = link.Href

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return t, err
	}

	_, err = c.Do(req, &t, http.StatusOK)
	return t, err
}

func (c *Client) DeleteTopic(link Link) error {
	if link.Href == "" {
		return fmt.Errorf("invalid link %v", link)
	}
	u := *c.url
	u.Path = link.Href

	req, err := http.NewRequest("DELETE", u.String(), nil)
	if err != nil {
		return err
	}

	_, err = c.Do(req, nil, http.StatusNoContent)
	return err
}

type TopicEvents struct {
	Link   Link         `json:"link"`
	Topic  string       `json:"topic"`
	Events []TopicEvent `json:"events"`
}

type TopicEvent struct {
	Link  Link       `json:"link"`
	ID    string     `json:"id"`
	State EventState `json:"state"`
}

type EventState struct {
	Message  string    `json:"message"`
	Details  string    `json:"details"`
	Time     time.Time `json:"time"`
	Duration Duration  `json:"duration"`
	Level    string    `json:"level"`
}

// TopicEvent retrieves details for a single event of a topic
// Errors if no event exists.
func (c *Client) TopicEvent(link Link) (TopicEvent, error) {
	e := TopicEvent{}
	if link.Href == "" {
		return e, fmt.Errorf("invalid link %v", link)
	}

	u := *c.url
	u.Path = link.Href

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return e, err
	}

	_, err = c.Do(req, &e, http.StatusOK)
	return e, err
}

type ListTopicEventsOptions struct {
	MinLevel string
}

func (o *ListTopicEventsOptions) Default() {
	if o.MinLevel == "" {
		o.MinLevel = "OK"
	}
}

func (o *ListTopicEventsOptions) Values() *url.Values {
	v := &url.Values{}
	v.Set("min-level", o.MinLevel)
	return v
}

// ListTopicEvents returns the current state for events within a topic.
func (c *Client) ListTopicEvents(link Link, opt *ListTopicEventsOptions) (TopicEvents, error) {
	t := TopicEvents{}
	if link.Href == "" {
		return t, fmt.Errorf("invalid link %v", link)
	}

	if opt == nil {
		opt = new(ListTopicEventsOptions)
	}
	opt.Default()

	u := *c.url
	u.Path = link.Href
	u.RawQuery = opt.Values().Encode()

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return t, err
	}

	_, err = c.Do(req, &t, http.StatusOK)
	return t, err
}

type TopicHandlers struct {
	Link     Link      `json:"link"`
	Topic    string    `json:"topic"`
	Handlers []Handler `json:"handlers"`
}

// TopicHandlers returns the current state for events within a topic.
func (c *Client) ListTopicHandlers(link Link) (TopicHandlers, error) {
	t := TopicHandlers{}
	if link.Href == "" {
		return t, fmt.Errorf("invalid link %v", link)
	}

	u := *c.url
	u.Path = link.Href

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return t, err
	}

	_, err = c.Do(req, &t, http.StatusOK)
	return t, err
}

type Handlers struct {
	Link     Link      `json:"link"`
	Handlers []Handler `json:"handlers"`
}

type Handler struct {
	Link    Link            `json:"link"`
	ID      string          `json:"id"`
	Topics  []string        `json:"topics"`
	Actions []HandlerAction `json:"actions"`
}

type HandlerAction struct {
	Kind    string                 `json:"kind" yaml:"kind"`
	Options map[string]interface{} `json:"options" yaml:"options"`
}

// Handler retrieves an alert handler.
// Errors if no handler exists.
func (c *Client) Handler(link Link) (Handler, error) {
	h := Handler{}
	if link.Href == "" {
		return h, fmt.Errorf("invalid link %v", link)
	}

	u := *c.url
	u.Path = link.Href

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return h, err
	}

	_, err = c.Do(req, &h, http.StatusOK)
	return h, err
}

type HandlerOptions struct {
	ID      string          `json:"id" yaml:"id"`
	Topics  []string        `json:"topics" yaml:"topics"`
	Actions []HandlerAction `json:"actions" yaml:"actions"`
}

// CreateHandler creates a new alert handler.
// Errors if the handler already exists.
func (c *Client) CreateHandler(opt HandlerOptions) (Handler, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	err := enc.Encode(opt)
	if err != nil {
		return Handler{}, err
	}

	u := *c.url
	u.Path = handlersPath

	req, err := http.NewRequest("POST", u.String(), &buf)
	if err != nil {
		return Handler{}, err
	}
	req.Header.Set("Content-Type", "application/json")

	h := Handler{}
	_, err = c.Do(req, &h, http.StatusOK)
	return h, err
}

// PatchHandler applies a patch operation to an existing handler.
func (c *Client) PatchHandler(link Link, patch JSONPatch) (Handler, error) {
	h := Handler{}
	if link.Href == "" {
		return h, fmt.Errorf("invalid link %v", link)
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	err := enc.Encode(patch)
	if err != nil {
		return h, err
	}

	u := *c.url
	u.Path = link.Href

	req, err := http.NewRequest("PATCH", u.String(), &buf)
	if err != nil {
		return h, err
	}
	req.Header.Set("Content-Type", "application/json+patch")

	_, err = c.Do(req, &h, http.StatusOK)
	return h, err
}

// ReplaceHandler replaces an existing handler, with the new definition.
func (c *Client) ReplaceHandler(link Link, opt HandlerOptions) (Handler, error) {
	h := Handler{}
	if link.Href == "" {
		return h, fmt.Errorf("invalid link %v", link)
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	err := enc.Encode(opt)
	if err != nil {
		return h, err
	}

	u := *c.url
	u.Path = link.Href

	req, err := http.NewRequest("PUT", u.String(), &buf)
	if err != nil {
		return h, err
	}
	req.Header.Set("Content-Type", "application/json")

	_, err = c.Do(req, &h, http.StatusOK)
	return h, err
}

// DeleteHandler deletes a handler.
func (c *Client) DeleteHandler(link Link) error {
	if link.Href == "" {
		return fmt.Errorf("invalid link %v", link)
	}
	u := *c.url
	u.Path = link.Href

	req, err := http.NewRequest("DELETE", u.String(), nil)
	if err != nil {
		return err
	}

	_, err = c.Do(req, nil, http.StatusNoContent)
	return err
}

type ListHandlersOptions struct {
	Pattern string
}

func (o *ListHandlersOptions) Default() {}

func (o *ListHandlersOptions) Values() *url.Values {
	v := &url.Values{}
	v.Set("pattern", o.Pattern)
	return v
}

func (c *Client) ListHandlers(opt *ListHandlersOptions) (Handlers, error) {
	handlers := Handlers{}
	if opt == nil {
		opt = new(ListHandlersOptions)
	}
	opt.Default()

	u := *c.url
	u.Path = handlersPath
	u.RawQuery = opt.Values().Encode()

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return handlers, err
	}

	_, err = c.Do(req, &handlers, http.StatusOK)
	if err != nil {
		return handlers, err
	}
	return handlers, nil
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
	req.Header.Set("Content-Type", "application/json")

	_, err = c.Do(req, nil, http.StatusNoContent)
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
	_, err = c.Do(req, &vars, http.StatusOK)
	return vars, err
}

type Duration time.Duration

func (d Duration) MarshalText() ([]byte, error) {
	return []byte(time.Duration(d).String()), nil
}
func (d *Duration) UnmarshalText(data []byte) error {
	dur, err := time.ParseDuration(string(data))
	if err != nil {
		return err
	}
	*d = Duration(dur)
	return nil
}
