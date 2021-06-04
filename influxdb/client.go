package influxdb

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/csv"
	imodels "github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/v2/models"
	khttp "github.com/influxdata/kapacitor/http"
	"github.com/pkg/errors"
)

// Client is an interface for writing to and querying from an InfluxDB instance.
type Client interface {
	// Ping checks that status of cluster
	// The provided context can be used to cancel the request.
	Ping(ctx context.Context) (time.Duration, string, error)

	// Write takes a BatchPoints object and writes all Points to InfluxDB.
	Write(bp BatchPoints) error

	// WriteV2 takes a FluxWrite object and writes all Points to InfluxDB using the V2 interface.
	WriteV2(w FluxWrite) error

	// Query makes an InfluxDB Query on the database.
	// The response is checked for an error and the is returned
	// if it exists
	Query(q Query) (*Response, error)

	// QueryFlux is for querying Influxdb with the Flux language
	// The response is checked for an error and the is returned
	// if it exists
	QueryFlux(q FluxQuery) (flux.ResultIterator, error)

	// QueryFlux is for querying Influxdb with the Flux language
	// The response is checked for an error and the is returned
	// if it exists.  Unlike QueryFlux, this returns a *Response
	// object.
	QueryFluxResponse(q FluxQuery) (*Response, error)
}

type ClientUpdater interface {
	Client
	Update(new Config) error
	Close() error
}

// BatchPointsConfig is the config data needed to create an instance of the BatchPoints struct
type BatchPointsConfig struct {
	// Precision is the write precision of the points, defaults to "ns"
	Precision string

	// Database is the database to write points to
	Database string

	// RetentionPolicy is the retention policy of the points
	RetentionPolicy string

	// Write consistency is the number of servers required to confirm write
	WriteConsistency string
}

// Query defines a query to send to the server
type Query struct {
	Command   string
	Database  string
	Precision string
}

type FluxQuery struct {
	Org   string
	OrgID string
	Query string
	Now   time.Time
}

// HTTPConfig is the config data needed to create an HTTP Client
type Config struct {
	// The URL of the InfluxDB server.
	URLs []string

	// Optional credentials for authenticating with the server.
	Credentials Credentials

	// UserAgent is the http User Agent, defaults to "KapacitorInfluxDBClient"
	UserAgent string

	// Timeout for requests, defaults to no timeout.
	Timeout time.Duration

	// Transport is the HTTP transport to use for requests
	// If nil, a default transport will be used.
	Transport *http.Transport

	// Which compression should we use for writing to influxdb, defaults to "gzip".
	Compression string
}

// AuthenticationMethod defines the type of authentication used.
type AuthenticationMethod int

// Supported authentication methods.
const (
	NoAuthentication AuthenticationMethod = iota
	UserAuthentication
	BearerAuthentication
	TokenAuthentication // like bearer authentication but with the word Token
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

// HTTPClient is safe for concurrent use.
type HTTPClient struct {
	mu          sync.RWMutex
	config      Config
	urls        []url.URL
	client      *http.Client
	index       int32
	compression string
}

// NewHTTPClient returns a new Client from the provided config.
// Client is safe for concurrent use by multiple goroutines.
func NewHTTPClient(conf Config) (*HTTPClient, error) {
	if conf.UserAgent == "" {
		conf.UserAgent = "KapacitorInfluxDBClient"
	}
	urls, err := parseURLs(conf.URLs)
	if err != nil {
		return nil, errors.Wrap(err, "invalid URLs")
	}
	if conf.Transport == nil {
		conf.Transport = khttp.NewDefaultTransport()
	}
	c := &HTTPClient{
		config: conf,
		urls:   urls,
		client: &http.Client{
			Timeout:   conf.Timeout,
			Transport: conf.Transport,
		},
	}
	switch compression := strings.ToLower(strings.TrimSpace(conf.Compression)); compression {
	case "none":
		return c, nil
	case "gzip", "": // treat gzip as default
		c.compression = "gzip"
	default:
		return nil, fmt.Errorf("%s is not a supported compression type", compression)
	}
	return c, nil
}

func parseURLs(urlStrs []string) ([]url.URL, error) {
	urls := make([]url.URL, len(urlStrs))
	for i, urlStr := range urlStrs {
		u, err := url.Parse(urlStr)
		if err != nil {
			return nil, err
		} else if u.Scheme != "http" && u.Scheme != "https" {
			return nil, fmt.Errorf(
				"Unsupported protocol scheme: %s, your address must start with http:// or https://",
				u.Scheme,
			)
		}
		urls[i] = *u
	}
	return urls, nil
}

func (c *HTTPClient) loadConfig() Config {
	c.mu.RLock()
	config := c.config
	c.mu.RUnlock()
	return config
}

func (c *HTTPClient) loadURLs() []url.URL {
	c.mu.RLock()
	urls := c.urls
	c.mu.RUnlock()
	return urls
}

func (c *HTTPClient) loadHTTPClient() *http.Client {
	c.mu.RLock()
	client := c.client
	c.mu.RUnlock()
	return client
}

func (c *HTTPClient) Close() error {
	return nil
}

// UpdateURLs updates the running list of URLs.
func (c *HTTPClient) Update(new Config) error {
	if new.UserAgent == "" {
		new.UserAgent = "KapacitorInfluxDBClient"
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	old := c.config
	c.config = new
	// Replace urls
	urls, err := parseURLs(new.URLs)
	if err != nil {
		return err
	}
	c.urls = urls
	if old.Credentials != new.Credentials ||
		old.Timeout != new.Timeout ||
		old.Transport != new.Transport {
		//Replace the client
		tr := new.Transport
		if tr == nil {
			tr = old.Transport
		}
		c.client = &http.Client{
			Timeout:   new.Timeout,
			Transport: tr,
		}
	}
	return nil
}

func (c *HTTPClient) url() url.URL {
	urls := c.loadURLs()
	i := atomic.LoadInt32(&c.index)
	i = (i + 1) % int32(len(urls))
	atomic.StoreInt32(&c.index, i)
	return urls[i]
}

func (c *HTTPClient) do(req *http.Request, result interface{}, codes ...int) (*http.Response, error) {
	// Get current config
	config := c.loadConfig()
	// Set auth credentials
	cred := config.Credentials
	switch cred.Method {
	case NoAuthentication:
	case UserAuthentication:
		req.SetBasicAuth(cred.Username, cred.Password)
	case BearerAuthentication:
		req.Header.Set("Authorization", "Bearer "+cred.Token)
	case TokenAuthentication:
		req.Header.Set("Authorization", "Token "+cred.Token)
	default:
		return nil, errors.New("unknown authentication method set")
	}
	// Set user agent
	req.Header.Set("User-Agent", config.UserAgent)

	// Get client
	client := c.loadHTTPClient()

	// Do request
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	body := resp.Body
	defer func() {
		_, _ = io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}()

	valid := false
	for _, code := range codes {
		if resp.StatusCode == code {
			valid = true
			break
		}
	}
	if resp.Header.Get("Content-Encoding") == "gzip" {
		body, err = gzip.NewReader(body)
		if err != nil {
			return nil, err
		}
		defer body.Close()
	}

	if !valid {
		body, err := ioutil.ReadAll(body)
		if err != nil {
			return nil, err
		}
		d := json.NewDecoder(bytes.NewReader(body))
		rp := struct {
			Error string `json:"error"`
		}{}
		if err := d.Decode(&rp); err != nil {
			return nil, err
		}
		if rp.Error != "" {
			return nil, errors.New(rp.Error)
		}
		return nil, fmt.Errorf("invalid response: code %d", resp.StatusCode)
	}

	if result != nil {
		d := json.NewDecoder(body)
		d.UseNumber()
		err = d.Decode(result)
		if err != nil {
			return nil, errors.Wrap(err, "failed to decode JSON")
		}
	}
	return resp, nil
}

type readClose struct {
	io.Reader
	close func() error
}

func (r readClose) Close() error {
	return r.close()
}

var _ io.ReadCloser = readClose{}

func (c *HTTPClient) doFlux(req *http.Request, codes ...int) (io.ReadCloser, error) {
	// Get current config
	config := c.loadConfig()
	// Set auth credentials
	cred := config.Credentials
	switch cred.Method {
	case NoAuthentication:
	case UserAuthentication:
		req.SetBasicAuth(cred.Username, cred.Password)
	case BearerAuthentication:
		req.Header.Set("Authorization", "Bearer "+cred.Token)
	case TokenAuthentication:
		req.Header.Set("Authorization", "Token "+cred.Token)
	default:
		return nil, errors.New("unknown authentication method set")
	}
	// Set user agent
	req.Header.Set("User-Agent", config.UserAgent)

	// Get client
	client := c.loadHTTPClient()
	// Do request
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	closer := func() error {
		_, _ = io.Copy(ioutil.Discard, resp.Body)
		return resp.Body.Close()
	}
	defer func() {
		if closer != nil {
			closer()
		}
	}()

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
		d := json.NewDecoder(bytes.NewReader(body))
		rp := struct {
			Message string `json:"message"`
			Code    string `json:"code"`
			Error   string `json:"error"`
		}{}
		if err := d.Decode(&rp); err != nil {
			return nil, err
		}
		if rp.Error != "" {
			return nil, errors.New(rp.Error)
		}
		if rp.Message != "" {
			return nil, errors.New(rp.Message)
		}
		return nil, fmt.Errorf("invalid response: code %d: body: %s", resp.StatusCode, string(body))
	}
	body := resp.Body
	if resp.Header.Get("Content-Encoding") == "gzip" {
		body, err = gzip.NewReader(resp.Body)
		if err != nil {
			closer()
			return nil, err
		}
	}
	reader := readClose{
		Reader: body,
		close:  closer,
	}
	closer = nil // so that we don't close the reader when we return
	return reader, nil
}

// Ping will check to see if the server is up with an optional timeout on waiting for leader.
// Ping returns how long the request took, the version of the server it connected to, and an error if one occurred.
func (c *HTTPClient) Ping(ctx context.Context) (time.Duration, string, error) {
	now := time.Now()
	u := c.url()
	u.Path = "ping"
	if ctx != nil {
		if dl, ok := ctx.Deadline(); ok {
			v := url.Values{}
			v.Set("wait_for_leader", fmt.Sprintf("%.0fs", time.Now().Sub(dl).Seconds()))
			u.RawQuery = v.Encode()
		}
	}

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return 0, "", err
	}
	if ctx != nil {
		req = req.WithContext(ctx)
	}
	resp, err := c.do(req, nil, http.StatusNoContent)
	if err != nil {
		return 0, "", err
	}
	version := resp.Header.Get("X-Influxdb-Version")
	return time.Since(now), version, nil
}

func (c *HTTPClient) Write(bp BatchPoints) error {
	b := &bytes.Buffer{}
	precision := bp.Precision()

	u := c.url()
	u.Path = "write"
	v := url.Values{}
	v.Set("db", bp.Database())
	v.Set("rp", bp.RetentionPolicy())
	v.Set("precision", bp.Precision())
	v.Set("consistency", bp.WriteConsistency())
	u.RawQuery = v.Encode()

	if c.compression == "gzip" {
		bodyWriteCloser := gzip.NewWriter(b)
		for _, p := range bp.Points() {
			if _, err := bodyWriteCloser.Write(p.BytesWithLineFeed(precision)); err != nil {
				return err
			}
		}
		if err := bodyWriteCloser.Close(); err != nil {
			return err
		}

	} else {
		for _, p := range bp.Points() {
			if _, err := b.Write(p.BytesWithLineFeed(precision)); err != nil {
				return err
			}
		}
	}

	req, err := http.NewRequest("POST", u.String(), b)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	if c.compression == "gzip" {
		req.Header.Set("Content-Encoding", "gzip")
	}

	_, err = c.do(req, nil, http.StatusNoContent, http.StatusOK)
	return err
}

type FluxWrite struct {
	Bucket string
	Org    string
	OrgID  string
	Points models.Points
}

func (c *HTTPClient) WriteV2(w FluxWrite) error {
	// TODO: make this more efficient and enable gzip
	b := make([]byte, 0)

	u := c.url()
	u.Path = "api/v2/write"
	v := url.Values{}
	if w.Org != "" {
		v.Set("org", w.Org)
	}
	if w.OrgID != "" {
		v.Set("orgID", w.OrgID)
	}
	v.Set("bucket", w.Bucket)
	u.RawQuery = v.Encode()

	for _, p := range w.Points {
		b = p.AppendString(b)
	}
	reader := bytes.NewReader(b)
	req, err := http.NewRequest("POST", u.String(), reader)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	_, err = c.do(req, nil, http.StatusNoContent, http.StatusOK)
	return err
}

// Response represents a list of statement results.
type Response struct {
	Results []Result
	Err     string `json:"error,omitempty"`
}

// Error returns the first error from any statement.
// Returns nil if no errors occurred on any statements.
func (r *Response) Error() error {
	if r.Err != "" {
		return fmt.Errorf(r.Err)
	}
	for _, result := range r.Results {
		if result.Err != "" {
			return fmt.Errorf(result.Err)
		}
	}
	return nil
}

// Message represents a user message.
type Message struct {
	Level string
	Text  string
}

// Result represents a resultset returned from a single statement.
type Result struct {
	Series   []imodels.Row
	Messages []*Message
	Err      string `json:"error,omitempty"`
}

func (c *HTTPClient) buildFluxRequest(q FluxQuery) (*http.Request, error) {
	u := c.url()
	if c.url().Path == "" || c.url().Path == "/" {
		u.Path = "/api/v2/query"
	}

	v := url.Values{}
	if q.Org != "" {
		v.Set("org", q.Org)
	}
	if q.OrgID != "" {
		v.Set("orgID", q.OrgID)
	}
	u.RawQuery = v.Encode()

	type dialect struct {
		Annotations []string `json:"annotations,omitempty"`
		Delimiter   string   `json:"delimiter,omitempty"`
		Header      bool     `json:"header"`
	}
	nowString := ""
	if !q.Now.IsZero() {
		nowString = q.Now.Format(time.RFC3339Nano)
	}

	body, err := json.Marshal(&struct {
		Type    string  `json:"type"`
		Now     string  `json:"now,omitempty"`
		Query   string  `json:"query"`
		Dialect dialect `json:"dialect"`
	}{
		Type:  "flux",
		Now:   nowString,
		Query: q.Query,
		Dialect: dialect{
			Annotations: []string{"datatype", "default", "group"},
			Delimiter:   ",",
			Header:      true,
		},
	})
	req, err := http.NewRequest("POST", u.String(), bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept-Encoding", "gzip")
	return req, nil
}

func (c *HTTPClient) QueryFluxResponse(q FluxQuery) (*Response, error) {
	req, err := c.buildFluxRequest(q)
	if err != nil {
		return nil, err
	}
	reader, err := c.doFlux(req, http.StatusOK)
	if err != nil {
		return nil, err
	}

	resp, err := NewFluxQueryResponse(reader)
	if err != nil {
		return nil, err
	}
	return resp, nil

}

func (c *HTTPClient) QueryFlux(q FluxQuery) (flux.ResultIterator, error) {
	req, err := c.buildFluxRequest(q)
	if err != nil {
		return nil, err
	}
	reader, err := c.doFlux(req, http.StatusOK)
	if err != nil {
		return nil, err
	}
	decoder := csv.NewMultiResultDecoder(csv.ResultDecoderConfig{})
	itr, err := decoder.Decode(reader)
	if err != nil {
		return nil, err
	}
	return itr, nil
}

// Query sends a command to the server and returns the Response
func (c *HTTPClient) Query(q Query) (*Response, error) {
	u := c.url()
	u.Path = "query"
	v := url.Values{}
	v.Set("q", q.Command)
	v.Set("db", q.Database)
	if q.Precision != "" {
		v.Set("epoch", q.Precision)
	}
	u.RawQuery = v.Encode()

	req, err := http.NewRequest("POST", u.String(), nil)
	if err != nil {
		return nil, err
	}

	response := &Response{}
	_, err = c.do(req, response, http.StatusOK)
	if err != nil {
		return nil, err
	}
	if err := response.Error(); err != nil {
		return nil, err
	}
	return response, nil
}

// BatchPoints is an interface into a batched grouping of points to write into
// InfluxDB together. BatchPoints is NOT thread-safe, you must create a separate
// batch for each goroutine.
type BatchPoints interface {
	// AddPoint adds the given point to the Batch of points
	AddPoint(p Point)
	// AddPoints adds the given points to the Batch of points
	AddPoints(ps []Point)
	// Points lists the points in the Batch
	Points() []Point

	// Precision returns the currently set precision of this Batch
	Precision() string
	// SetPrecision sets the precision of this batch.
	SetPrecision(s string) error

	// Database returns the currently set database of this Batch
	Database() string
	// SetDatabase sets the database of this Batch
	SetDatabase(s string)

	// WriteConsistency returns the currently set write consistency of this Batch
	WriteConsistency() string
	// SetWriteConsistency sets the write consistency of this Batch
	SetWriteConsistency(s string)

	// RetentionPolicy returns the currently set retention policy of this Batch
	RetentionPolicy() string
	// SetRetentionPolicy sets the retention policy of this Batch
	SetRetentionPolicy(s string)
}

// NewBatchPoints returns a BatchPoints interface based on the given config.
func NewBatchPoints(conf BatchPointsConfig) (BatchPoints, error) {
	if conf.Precision == "" {
		conf.Precision = "ns"
	}
	if _, err := time.ParseDuration("1" + conf.Precision); err != nil {
		return nil, err
	}
	bp := &batchpoints{
		database:         conf.Database,
		precision:        conf.Precision,
		retentionPolicy:  conf.RetentionPolicy,
		writeConsistency: conf.WriteConsistency,
	}
	return bp, nil
}

type batchpoints struct {
	points           []Point
	database         string
	precision        string
	retentionPolicy  string
	writeConsistency string
}

func (bp *batchpoints) AddPoint(p Point) {
	bp.points = append(bp.points, p)
}

func (bp *batchpoints) AddPoints(ps []Point) {
	bp.points = append(bp.points, ps...)
}

func (bp *batchpoints) Points() []Point {
	return bp.points
}

func (bp *batchpoints) Precision() string {
	return bp.precision
}

func (bp *batchpoints) Database() string {
	return bp.database
}

func (bp *batchpoints) WriteConsistency() string {
	return bp.writeConsistency
}

func (bp *batchpoints) RetentionPolicy() string {
	return bp.retentionPolicy
}

func (bp *batchpoints) SetPrecision(p string) error {
	if _, err := time.ParseDuration("1" + p); err != nil {
		return err
	}
	bp.precision = p
	return nil
}

func (bp *batchpoints) SetDatabase(db string) {
	bp.database = db
}

func (bp *batchpoints) SetWriteConsistency(wc string) {
	bp.writeConsistency = wc
}

func (bp *batchpoints) SetRetentionPolicy(rp string) {
	bp.retentionPolicy = rp
}

type Point struct {
	Name   string
	Tags   map[string]string
	Fields map[string]interface{}
	Time   time.Time
}

// Returns byte array of a line protocol representation of the point
func (p Point) Bytes(precision string) []byte {
	key := imodels.MakeKey([]byte(p.Name), imodels.NewTags(p.Tags))
	fields := imodels.Fields(p.Fields).MarshalBinary()
	kl := len(key)
	fl := len(fields)
	var bytes []byte

	if p.Time.IsZero() {
		bytes = make([]byte, fl+kl+1)
		copy(bytes, key)
		bytes[kl] = ' '
		copy(bytes[kl+1:], fields)
	} else {
		timeStr := strconv.FormatInt(p.Time.UnixNano()/imodels.GetPrecisionMultiplier(precision), 10)
		tl := len(timeStr)
		bytes = make([]byte, fl+kl+tl+2)
		copy(bytes, key)
		bytes[kl] = ' '
		copy(bytes[kl+1:], fields)
		bytes[kl+fl+1] = ' '
		copy(bytes[kl+fl+2:], []byte(timeStr))
	}

	return bytes
}

func (p Point) BytesWithLineFeed(precision string) []byte {
	key := imodels.MakeKey([]byte(p.Name), imodels.NewTags(p.Tags))
	fields := imodels.Fields(p.Fields).MarshalBinary()
	kl := len(key)
	fl := len(fields)
	var bytes []byte

	if p.Time.IsZero() {
		bytes = make([]byte, fl+kl+2)
		copy(bytes, key)
		bytes[kl] = ' '
		copy(bytes[kl+1:], fields)
	} else {
		timeStr := strconv.FormatInt(p.Time.UnixNano()/imodels.GetPrecisionMultiplier(precision), 10)
		tl := len(timeStr)
		bytes = make([]byte, fl+kl+tl+3)
		copy(bytes, key)
		bytes[kl] = ' '
		copy(bytes[kl+1:], fields)
		bytes[kl+fl+1] = ' '
		copy(bytes[kl+fl+2:], []byte(timeStr))
	}
	bytes[len(bytes)-1] = '\n'
	return bytes

}

// Simple type to create github.com/influxdata/kapacitor/influxdb clients.
type ClientCreator struct{}

func (ClientCreator) Create(config Config) (ClientUpdater, error) {
	return NewHTTPClient(config)
}
