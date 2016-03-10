package httpd

import (
	"compress/gzip"
	"encoding/json"
	"expvar"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/pprof"
	"strings"
	"time"

	"github.com/influxdata/kapacitor/wlog"
	"github.com/influxdb/influxdb"
	"github.com/influxdb/influxdb/cluster"
	"github.com/influxdb/influxdb/influxql"
	"github.com/influxdb/influxdb/models"
	"github.com/influxdb/influxdb/services/meta"
	"github.com/influxdb/influxdb/uuid"
)

// statistics gathered by the httpd package.
const (
	statRequest                   = "req"                 // Number of HTTP requests served
	statPingRequest               = "ping_req"            // Number of ping requests served
	statWriteRequest              = "write_req"           // Number of write requests serverd
	statWriteRequestBytesReceived = "write_req_bytes"     // Sum of all bytes in write requests
	statPointsWrittenOK           = "points_written_ok"   // Number of points written OK
	statPointsWrittenFail         = "points_written_fail" // Number of points that failed to be written
)

type Route struct {
	Name        string
	Method      string
	Pattern     string
	HandlerFunc interface{}
}

// Handler represents an HTTP handler for the Kapacitor API server.
type Handler struct {
	methodMux             map[string]*ServeMux
	requireAuthentication bool
	Version               string

	MetaClient interface {
		Database(name string) (*meta.DatabaseInfo, error)
		Authenticate(username, password string) (ui *meta.UserInfo, err error)
		Users() ([]meta.UserInfo, error)
	}

	PointsWriter interface {
		WritePoints(p *cluster.WritePointsRequest) error
	}

	Logger         *log.Logger
	loggingEnabled bool // Log every HTTP access.
	WriteTrace     bool // Detailed logging of write path
	statMap        *expvar.Map
}

// NewHandler returns a new instance of handler with routes.
func NewHandler(requireAuthentication, loggingEnabled, writeTrace bool, statMap *expvar.Map, l *log.Logger) *Handler {
	h := &Handler{
		methodMux:             make(map[string]*ServeMux),
		requireAuthentication: requireAuthentication,
		Logger:                l,
		loggingEnabled:        loggingEnabled,
		WriteTrace:            writeTrace,
		statMap:               statMap,
	}

	h.AddRoutes([]Route{
		{
			// Ping
			Name:        "ping",
			Method:      "GET",
			Pattern:     "/ping",
			HandlerFunc: h.servePing,
		},
		{
			// Ping
			Name:        "ping-head",
			Method:      "HEAD",
			Pattern:     "/ping",
			HandlerFunc: h.servePing,
		},
		{
			// Satisfy CORS checks.
			Name:        "write",
			Method:      "OPTIONS",
			Pattern:     "/write",
			HandlerFunc: ServeOptions,
		},
		{
			// Data-ingest route.
			Name:        "write",
			Method:      "POST",
			Pattern:     "/write",
			HandlerFunc: h.serveWrite,
		},
		{
			// Display current API routes
			Name:        "routes",
			Method:      "GET",
			Pattern:     "/:routes",
			HandlerFunc: h.serveRoutes,
		},
		{
			// Display current log level
			Name:        "log-level",
			Method:      "POST",
			Pattern:     "/loglevel",
			HandlerFunc: h.serveLogLevel,
		},
		{
			// Catch all 404
			Name:        "404",
			Method:      "GET",
			Pattern:     "/",
			HandlerFunc: h.serve404,
		},
		{
			// Catch all 404
			Name:        "404",
			Method:      "POST",
			Pattern:     "/",
			HandlerFunc: h.serve404,
		},
		{
			// Catch all 404
			Name:        "404",
			Method:      "DELETE",
			Pattern:     "/",
			HandlerFunc: h.serve404,
		},
		{
			// Catch all 404
			Name:        "404",
			Method:      "HEAD",
			Pattern:     "/",
			HandlerFunc: h.serve404,
		},
	})

	return h
}

func (h *Handler) AddRoutes(routes []Route) error {
	for _, r := range routes {
		err := h.AddRoute(r)
		if err != nil {
			return err
		}
	}
	return nil
}

func (h *Handler) AddRoute(r Route) error {
	var handler http.Handler
	// If it's a handler func that requires authorization, wrap it in authorization
	if hf, ok := r.HandlerFunc.(func(http.ResponseWriter, *http.Request, *meta.UserInfo)); ok {
		handler = authenticate(hf, h, h.requireAuthentication)
	}
	// This is a normal handler signature and does not require authorization
	if hf, ok := r.HandlerFunc.(func(http.ResponseWriter, *http.Request)); ok {
		handler = http.HandlerFunc(hf)
	}

	// Set basic handlers for all requests
	handler = jsonContent(handler)
	handler = gzipFilter(handler)
	handler = versionHeader(handler, h)
	handler = cors(handler)
	handler = requestID(handler)

	if h.loggingEnabled {
		handler = logging(handler, r.Name, h.Logger)
	}
	handler = recovery(handler, r.Name, h.Logger) // make sure recovery is always last

	mux, ok := h.methodMux[r.Method]
	if !ok {
		mux = NewServeMux()
		h.methodMux[r.Method] = mux
	}
	return mux.Handle(r.Pattern, handler)
}

func (h *Handler) DelRoutes(routes []Route) {
	for _, r := range routes {
		h.DelRoute(r)
	}
}

// Delete a route from the handler. No-op if route does not exist.
func (h *Handler) DelRoute(r Route) {
	mux, ok := h.methodMux[r.Method]
	if ok {
		mux.Deregister(r.Pattern)
	}
}

// ServeHTTP responds to HTTP request to the handler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.statMap.Add(statRequest, 1)

	// FIXME(benbjohnson): Add pprof enabled flag.
	if strings.HasPrefix(r.URL.Path, "/debug/pprof") {
		switch r.URL.Path {
		case "/debug/pprof/cmdline":
			pprof.Cmdline(w, r)
		case "/debug/pprof/profile":
			pprof.Profile(w, r)
		case "/debug/pprof/symbol":
			pprof.Symbol(w, r)
		default:
			pprof.Index(w, r)
		}
	} else if strings.HasPrefix(r.URL.Path, "/debug/vars") {
		serveExpvar(w, r)
	} else {
		method := r.Method
		if method == "" {
			method = "GET"
		}
		if mux, ok := h.methodMux[method]; ok {
			mux.ServeHTTP(w, r)
		}
	}
}

// serveLogLevel sets the log level of the server
func (h *Handler) serveLogLevel(w http.ResponseWriter, r *http.Request) {
	l := r.URL.Query().Get("level")
	err := wlog.SetLevelFromName(l)
	if err != nil {
		HttpError(w, err.Error(), true, http.StatusBadRequest)
	}
}

// serveRoutes returns a list of all routs and their methods
func (h *Handler) serveRoutes(w http.ResponseWriter, r *http.Request) {
	routes := make(map[string][]string)

	for method, mux := range h.methodMux {
		patterns := mux.Patterns()
		for _, p := range patterns {
			routes[p] = append(routes[p], method)
		}
	}

	w.Write(MarshalJSON(routes, true))
}

// serve404 returns an a formated 404 error
func (h *Handler) serve404(w http.ResponseWriter, r *http.Request) {
	HttpError(w, "Not Found", true, http.StatusNotFound)
}

func (h *Handler) writeError(w http.ResponseWriter, result influxql.Result, statusCode int) {
	w.WriteHeader(statusCode)
	w.Write([]byte(result.Err.Error()))
	w.Write([]byte("\n"))
}

// ServeOptions returns an empty response to comply with OPTIONS pre-flight requests
func ServeOptions(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNoContent)
}

// servePing returns a simple response to let the client know the server is running.
func (h *Handler) servePing(w http.ResponseWriter, r *http.Request) {
	h.statMap.Add(statPingRequest, 1)
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) serveWrite(w http.ResponseWriter, r *http.Request, user *meta.UserInfo) {
	h.statMap.Add(statWriteRequest, 1)

	// Handle gzip decoding of the body
	body := r.Body
	if r.Header.Get("Content-encoding") == "gzip" {
		b, err := gzip.NewReader(r.Body)
		if err != nil {
			h.writeError(w, influxql.Result{Err: err}, http.StatusBadRequest)
			return
		}
		body = b
	}
	defer body.Close()

	b, err := ioutil.ReadAll(body)
	if err != nil {
		if h.WriteTrace {
			h.Logger.Print("E! write handler unable to read bytes from request body")
		}
		h.writeError(w, influxql.Result{Err: err}, http.StatusBadRequest)
		return
	}
	h.statMap.Add(statWriteRequestBytesReceived, int64(len(b)))
	if h.WriteTrace {
		h.Logger.Printf("D! write body received by handler: %s", string(b))
	}

	h.serveWriteLine(w, r, b, user)
}

// serveWriteLine receives incoming series data in line protocol format and writes it to the database.
func (h *Handler) serveWriteLine(w http.ResponseWriter, r *http.Request, body []byte, user *meta.UserInfo) {
	precision := r.FormValue("precision")
	if precision == "" {
		precision = "n"
	}

	points, err := models.ParsePointsWithPrecision(body, time.Now().UTC(), precision)
	if err != nil {
		if err.Error() == "EOF" {
			w.WriteHeader(http.StatusOK)
			return
		}
		h.writeError(w, influxql.Result{Err: err}, http.StatusBadRequest)
		return
	}

	database := r.FormValue("db")
	if database == "" {
		h.writeError(w, influxql.Result{Err: fmt.Errorf("database is required")}, http.StatusBadRequest)
		return
	}

	if di, err := h.MetaClient.Database(database); err != nil {
		h.writeError(w, influxql.Result{Err: fmt.Errorf("metastore database error: %s", err)}, http.StatusInternalServerError)
		return
	} else if di == nil {
		h.writeError(w, influxql.Result{Err: fmt.Errorf("database not found: %q", database)}, http.StatusNotFound)
		return
	}

	if h.requireAuthentication && user == nil {
		h.writeError(w, influxql.Result{Err: fmt.Errorf("user is required to write to database %q", database)}, http.StatusUnauthorized)
		return
	}

	if h.requireAuthentication && !user.Authorize(influxql.WritePrivilege, database) {
		h.writeError(w, influxql.Result{Err: fmt.Errorf("%q user is not authorized to write to database %q", user.Name, database)}, http.StatusUnauthorized)
		return
	}

	// Determine required consistency level.
	consistency := cluster.ConsistencyLevelOne
	switch r.Form.Get("consistency") {
	case "all":
		consistency = cluster.ConsistencyLevelAll
	case "any":
		consistency = cluster.ConsistencyLevelAny
	case "one":
		consistency = cluster.ConsistencyLevelOne
	case "quorum":
		consistency = cluster.ConsistencyLevelQuorum
	}

	// Write points.
	if err := h.PointsWriter.WritePoints(&cluster.WritePointsRequest{
		Database:         database,
		RetentionPolicy:  r.FormValue("rp"),
		ConsistencyLevel: consistency,
		Points:           points,
	}); influxdb.IsClientError(err) {
		h.statMap.Add(statPointsWrittenFail, int64(len(points)))
		h.writeError(w, influxql.Result{Err: err}, http.StatusBadRequest)
		return
	} else if err != nil {
		h.statMap.Add(statPointsWrittenFail, int64(len(points)))
		h.writeError(w, influxql.Result{Err: err}, http.StatusInternalServerError)
		return
	}

	h.statMap.Add(statPointsWrittenOK, int64(len(points)))
	w.WriteHeader(http.StatusNoContent)
}

// MarshalJSON will marshal v to JSON. Pretty prints if pretty is true.
func MarshalJSON(v interface{}, pretty bool) []byte {
	var b []byte
	var err error
	if pretty {
		b, err = json.MarshalIndent(v, "", "    ")
	} else {
		b, err = json.Marshal(v)
	}

	if err != nil {
		return []byte(err.Error())
	}
	return b
}

// serveExpvar serves registered expvar information over HTTP.
func serveExpvar(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "{\n")
	first := true
	expvar.Do(func(kv expvar.KeyValue) {
		if !first {
			fmt.Fprintf(w, ",\n")
		}
		first = false
		fmt.Fprintf(w, "%q: %s", kv.Key, kv.Value)
	})
	fmt.Fprintf(w, "\n}\n")
}

// HttpError writes an error to the client in a standard format.
func HttpError(w http.ResponseWriter, err string, pretty bool, code int) {
	w.WriteHeader(code)

	type errResponse struct {
		Error string
	}

	response := errResponse{Error: err}
	var b []byte
	if pretty {
		b, _ = json.MarshalIndent(response, "", "    ")
	} else {
		b, _ = json.Marshal(response)
	}
	w.Write(b)
}

func resultError(w http.ResponseWriter, result influxql.Result, code int) {
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(&result)
}

// Filters and filter helpers

// parseCredentials returns the username and password encoded in
// a request. The credentials may be present as URL query params, or as
// a Basic Authentication header.
// as params: http://127.0.0.1/query?u=username&p=password
// as basic auth: http://username:password@127.0.0.1
func parseCredentials(r *http.Request) (string, string, error) {
	q := r.URL.Query()

	if u, p := q.Get("u"), q.Get("p"); u != "" && p != "" {
		return u, p, nil
	}
	if u, p, ok := r.BasicAuth(); ok {
		return u, p, nil
	}
	return "", "", fmt.Errorf("unable to parse Basic Auth credentials")
}

// authenticate wraps a handler and ensures that if user credentials are passed in
// an attempt is made to authenticate that user. If authentication fails, an error is returned.
// NOT IMPLEMENTED YET
func authenticate(inner func(http.ResponseWriter, *http.Request, *meta.UserInfo), h *Handler, requireAuthentication bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Return early if we are not authenticating
		if !requireAuthentication {
			inner(w, r, nil)
			return
		}
		panic("Authentication not implemented")
	})
}

type gzipResponseWriter struct {
	io.Writer
	http.ResponseWriter
}

func (w gzipResponseWriter) Write(b []byte) (int, error) {
	return w.Writer.Write(b)
}

func (w gzipResponseWriter) Flush() {
	w.Writer.(*gzip.Writer).Flush()
}

// determines if the client can accept compressed responses, and encodes accordingly
func gzipFilter(inner http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			inner.ServeHTTP(w, r)
			return
		}
		w.Header().Set("Content-Encoding", "gzip")
		gz := gzip.NewWriter(w)
		defer gz.Close()
		gzw := gzipResponseWriter{Writer: gz, ResponseWriter: w}
		inner.ServeHTTP(gzw, r)
	})
}

func jsonContent(inner http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		inner.ServeHTTP(w, r)
	})
}

// versionHeader takes a HTTP handler and returns a HTTP handler
// and adds the X-KAPACITOR-VERSION header to outgoing responses.
func versionHeader(inner http.Handler, h *Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("X-KAPACITOR-Version", h.Version)
		inner.ServeHTTP(w, r)
	})
}

// cors responds to incoming requests and adds the appropriate cors headers
// TODO: corylanou: add the ability to configure this in our config
func cors(inner http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if origin := r.Header.Get("Origin"); origin != "" {
			w.Header().Set(`Access-Control-Allow-Origin`, origin)
			w.Header().Set(`Access-Control-Allow-Methods`, strings.Join([]string{
				`DELETE`,
				`GET`,
				`OPTIONS`,
				`POST`,
				`PUT`,
			}, ", "))

			w.Header().Set(`Access-Control-Allow-Headers`, strings.Join([]string{
				`Accept`,
				`Accept-Encoding`,
				`Authorization`,
				`Content-Length`,
				`Content-Type`,
				`X-CSRF-Token`,
				`X-HTTP-Method-Override`,
			}, ", "))
		}

		if r.Method == "OPTIONS" {
			return
		}

		inner.ServeHTTP(w, r)
	})
}

func requestID(inner http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		uid := uuid.TimeUUID()
		r.Header.Set("Request-Id", uid.String())
		w.Header().Set("Request-Id", r.Header.Get("Request-Id"))

		inner.ServeHTTP(w, r)
	})
}

func logging(inner http.Handler, name string, weblog *log.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		l := &responseLogger{w: w}
		inner.ServeHTTP(l, r)
		logLine := "I! " + buildLogLine(l, r, start)
		weblog.Println(logLine)
	})
}

func recovery(inner http.Handler, name string, weblog *log.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		l := &responseLogger{w: w}
		inner.ServeHTTP(l, r)
		if err := recover(); err != nil {
			logLine := buildLogLine(l, r, start)
			logLine = fmt.Sprintf("E! %s [err:%s]", logLine, err)
			weblog.Println(logLine)
		}
	})
}
