package httpd

import (
	"compress/gzip"
	"encoding/json"
	"errors"
	"expvar"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/pprof"
	"strings"
	"time"

	"github.com/dgrijalva/jwt-go"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/uuid"
	"github.com/influxdata/kapacitor/auth"
	"github.com/influxdata/kapacitor/client/v1"
	"github.com/influxdata/kapacitor/services/logging"
	"github.com/influxdata/wlog"
)

// statistics gathered by the httpd package.
const (
	statRequest                   = "req"                 // Number of HTTP requests served
	statPingRequest               = "ping_req"            // Number of ping requests served
	statWriteRequest              = "write_req"           // Number of write requests serverd
	statWriteRequestBytesReceived = "write_req_bytes"     // Sum of all bytes in write requests
	statPointsWrittenOK           = "points_written_ok"   // Number of points written OK
	statPointsWrittenFail         = "points_written_fail" // Number of points that failed to be written
	statAuthFail                  = "auth_fail"           // Number of requests that failed to authenticate
)

const BasePath = "/kapacitor/v1"

// AuthenticationMethod defines the type of authentication used.
type AuthenticationMethod int

// Supported authentication methods.
const (
	UserAuthentication AuthenticationMethod = iota
	BearerAuthentication
)

type AuthorizationHandler func(http.ResponseWriter, *http.Request, auth.User)

type Route struct {
	Name        string
	Method      string
	Pattern     string
	HandlerFunc interface{}
	noJSON      bool
}

// Handler represents an HTTP handler for the Kapacitor API server.
type Handler struct {
	methodMux map[string]*ServeMux

	requireAuthentication bool
	sharedSecret          string

	allowGzip bool

	Version string

	AuthService auth.Interface

	PointsWriter interface {
		WritePoints(database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, points []models.Point) error
	}

	// Normal wlog logger
	logger *log.Logger
	// Detailed logging of write path
	// Uses normal logger
	writeTrace bool

	// Common log format logger.
	// This logger does not use log levels with wlog.
	// Its simply a binary on off from the config.
	clfLogger *log.Logger
	// Log every HTTP access.
	loggingEnabled bool

	statMap *expvar.Map
}

// NewHandler returns a new instance of handler with routes.
func NewHandler(
	requireAuthentication,
	loggingEnabled,
	writeTrace,
	allowGzip bool,
	statMap *expvar.Map,
	l *log.Logger,
	li logging.Interface,
	sharedSecret string,
) *Handler {
	h := &Handler{
		methodMux:             make(map[string]*ServeMux),
		requireAuthentication: requireAuthentication,
		sharedSecret:          sharedSecret,
		allowGzip:             allowGzip,
		logger:                l,
		writeTrace:            writeTrace,
		clfLogger:             li.NewRawLogger("[httpd] ", 0),
		loggingEnabled:        loggingEnabled,
		statMap:               statMap,
	}

	allowedMethods := []string{
		"GET",
		"POST",
		"PATCH",
		"DELETE",
		"HEAD",
		"OPTIONS",
	}

	for _, method := range allowedMethods {
		h.methodMux[method] = NewServeMux()
		route := Route{
			// Catch all 404
			Name:        "404",
			Method:      method,
			Pattern:     "/",
			HandlerFunc: h.serve404,
		}
		h.addRawRoute(route)
	}

	h.addRawRoutes([]Route{
		{
			// Ping
			Name:        "ping",
			Method:      "GET",
			Pattern:     BasePath + "/ping",
			HandlerFunc: h.servePing,
		},
		{
			// Ping
			Name:        "ping-head",
			Method:      "HEAD",
			Pattern:     BasePath + "/ping",
			HandlerFunc: h.servePing,
		},
		{
			// Data-ingest route.
			Name:        "write",
			Method:      "POST",
			Pattern:     BasePath + "/write",
			HandlerFunc: h.serveWrite,
		},
		{
			// Satisfy CORS checks.
			Name:        "write",
			Method:      "OPTIONS",
			Pattern:     BasePath + "/write",
			HandlerFunc: ServeOptions,
		},
		{
			// Data-ingest route for /write endpoint without base path
			Name:        "write-raw",
			Method:      "POST",
			Pattern:     "/write",
			HandlerFunc: h.serveWrite,
		},
		{
			// Satisfy CORS checks.
			Name:        "write-raw",
			Method:      "OPTIONS",
			Pattern:     "/write",
			HandlerFunc: ServeOptions,
		},
		{
			// Display current API routes
			Name:        "routes",
			Method:      "GET",
			Pattern:     BasePath + "/:routes",
			HandlerFunc: h.serveRoutes,
		},
		{
			// Change current log level
			Name:        "log-level",
			Method:      "POST",
			Pattern:     BasePath + "/loglevel",
			HandlerFunc: h.serveLogLevel,
		},
		{
			Name:        "pprof",
			Method:      "GET",
			Pattern:     BasePath + "/debug/pprof/",
			HandlerFunc: pprof.Index,
			noJSON:      true,
		},
		{
			Name:        "pprof/cmdline",
			Method:      "GET",
			Pattern:     BasePath + "/debug/pprof/cmdline",
			HandlerFunc: pprof.Cmdline,
			noJSON:      true,
		},
		{
			Name:        "pprof/profile",
			Method:      "GET",
			Pattern:     BasePath + "/debug/pprof/profile",
			HandlerFunc: pprof.Profile,
			noJSON:      true,
		},
		{
			Name:        "pprof/symbol",
			Method:      "GET",
			Pattern:     BasePath + "/debug/pprof/symbol",
			HandlerFunc: pprof.Symbol,
			noJSON:      true,
		},
		{
			Name:        "pprof/trace",
			Method:      "GET",
			Pattern:     BasePath + "/debug/pprof/trace",
			HandlerFunc: pprof.Trace,
			noJSON:      true,
		},
		{
			Name:        "debug/vars",
			Method:      "GET",
			Pattern:     BasePath + "/debug/vars",
			HandlerFunc: serveExpvar,
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
	if len(r.Pattern) > 0 && r.Pattern[0] != '/' {
		return fmt.Errorf("route patterns must begin with a '/' %s", r.Pattern)
	}
	r.Pattern = BasePath + r.Pattern
	return h.addRawRoute(r)
}

func (h *Handler) addRawRoutes(routes []Route) error {
	for _, r := range routes {
		err := h.addRawRoute(r)
		if err != nil {
			return err
		}
	}
	return nil
}

// Add a route without prepending the BasePath
func (h *Handler) addRawRoute(r Route) error {
	var handler http.Handler
	// If it's a handler func that requires special authorization, wrap it in authentication only.
	if hf, ok := r.HandlerFunc.(func(http.ResponseWriter, *http.Request, auth.User)); ok {
		handler = authenticate(authorizeForward(hf), h, h.requireAuthentication)
	}

	// This is a normal handler signature so perform standard authentication/authorization.
	if hf, ok := r.HandlerFunc.(func(http.ResponseWriter, *http.Request)); ok {
		handler = authenticate(authorize(hf), h, h.requireAuthentication)
	}
	if handler == nil {
		return errors.New("route does not have valid handler function")
	}

	// Set basic handlers for all requests
	if !r.noJSON {
		handler = jsonContent(handler)
	}
	if h.allowGzip {
		handler = gzipFilter(handler)
	}
	handler = versionHeader(handler, h)
	handler = cors(handler)
	handler = requestID(handler)

	if h.loggingEnabled {
		handler = logHandler(handler, r.Name, h.clfLogger)
	}
	handler = recovery(handler, r.Name, h.logger) // make sure recovery is always last

	mux, ok := h.methodMux[r.Method]
	if !ok {
		return fmt.Errorf("unsupported method %q", r.Method)
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
	r.Pattern = BasePath + r.Pattern
	h.delRawRoute(r)
}

// Delete a route from the handler. No-op if route does not exist.
func (h *Handler) delRawRoute(r Route) {
	mux, ok := h.methodMux[r.Method]
	if ok {
		mux.Deregister(r.Pattern)
	}
}

// ServeHTTP responds to HTTP request to the handler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.statMap.Add(statRequest, 1)
	method := r.Method
	if method == "" {
		method = "GET"
	}
	if mux, ok := h.methodMux[method]; ok {
		mux.ServeHTTP(w, r)
	} else {
		h.serve404(w, r)
	}
}

// serveLogLevel sets the log level of the server
func (h *Handler) serveLogLevel(w http.ResponseWriter, r *http.Request) {
	var opt client.LogLevelOptions
	dec := json.NewDecoder(r.Body)
	err := dec.Decode(&opt)
	if err != nil {
		HttpError(w, "invalid json: "+err.Error(), true, http.StatusBadRequest)
		return
	}
	err = wlog.SetLevelFromName(opt.Level)
	if err != nil {
		HttpError(w, err.Error(), true, http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusNoContent)
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

func (h *Handler) serveWrite(w http.ResponseWriter, r *http.Request, user auth.User) {
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
		if h.writeTrace {
			h.logger.Print("E! write handler unable to read bytes from request body")
		}
		h.writeError(w, influxql.Result{Err: err}, http.StatusBadRequest)
		return
	}
	h.statMap.Add(statWriteRequestBytesReceived, int64(len(b)))
	if h.writeTrace {
		h.logger.Printf("D! write body received by handler: %s", string(b))
	}

	h.serveWriteLine(w, r, b, user)
}

// serveWriteLine receives incoming series data in line protocol format and writes it to the database.
func (h *Handler) serveWriteLine(w http.ResponseWriter, r *http.Request, body []byte, user auth.User) {
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

	action := auth.Action{
		Resource:  auth.DatabaseResource(database),
		Privilege: auth.WritePrivilege,
	}
	if err := user.AuthorizeAction(action); err != nil {
		h.writeError(w, influxql.Result{Err: fmt.Errorf("%q user is not authorized to write to database %q", user.Name(), database)}, http.StatusUnauthorized)
		return
	}

	// Write points.
	if err := h.PointsWriter.WritePoints(
		database,
		r.FormValue("rp"),
		models.ConsistencyLevelAll,
		points,
	); influxdb.IsClientError(err) {
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
		type errResponse struct {
			Error string `json:"error"`
		}
		er := errResponse{Error: err.Error()}
		b, _ = json.Marshal(er)
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
		Error string `json:"error"`
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

// authenticate wraps a handler and ensures that if user credentials are passed in
// an attempt is made to authenticate that user. If authentication fails, an error is returned.
func authenticate(inner AuthorizationHandler, h *Handler, requireAuthentication bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Return early if we are not authenticating
		if !requireAuthentication {
			inner(w, r, auth.AdminUser)
			return
		}

		var user auth.User

		creds, err := parseCredentials(r)
		if err != nil {
			h.statMap.Add(statAuthFail, 1)
			HttpError(w, err.Error(), false, http.StatusUnauthorized)
			return
		}

		switch creds.Method {
		case UserAuthentication:
			if creds.Username == "" {
				h.statMap.Add(statAuthFail, 1)
				HttpError(w, "username required", false, http.StatusUnauthorized)
				return
			}

			user, err = h.AuthService.Authenticate(creds.Username, creds.Password)
			if err != nil {
				h.statMap.Add(statAuthFail, 1)
				HttpError(w, "authorization failed", false, http.StatusUnauthorized)
				return
			}
		case BearerAuthentication:
			keyLookupFn := func(token *jwt.Token) (interface{}, error) {
				// Check for expected signing method.
				if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
					return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
				}
				return []byte(h.sharedSecret), nil
			}

			// Parse and validate the token.
			token, err := jwt.Parse(creds.Token, keyLookupFn)
			if err != nil {
				HttpError(w, fmt.Sprintf("invalid token: %s", err.Error()), false, http.StatusUnauthorized)
				return
			} else if !token.Valid {
				HttpError(w, "invalid token", false, http.StatusUnauthorized)
				return
			}
			claims, ok := token.Claims.(jwt.MapClaims)
			if !ok {
				// This should not be possible, but just in case.
				HttpError(w, "invalid claims type", false, http.StatusUnauthorized)
				return
			}

			// The exp claim is validated internally as long as it exists and is non-zero.
			// Make sure a non-zero expiration was set on the token.
			if exp, ok := claims["exp"].(float64); !ok || exp <= 0.0 {
				HttpError(w, "token expiration required", false, http.StatusUnauthorized)
				return
			}

			// Get the username from the token.
			username, ok := claims["username"].(string)
			if !ok {
				HttpError(w, "username in token must be a string", false, http.StatusUnauthorized)
				return
			} else if username == "" {
				HttpError(w, "token must contain a username", false, http.StatusUnauthorized)
				return
			}

			if user, err = h.AuthService.User(username); err != nil {
				HttpError(w, err.Error(), false, http.StatusUnauthorized)
				return
			}
		default:
			HttpError(w, "unsupported authentication", false, http.StatusUnauthorized)
		}
		inner(w, r, user)
	})
}

// Map an HTTP method to an auth.Privilege.
func requiredPrivilegeForHTTPMethod(method string) (auth.Privilege, error) {
	switch m := strings.ToUpper(method); m {
	case "HEAD", "OPTIONS":
		return auth.NoPrivileges, nil
	case "GET":
		return auth.ReadPrivilege, nil
	case "POST", "PATCH":
		return auth.WritePrivilege, nil
	case "DELETE":
		return auth.DeletePrivilege, nil
	default:
		return auth.AllPrivileges, fmt.Errorf("unknown method %q", m)
	}
}

// Check if user is authorized to perform request.
func authorizeRequest(r *http.Request, user auth.User) error {
	// Now that we have a user authorize the request
	rp, err := requiredPrivilegeForHTTPMethod(r.Method)
	if err != nil {
		return err
	}
	action := auth.Action{
		Resource:  strings.TrimPrefix(r.URL.Path, BasePath),
		Privilege: rp,
	}
	return user.AuthorizeAction(action)
}

// Authorize the request and call normal inner handler.
func authorize(inner http.HandlerFunc) AuthorizationHandler {
	return func(w http.ResponseWriter, r *http.Request, user auth.User) {
		if err := authorizeRequest(r, user); err != nil {
			HttpError(w, err.Error(), false, http.StatusForbidden)
			return
		}
		inner(w, r)
	}
}

// Authorize the request and forward user to inner handler.
func authorizeForward(inner AuthorizationHandler) AuthorizationHandler {
	return func(w http.ResponseWriter, r *http.Request, user auth.User) {
		if err := authorizeRequest(r, user); err != nil {
			HttpError(w, err.Error(), false, http.StatusForbidden)
			return
		}
		inner(w, r, user)
	}
}

type credentials struct {
	Method   AuthenticationMethod
	Username string
	Password string
	Token    string
}

// parseCredentials parses a request and returns the authentication credentials.
// The credentials may be present as URL query params, or as a Basic
// Authentication header.
// As params: http://127.0.0.1/query?u=username&p=password
// As basic auth: http://username:password@127.0.0.1
// As Bearer token in Authorization header: Bearer <JWT_TOKEN_BLOB>
func parseCredentials(r *http.Request) (credentials, error) {
	q := r.URL.Query()

	// Check for the HTTP Authorization header.
	if s := r.Header.Get("Authorization"); s != "" {
		// Check for Bearer token.
		strs := strings.Split(s, " ")
		if len(strs) == 2 && strs[0] == "Bearer" {
			return credentials{
				Method: BearerAuthentication,
				Token:  strs[1],
			}, nil
		}

		// Check for basic auth.
		if u, p, ok := r.BasicAuth(); ok {
			return credentials{
				Method:   UserAuthentication,
				Username: u,
				Password: p,
			}, nil
		}
	}

	// Check for username and password in URL params.
	if u, p := q.Get("u"), q.Get("p"); u != "" && p != "" {
		return credentials{
			Method:   UserAuthentication,
			Username: u,
			Password: p,
		}, nil
	}

	return credentials{}, fmt.Errorf("unable to parse authentication credentials")
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
				`PATCH`,
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

func logHandler(inner http.Handler, name string, weblog *log.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		l := &responseLogger{w: w}
		inner.ServeHTTP(l, r)
		weblog.Println(buildLogLine(l, r, start))
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
