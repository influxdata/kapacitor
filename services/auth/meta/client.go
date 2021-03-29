// Package meta provides a client to the meta node to allow Kapacitor to perform auth.
// The real home of the meta client is in the InfluxDB Enterprise repo, however kapacitor's
// current dependency in influxdb is stuck on 1.1.
// This package allows kapacitor to use the newest meta client API but avoid having to
// upgrade kapacitor's dependency on influxdb which is now a couple years behind.
package meta

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"net/url"
	"time"

	"github.com/dgrijalva/jwt-go"
)

const controlClientUA = "InfluxDB Cluster Client"

const (
	// DefaultTimeout is the duration that the client will continue to
	// retry a failed operation.
	DefaultTimeout = 10 * time.Second

	// DefaultBackoffCap is the maximum duration a backoff value can
	// have.
	DefaultBackoffCap = DefaultTimeout / 3

	// MaxClientRedirects is the maximum number of redirects the Client will
	// attempt to follow for a single request, before failing.
	MaxClientRedirects = 5
)

// AuthType identifies a method of authentication.
type AuthType int

const (
	// NoAuth means no authentication will be used.
	NoAuth AuthType = iota
	// BasicAuth means basic user authentication will be used.
	BasicAuth
	// BearerAuth means JWT tokens will be used.
	BearerAuth
)

// Client is a client for the HTTP API exposed by a Plutonium Meta node.
type Client struct {
	client *http.Client

	metaBind string      // Address to bind to meta node.
	useTLS   bool        // Whether to use TLS protocol.
	tls      *tls.Config // TLS configuration to use if not skipping verification
	skipTLS  bool        // Whether to skip TLS verification

	timeout    time.Duration // Duration to timeout requests to meta node.
	backoffCap time.Duration // Maximum wait between request retries.

	authType AuthType
	username string // Name of user if authentication is enabled.
	password string // User's password if authentication is enabled.
	secret   string // Shared secret for JWT token signing.
}

// ClientOption is a functional option for a Client.
type ClientOption func(c *Client)

// UseAuth sets the authentication type and credentials that will be used.
var UseAuth = func(t AuthType, username, password, secret string) ClientOption {
	return func(c *Client) {
		c.authType = t
		c.username = username
		c.password = password
		c.secret = secret
	}
}

// WithTLS specifies if communication to the meta node uses the specified TLS config if any
// and if we skip verifying certificates, allowing for self-signed certificates.
var WithTLS = func(tlsConfig *tls.Config, useTLS bool, skip bool) ClientOption {
	return func(c *Client) {
		c.useTLS = useTLS
		if skip {
			c.skipTLS = true
			c.tls = &tls.Config{
				InsecureSkipVerify: true,
			}
		} else if useTLS {
			c.tls = tlsConfig
		}

		if t, ok := c.client.Transport.(*http.Transport); ok {
			t.TLSClientConfig = c.tls
		} else {
			c.client.Transport = &http.Transport{
				TLSClientConfig: c.tls,
			}
		}
	}
}

// WithTimeout specifies the duration the client will continue to retry
// failed operations before giving up.
var WithTimeout = func(d time.Duration) ClientOption {
	return func(c *Client) {
		c.timeout = d
		c.backoffCap = c.timeout / 3
		if t, ok := c.client.Transport.(*http.Transport); ok {
			t.ResponseHeaderTimeout = d
		} else {
			c.client.Transport = &http.Transport{
				ResponseHeaderTimeout: d,
			}
		}
	}
}

// NewClient returns a new Client, which will make requests to the Meta
// node listening on addr. New accepts zero or more functional options
// for configuring aspects of the returned Client.
//
// By default, a Client does not send communications over a TLS
// connection. TLS can be enforced using the WithTLS option. Output and
// errors are logged to stdout and stderr, respectively; use the
// appropriate options to set those outputs.
func NewClient(addr string, options ...ClientOption) *Client {
	c := &Client{
		metaBind:   addr,
		timeout:    DefaultTimeout,
		backoffCap: DefaultBackoffCap,
	}

	c.setRedirectClient()

	for _, option := range options {
		option(c)
	}

	return c
}

// setRedirectClient initializes the HTTP client
// to set Auth credentials on a redirect if needed.
func (c *Client) setRedirectClient() {
	c.client = &http.Client{}

	// Configure HTTP client to forward request headers on redirect.
	c.client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if len(via) >= 10 {
			return errors.New("too many redirects")
		} else if len(via) == 0 {
			return nil
		}
		for attr, val := range via[0].Header {
			if _, ok := req.Header[attr]; !ok {
				req.Header[attr] = val
			}
		}
		return c.setAuth(req)
	}
}

// setAuth adds the correct Authorization header to a request, if needed
func (c *Client) setAuth(r *http.Request) error {
	switch c.authType {
	case BasicAuth:
		r.SetBasicAuth(c.username, c.password)
	case BearerAuth:
		token := jwt.NewWithClaims(jwt.SigningMethodHS512, jwt.MapClaims{
			"username": c.username,
			"exp":      time.Now().Add(5 * time.Minute).Unix(),
		})
		tokenStr, err := token.SignedString([]byte(c.secret))
		if err != nil {
			return err
		}
		r.Header.Set("Authorization", fmt.Sprintf("Bearer %s", tokenStr))
	}

	return nil
}

// retryWithBackoff will continue to retry remote operation f until the
// the success predicate is satisfied. On failure, backoff will be
// exponentially increased up to a maximum capacity.
func (c *Client) retryWithBackoff(f func() error) error {
	var (
		base    = 10.0 // Will end up being 10 milliseconds.
		timeout = time.NewTimer(c.timeout)
		try     = time.NewTimer(0)
		retries float64
		err     error
	)
	defer timeout.Stop()
	defer try.Stop()

	for {
		select {
		case <-timeout.C:
			return ErrTimeout(err)
		case <-try.C:
			if err = f(); err == nil {
				return nil
			} else if _, ok := err.(FatalError); ok {
				// A fatal error has occurred; don't retry.
				return err
			}

			// On non-fatal failure exponentially increase backoff up to
			// cap.
			retries++
			d := time.Duration(math.Pow(2.0*base, retries)) * time.Millisecond
			if d > c.backoffCap {
				d = c.backoffCap
			}
			try.Reset(d)
			// TODO(cwolff): we should log an error here, like the original code did.
		}
	}
}

// do carries out an HTTP request. If host is empty it will use the host
// associated with the Client.
//
// do has the abilty to automatially redirect a request if the initial
// response status code is 307 (temporary redirect). It will follow the
// Location header and replay the same request including the body.
func (c *Client) do(method, host, path string, query url.Values, ct string, body io.Reader) (*http.Response, error) {
	var (
		redirects int
		buf       bytes.Buffer
	)

	if host == "" {
		host = c.metaBind
	}

	// We need to save the body in case we have to redirect.
	if body != nil {
		if _, err := io.Copy(&buf, body); err != nil {
			return nil, err
		}
	}

	u := &url.URL{}
	u.Path = path
	u.RawQuery = query.Encode()
	u.Host = host
	u.Scheme = "http"
	if c.useTLS {
		u.Scheme = "https"
	}
	u.RawQuery = query.Encode()
	u.RequestURI()

	location := u.String()

	for {
		// Set the body to a copy of the original body passed in.
		// This allows us to reuse it each time.
		if body != nil {
			body = ioutil.NopCloser(bytes.NewReader(buf.Bytes()))
		}

		req, err := http.NewRequest(method, location, body)
		if err != nil {
			return nil, err
		}
		req.Header.Add("Accept", "application/json")

		if err = c.setAuth(req); err != nil {
			return nil, err
		}

		req.Header.Set("User-Agent", controlClientUA)
		if ct != "" {
			req.Header.Set("Content-Type", ct)
		}

		resp, err := c.client.Do(req)
		if err != nil {
			return resp, err
		}

		if resp.StatusCode == http.StatusTemporaryRedirect {
			if redirects > MaxClientRedirects {
				return resp, ErrMaximumRedirectsReached
			}

			// We're done with the current response so make sure we
			// close the body.
			resp.Body.Close()

			redirects++
			location = resp.Header.Get("Location")
			continue
		}

		// 307 status code *not* returned, so we're done.
		return resp, err
	}
}

// get is a helper method for making a GET request to the node the
// client is bound to, using do. get sets application/json as the
// Content-Type.
func (c *Client) get(path string, query url.Values) (*http.Response, error) {
	return c.do("GET", c.metaBind, path, query, "application/json", nil)
}

// errorFromResponse will read r and attempt to unmarshal JSON from r
// into an Error. If r is empty, then an Error without any message will
// be returned.
// If r is not empty, but it's not possible to unmarshal r into a valid
// Error then an error will be returned.
func errorFromResponse(r io.Reader, code int) (Error, error) {
	var errMsg Error
	if err := json.NewDecoder(r).Decode(&errMsg); err != nil {
		if err != io.EOF {
			return Error{}, err
		}
		// In this case r was empty.
	}
	errMsg.Code = code
	return errMsg, nil
}

// Users returns a given user or a list of all users if name is empty.
// If the user is not found an empty list is returned without an error.
func (c *Client) Users(name string) ([]User, error) {
	var users struct {
		Users []User `json:"users"`
	}

	data := url.Values{"name": {name}}

	// cmd will be retried with back-off, until success or timeout.
	cmd := func() error {
		resp, err := c.get("/user", data)
		if err != nil {
			return err
		}
		if resp == nil {
			return fmt.Errorf("unable to get user: %q", name)
		}

		defer resp.Body.Close()

		switch resp.StatusCode {
		case http.StatusOK:
			return json.NewDecoder(resp.Body).Decode(&users)
		default:
			msg, err := errorFromResponse(resp.Body, resp.StatusCode)
			if err != nil {
				return err
			}
			return msg
		}
	}

	// Run cmd and wait for timeout error, if any.
	if err := c.retryWithBackoff(cmd); err != nil {
		return nil, err
	}

	return users.Users, nil
}

func (c *Client) CheckPass(name, pass string) error {
	data := url.Values{
		"name":       {name},
		"password":   {pass},
		"permission": {"NoPermissions"}, // because we just want to check the password here, not the other stuff.
		"resource":   {"_"},
	}
	cmd := func() error {
		resp, err := c.get("/authorized", data)
		if err != nil {
			return err
		}
		switch resp.StatusCode {
		case http.StatusOK:
			return nil
		default:
			msg, err := errorFromResponse(resp.Body, resp.StatusCode)
			if err != nil {
				return err
			}
			return msg
		}
	}
	return c.retryWithBackoff(cmd)
}
