package influxdb

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang-jwt/jwt"
	"github.com/influxdata/flux"
	"github.com/influxdata/kapacitor/keyvalue"
	"github.com/pkg/errors"
)

type Diagnostic interface {
	Error(msg string, err error, ctx ...keyvalue.T)
}

type TokenClientCreator struct {
	httpSharedSecret []byte
	tokenDuration    time.Duration
	diag             Diagnostic
}

func NewTokenClientCreator(httpSharedSecret []byte, tokenDuration time.Duration, d Diagnostic) *TokenClientCreator {
	return &TokenClientCreator{
		httpSharedSecret: httpSharedSecret,
		tokenDuration:    tokenDuration,
		diag:             d,
	}
}

func (cc *TokenClientCreator) Create(config Config) (ClientUpdater, error) {
	if config.Credentials.Method != BearerAuthentication {
		// Config doesn't need token auth, use normal client
		return NewHTTPClient(config)
	}
	if !config.Credentials.HttpSharedSecret {
		// This should not happen since we only set BearerAuthentication in services/influxdb.httpConfig
		// if http-shared-secret was false. Eventually we could add an additional config value to set a shared secret
		// that is not the one from KAPACITOR_HTTP_SHARED_SECRET
		return nil, errors.New("invalid config: bearer auth configured but http-shared-secret was false")
	}
	// Generate the first token
	token, err := generateToken(config.Credentials.Username, cc.httpSharedSecret, cc.tokenDuration)
	if err != nil {
		return nil, errors.Wrap(err, "generating first token")
	}
	// Update credentials to use token
	config.Credentials.Method = BearerAuthentication
	config.Credentials.Token = token

	cli, err := NewHTTPClient(config)
	if err != nil {
		return nil, err
	}
	tcli := &tokenClient{
		client:        cli,
		username:      config.Credentials.Username,
		sharedSecret:  cc.httpSharedSecret,
		tokenDuration: cc.tokenDuration,
		diag:          cc.diag,
		closing:       make(chan struct{}),
		closed:        true,
	}
	tcli.configValue.Store(config)

	if err := tcli.Open(); err != nil {
		return nil, errors.Wrap(err, "failed to open client")
	}
	return tcli, nil
}

type tokenClient struct {
	wg            sync.WaitGroup
	configValue   atomic.Value // influxdb.Config
	client        *HTTPClient
	username      string
	sharedSecret  []byte
	tokenDuration time.Duration

	mu      sync.Mutex
	closing chan struct{}
	closed  bool
	diag    Diagnostic
}

// Update updates the running configuration.
func (tc *tokenClient) Update(c Config) error {
	tc.configValue.Store(c)
	return tc.client.Update(c)
}

// Ping checks that status of cluster
func (tc *tokenClient) Ping(c context.Context) (time.Duration, string, error) {
	return tc.client.Ping(c)
}

// Write takes a BatchPoints object and writes all Points to InfluxDB.
func (tc *tokenClient) Write(bp BatchPoints) error {
	return tc.client.Write(bp)
}

// Query makes an InfluxDB Query on the database.
func (tc *tokenClient) Query(q Query) (*Response, error) {
	return tc.client.Query(q)
}

// WriteV2 writes to InfluxDB using the v2 write protocol
func (tc *tokenClient) WriteV2(w FluxWrite) error {
	return tc.client.WriteV2(w)
}

// QueryFlux makes a flux query to InfluxDB
func (tc *tokenClient) QueryFlux(q FluxQuery) (flux.ResultIterator, error) {
	return tc.client.QueryFlux(q)
}

// QueryFluxResponse makes a flux query to InfluxDB and translates it to a Response
func (tc *tokenClient) QueryFluxResponse(q FluxQuery) (*Response, error) {
	return tc.client.QueryFluxResponse(q)
}

func (tc *tokenClient) CreateBucketV2(bucket, org, orgID string) error {
	return tc.client.CreateBucketV2(bucket, org, orgID)
}

func (tc *tokenClient) Open() error {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	if !tc.closed {
		return nil
	}
	tc.closed = false
	// Start background routine to preemptively update the token before it expires.
	tc.wg.Add(1)
	go func() {
		defer tc.wg.Done()
		tc.manageToken()
	}()
	return nil
}

// Close the client.
func (tc *tokenClient) Close() error {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	if tc.closed {
		return nil
	}
	close(tc.closing)
	tc.closed = true
	tc.wg.Wait()
	return tc.client.Close()
}

// Preemptively update the token before it can expire.
func (tc *tokenClient) manageToken() {
	ticker := time.NewTicker(tc.tokenDuration / 2)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			token, err := generateToken(tc.username, tc.sharedSecret, tc.tokenDuration)
			if err != nil {
				tc.diag.Error("failed to generate new token", err)
				continue
			}
			c := tc.configValue.Load().(Config)
			c.Credentials.Token = token
			tc.Update(c)
		case <-tc.closing:
			return
		}
	}
}

// Generate a new signed token for the user. The token will expire after tokenDuration has elapsed.
func generateToken(username string, secret []byte, tokenDuration time.Duration) (string, error) {
	// Create a new token object, specifying signing method and the claims
	// you would like it to contain.
	token := jwt.NewWithClaims(jwt.SigningMethodHS512, jwt.MapClaims{
		"username": username,
		"exp":      time.Now().Add(tokenDuration).Unix(),
	})

	// Sign and get the complete encoded token as a string using the secret
	tokenString, err := token.SignedString(secret)
	return tokenString, errors.Wrap(err, "signing authentication token")
}
