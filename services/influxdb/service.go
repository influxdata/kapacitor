package influxdb

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/kapacitor/influxdb"
	"github.com/influxdata/kapacitor/server/vars"
	"github.com/influxdata/kapacitor/services/httpd"
	"github.com/influxdata/kapacitor/services/udp"
	"github.com/influxdata/kapacitor/uuid"
	"github.com/pkg/errors"
)

const (
	// Legacy name given to all subscriptions.
	legacySubName = "kapacitor"
	subNamePrefix = "kapacitor-"

	// Size in bytes of a token for subscription authentication
	tokenSize = 64

	// API endpoint paths
	subscriptionsPath         = "/subscriptions"
	subscriptionsPathAnchored = "/subscriptions/"
)

// IDer returns the current IDs of the cluster and server.
type IDer interface {
	// ClusterID returns the current cluster ID, this ID may change.
	ClusterID() uuid.UUID
	// ServerID returns the server ID which does not change.
	ServerID() uuid.UUID
}

// Handles requests to write or read from an InfluxDB cluster
type Service struct {
	mu     sync.RWMutex
	opened bool

	defaultInfluxDB string
	clusters        map[string]*influxdbCluster
	routes          []httpd.Route

	hostname  string
	ider      IDer
	httpPort  int
	useTokens bool

	PointsWriter interface {
		WritePoints(database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, points []models.Point) error
	}
	LogService interface {
		NewLogger(string, int) *log.Logger
	}
	HTTPDService interface {
		AddRoutes([]httpd.Route) error
		DelRoutes([]httpd.Route)
	}
	ClientCreator interface {
		Create(influxdb.Config) (influxdb.ClientUpdater, error)
	}
	AuthService interface {
		GrantSubscriptionAccess(token, db, rp string) error
		ListSubscriptionTokens() ([]string, error)
		RevokeSubscriptionAccess(token string) error
	}
	RandReader io.Reader
	logger     *log.Logger
}

func NewService(configs []Config, httpPort int, hostname string, ider IDer, useTokens bool, l *log.Logger) (*Service, error) {
	s := &Service{
		clusters:   make(map[string]*influxdbCluster),
		hostname:   hostname,
		ider:       ider,
		httpPort:   httpPort,
		useTokens:  useTokens,
		logger:     l,
		RandReader: rand.Reader,
	}
	if err := s.updateConfigs(configs); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Service) Open() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.opened {
		return nil
	}
	s.opened = true
	for _, cluster := range s.clusters {
		s.assignServiceToCluster(cluster)
		if err := cluster.Open(); err != nil {
			return err
		}
	}

	// Define API routes
	s.routes = []httpd.Route{
		{
			Method:      "POST",
			Pattern:     subscriptionsPath,
			HandlerFunc: s.handleSubscriptions,
		},
	}

	if err := s.HTTPDService.AddRoutes(s.routes); err != nil {
		return errors.Wrap(err, "adding API routes")
	}
	// Revoke any tokens for removed clusters.
	err := s.revokeClusterTokens()
	return errors.Wrap(err, "revoking old cluster tokens")
}

func (s *Service) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.opened {
		return nil
	}
	s.opened = false
	s.HTTPDService.DelRoutes(s.routes)
	var lastErr error
	for _, cluster := range s.clusters {
		err := cluster.Close()
		if err != nil {
			lastErr = err
		}
	}
	return lastErr
}

func (s *Service) Update(newConfigs []interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	configs := make([]Config, len(newConfigs))
	for i, c := range newConfigs {
		if config, ok := c.(Config); ok {
			configs[i] = config
		} else {
			return fmt.Errorf("unexpected config object type, got %T exp %T", c, config)
		}
	}
	if err := s.updateConfigs(configs); err != nil {
		return err
	}
	// Revoke any tokens for removed clusters.
	err := s.revokeClusterTokens()
	return errors.Wrap(err, "revoking old cluster tokens")
}

// updateConfigs updates the running configuration for the various clusters.
// Must have the lock to call.
func (s *Service) updateConfigs(configs []Config) error {
	removedClusters := make(map[string]*influxdbCluster, len(configs))
	s.defaultInfluxDB = ""
	enabledCount := 0
	for _, c := range configs {
		cluster, exists := s.clusters[c.Name]
		if !c.Enabled {
			if exists {
				removedClusters[c.Name] = cluster
			}
			// Skip disabled configs
			continue
		}
		enabledCount++
		if exists {
			if err := cluster.Update(c); err != nil {
				return errors.Wrapf(err, "failed to update cluster %s", c.Name)
			}
		} else {
			var err error
			cluster, err = newInfluxDBCluster(c, s.hostname, s.ider, s.httpPort, s.useTokens, s.logger)
			if err != nil {
				return err
			}
			s.assignServiceToCluster(cluster)
			if s.opened {
				if err := cluster.Open(); err != nil {
					return err
				}
			}
			s.clusters[c.Name] = cluster
		}
		if c.Default {
			s.defaultInfluxDB = c.Name
		}
	}
	// If only one enabled cluster assume it is the default
	if enabledCount == 1 {
		for _, c := range configs {
			if c.Enabled {
				s.defaultInfluxDB = c.Name
			}
		}
	}
	if enabledCount > 0 && s.defaultInfluxDB == "" {
		return errors.New("no default cluster found")
	}

	// Find any deleted clusters
	for name, cluster := range s.clusters {
		found := false
		for _, c := range configs {
			if c.Name == name {
				found = true
				break
			}
		}
		if !found {
			removedClusters[name] = cluster
		}
	}

	// Unlink/Close/Delete all removed clusters
	for name, cluster := range removedClusters {
		if err := cluster.UnlinkSubscriptions(); err != nil {
			s.logger.Printf("E! failed to unlink subscriptions for cluster %s: %s", name, err)
		}
		if err := cluster.Close(); err != nil {
			s.logger.Printf("E! failed to close cluster %s: %s", name, err)
		}
		delete(s.clusters, name)
	}
	return nil
}

func (s *Service) assignServiceToCluster(cluster *influxdbCluster) {
	cluster.PointsWriter = s.PointsWriter
	cluster.LogService = s.LogService
	cluster.AuthService = s.AuthService
	cluster.ClientCreator = s.ClientCreator
	cluster.randReader = s.RandReader
}

type testOptions struct {
	Cluster string `json:"cluster"`
}

func (s *Service) TestOptions() interface{} {
	return &testOptions{}
}

func (s *Service) Test(options interface{}) error {
	o, ok := options.(*testOptions)
	if !ok {
		return fmt.Errorf("unexpected options type %T", options)
	}

	// Get cluster
	s.mu.Lock()
	cluster, ok := s.clusters[o.Cluster]
	s.mu.Unlock()
	if !ok {
		return fmt.Errorf("cluster %q is not enabled or does not exist", o.Cluster)
	}

	// Get client and ping the cluster
	cli := cluster.NewClient()
	_, _, err := cli.Ping(nil)
	if err != nil {
		return errors.Wrapf(err, "failed to ping the influxdb cluster %q", o.Cluster)
	}
	return nil
}

// Refresh the subscriptions linking for all clusters.
func (s *Service) handleSubscriptions(w http.ResponseWriter, r *http.Request) {
	err := s.LinkSubscriptions()
	if err != nil {
		httpd.HttpError(w, fmt.Sprintf("failed to link subscriptions: %s", err.Error()), true, http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// Trigger a LinkSubscriptions event for all clusters
func (s *Service) LinkSubscriptions() error {
	for clusterName, cluster := range s.clusters {
		err := cluster.LinkSubscriptions()
		if err != nil {
			return errors.Wrapf(err, "linking cluster %s", clusterName)
		}
	}
	return nil
}

// Revoke tokens that are not associated with any current cluster.
func (s *Service) revokeClusterTokens() error {
	// revoke any extra tokens
	tokens, err := s.AuthService.ListSubscriptionTokens()
	if err != nil {
		return errors.Wrap(err, "getting existing subscription tokens")
	}
	// Check all tokens against configured clusters
	for _, token := range tokens {
		clusterName, _, err := splitToken(token)
		if err != nil {
			// Revoke invalid token
			s.AuthService.RevokeSubscriptionAccess(token)
		} else if _, ok := s.clusters[clusterName]; !ok {
			// Revoke token for old non existent or disabled cluster
			s.AuthService.RevokeSubscriptionAccess(token)
		}
	}
	return nil
}

// NewNamedClient returns a new client for the given name or the default client if the name is empty.
func (s *Service) NewNamedClient(name string) (influxdb.Client, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if name == "" {
		name = s.defaultInfluxDB
	}
	cluster, ok := s.clusters[name]
	if !ok {
		return nil, fmt.Errorf("no such InfluxDB config %s", name)

	}
	return cluster.NewClient(), nil
}

type influxdbCluster struct {
	clusterName              string
	influxdbConfig           influxdb.Config
	client                   influxdb.ClientUpdater
	i                        int
	configSubs               map[subEntry]bool
	exConfigSubs             map[subEntry]bool
	hostname                 string
	httpPort                 int
	logger                   *log.Logger
	protocol                 string
	udpBind                  string
	udpBuffer                int
	udpReadBuffer            int
	startupTimeout           time.Duration
	subscriptionSyncInterval time.Duration
	disableSubs              bool
	runningSubs              map[subEntry]bool
	useTokens                bool

	// ider provides an interface for getting the IDs.
	ider IDer
	// subName is cached copy of the name of the subscriptions.
	subName string

	subSyncTicker *time.Ticker
	services      map[subEntry]openCloser

	randReader io.Reader

	PointsWriter interface {
		WritePoints(database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, points []models.Point) error
	}
	LogService interface {
		NewLogger(string, int) *log.Logger
	}
	ClientCreator interface {
		Create(influxdb.Config) (influxdb.ClientUpdater, error)
	}
	AuthService interface {
		GrantSubscriptionAccess(token, db, rp string) error
		ListSubscriptionTokens() ([]string, error)
		RevokeSubscriptionAccess(token string) error
	}
	ctxMu     sync.Mutex
	ctx       context.Context
	cancelCtx context.CancelFunc

	mu     sync.RWMutex
	opened bool
}

type openCloser interface {
	Open() error
	Close() error
}

type subEntry struct {
	db   string
	rp   string
	name string
}

type subInfo struct {
	Mode         string
	Destinations []string
}

func newInfluxDBCluster(c Config, hostname string, ider IDer, httpPort int, useTokens bool, l *log.Logger) (*influxdbCluster, error) {
	if c.InsecureSkipVerify {
		l.Printf("W! Using InsecureSkipVerify when connecting to InfluxDB @ %v this is insecure!", c.URLs)
	}
	config, err := httpConfig(c)
	if err != nil {
		return nil, err
	}
	subName, err := getSubName(c, ider)
	if err != nil {
		return nil, err
	}
	subs := subsFromConfig(subName, c.Subscriptions)
	exSubs := subsFromConfig(subName, c.ExcludedSubscriptions)
	port := httpPort
	if c.HTTPPort != 0 {
		port = c.HTTPPort
	}
	host := hostname
	if c.KapacitorHostname != "" {
		host = c.KapacitorHostname
	}
	return &influxdbCluster{
		clusterName:              c.Name,
		influxdbConfig:           config,
		configSubs:               subs,
		exConfigSubs:             exSubs,
		hostname:                 host,
		httpPort:                 port,
		logger:                   l,
		udpBind:                  c.UDPBind,
		udpBuffer:                c.UDPBuffer,
		udpReadBuffer:            c.UDPReadBuffer,
		startupTimeout:           time.Duration(c.StartUpTimeout),
		subscriptionSyncInterval: time.Duration(c.SubscriptionSyncInterval),
		ider:        ider,
		subName:     subName,
		disableSubs: c.DisableSubscriptions,
		protocol:    c.SubscriptionProtocol,
		runningSubs: make(map[subEntry]bool, len(c.Subscriptions)),
		services:    make(map[subEntry]openCloser, len(c.Subscriptions)),
		// Do not use tokens for non http protocols
		useTokens: useTokens && (c.SubscriptionProtocol == "http" || c.SubscriptionProtocol == "https"),
	}, nil
}

func httpConfig(c Config) (influxdb.Config, error) {
	tlsConfig, err := getTLSConfig(c.SSLCA, c.SSLCert, c.SSLKey, c.InsecureSkipVerify)
	if err != nil {
		return influxdb.Config{}, errors.Wrap(err, "invalid TLS options")
	}
	tr := &http.Transport{
		Proxy:           http.ProxyFromEnvironment,
		TLSClientConfig: tlsConfig,
	}
	var credentials influxdb.Credentials
	if c.Username != "" {
		credentials = influxdb.Credentials{
			Method:   influxdb.UserAuthentication,
			Username: c.Username,
			Password: c.Password,
		}
	}
	return influxdb.Config{
		URLs:        c.URLs,
		Timeout:     time.Duration(c.Timeout),
		Transport:   tr,
		Credentials: credentials,
	}, nil
}

func subsFromConfig(subName string, s map[string][]string) map[subEntry]bool {
	subs := make(map[subEntry]bool, len(s))
	for cluster, rps := range s {
		for _, rp := range rps {
			se := subEntry{cluster, rp, subName}
			subs[se] = true
		}
	}
	return subs
}

func (c *influxdbCluster) Open() error {
	ctx, cancel := c.setupContext()
	defer cancel()

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.opened {
		return nil
	}
	c.opened = true

	if cli, err := c.ClientCreator.Create(c.influxdbConfig); err != nil {
		return errors.Wrap(err, "failed to create client")
	} else {
		c.client = cli
	}

	c.watchSubs()

	if err := c.linkSubscriptions(ctx, c.subName); err != nil {
		return errors.Wrap(err, "failed to link subscription on startup")
	}
	return nil
}

func (c *influxdbCluster) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.opened {
		return nil
	}
	c.opened = false

	if c.subSyncTicker != nil {
		c.subSyncTicker.Stop()
	}
	if c.client != nil {
		c.client.Close()
	}
	return c.closeServices()
}

// closeServices closes all running services.
// Must have lock to call.
func (c *influxdbCluster) closeServices() error {
	var lastErr error
	for se, service := range c.services {
		delete(c.runningSubs, se)
		delete(c.services, se)
		err := service.Close()
		if err != nil {
			lastErr = err
		}
	}
	return lastErr
}

func getSubName(conf Config, ider IDer) (string, error) {
	switch conf.SubscriptionMode {
	case ClusterMode:
		return subNamePrefix + ider.ClusterID().String(), nil
	case ServerMode:
		return subNamePrefix + ider.ServerID().String(), nil
	default:
		return "", errors.New("invalid subscription mode")
	}
}

func (c *influxdbCluster) Update(conf Config) error {
	// Setup context before getting lock
	ctx, cancel := c.setupContext()

	c.mu.Lock()
	defer c.mu.Unlock()

	// Get new subscription name
	newSubName, err := getSubName(conf, c.ider)
	if err != nil {
		return err
	}

	if conf.InsecureSkipVerify {
		c.logger.Printf("W! Using InsecureSkipVerify when connecting to InfluxDB @ %v this is insecure!", conf.URLs)
	}
	if conf.HTTPPort != 0 {
		c.httpPort = conf.HTTPPort
	}
	if conf.KapacitorHostname != "" {
		c.hostname = conf.KapacitorHostname
	}

	reset := false
	resetServices := func() {
		// Close services and let them get re-opened during linking.
		if !reset {
			c.closeServices()
			reset = true
		}
	}

	if c.udpBind != conf.UDPBind {
		c.udpBind = conf.UDPBind
		// UDP bind changed
		resetServices()
	}
	if c.udpBuffer != conf.UDPBuffer {
		c.udpBuffer = conf.UDPBuffer
		// UDP buffer changed
		resetServices()
	}
	if c.udpReadBuffer != conf.UDPReadBuffer {
		c.udpReadBuffer = conf.UDPReadBuffer
		// UDP read buffer changed
		resetServices()
	}

	// If the cluster is open and either the subscription name changed or the subscriptions are now disabled,
	// we need to unlink existing subscriptions.
	unlinkDone := make(chan struct{})
	oldSubName := c.subName
	c.subName = newSubName
	oldDisableSubscriptions := c.disableSubs
	c.disableSubs = conf.DisableSubscriptions
	if c.opened &&
		((conf.DisableSubscriptions && oldDisableSubscriptions != conf.DisableSubscriptions) ||
			newSubName != oldSubName) {
		go func() {
			c.mu.Lock()
			defer c.mu.Unlock()
			defer close(unlinkDone)
			c.unlinkSubscriptions(oldSubName)
		}()
	} else {
		close(unlinkDone)
	}

	// Check if subscriptions sync interval changed.
	if i := time.Duration(conf.SubscriptionSyncInterval); c.subscriptionSyncInterval != i {
		c.subscriptionSyncInterval = i
		c.watchSubs()
	}

	c.startupTimeout = time.Duration(conf.StartUpTimeout)
	c.protocol = conf.SubscriptionProtocol
	c.influxdbConfig, err = httpConfig(conf)
	if err != nil {
		return err
	}
	if c.client != nil {
		err := c.client.Update(c.influxdbConfig)
		if err != nil {
			return errors.Wrap(err, "failed to update client")
		}
	}
	c.configSubs = subsFromConfig(c.subName, conf.Subscriptions)
	c.exConfigSubs = subsFromConfig(c.subName, conf.ExcludedSubscriptions)

	// Run linkSubscriptions in the background as it can take a while
	// because of validateClientWithBackoff.
	if c.opened {
		go func() {
			// Wait for any unlinking to finish
			<-unlinkDone

			c.mu.Lock()
			defer c.mu.Unlock()
			defer cancel()

			err := c.linkSubscriptions(ctx, newSubName)
			if err != nil {
				c.logger.Printf("E! failed to link subscriptions for cluster %s: %v", c.clusterName, err)
			}
		}()
	}

	return nil
}

// watchSubs setups the goroutine to watch the subscriptions and continuously link them.
// The caller must have the lock.
func (c *influxdbCluster) watchSubs() {
	if c.subSyncTicker != nil {
		c.subSyncTicker.Stop()
	}
	if !c.disableSubs && c.subscriptionSyncInterval != 0 {
		c.subSyncTicker = time.NewTicker(c.subscriptionSyncInterval)
		ticker := c.subSyncTicker
		go func() {
			for _ = range ticker.C {
				c.LinkSubscriptions()
			}
		}()
	}
}

func (c *influxdbCluster) NewClient() influxdb.Client {
	return c.client
}

// validateClientWithBackoff repeatedly calls client.Ping until either
// a successfull response or the context is canceled.
func (c *influxdbCluster) validateClientWithBackoff(ctx context.Context) error {
	b := backoff.NewExponentialBackOff()
	b.MaxElapsedTime = c.startupTimeout
	ticker := backoff.NewTicker(b)
	defer ticker.Stop()
	done := ctx.Done()
	for {
		select {
		case <-done:
			return errors.New("canceled")
		case _, ok := <-ticker.C:
			if !ok {
				return errors.New("failed to connect to InfluxDB, retry limit reached")
			}
			_, _, err := c.client.Ping(ctx)
			if err != nil {
				c.logger.Println("D! failed to connect to InfluxDB, retrying... ", err)
				continue
			}
			return nil
		}
	}
}

// UnlinkSubscriptions acquires the lock and then unlinks the subscriptions
func (c *influxdbCluster) UnlinkSubscriptions() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.unlinkSubscriptions(c.subName)
}

// unlinkSubscriptions, you must have the lock to call this function.
func (c *influxdbCluster) unlinkSubscriptions(subName string) error {
	c.logger.Println("D! unlinking subscriptions for cluster", c.clusterName)
	// Get all existing subscriptions
	resp, err := c.execQuery(&influxql.ShowSubscriptionsStatement{})
	if err != nil {
		return err
	}
	for _, res := range resp.Results {
		for _, series := range res.Series {
			for _, v := range series.Values {
				se := subEntry{
					db: series.Name,
				}
				for i, c := range series.Columns {
					switch c {
					case "retention_policy":
						se.rp = v[i].(string)
					case "name":
						se.name = v[i].(string)
					}
				}
				if se.name == subName {
					c.dropSub(se.name, se.db, se.rp)
					c.closeSub(se)
				}
			}
		}
	}
	vars.NumSubscriptionsVar.Set(c.clusterName, 0)
	return nil
}

// setupContext returns a context, the previous context will be canceled if it exists.
// Must be called without the mu lock.
func (c *influxdbCluster) setupContext() (context.Context, context.CancelFunc) {
	// Check existing context
	c.ctxMu.Lock()
	defer c.ctxMu.Unlock()
	if c.ctx != nil {
		// Cancel existing context
		c.cancelCtx()
	}

	c.ctx, c.cancelCtx = context.WithCancel(context.Background())
	return c.ctx, c.cancelCtx
}

// LinkSubscriptions acquires the lock and then links the subscriptions.
func (c *influxdbCluster) LinkSubscriptions() error {
	ctx, cancel := c.setupContext()
	defer cancel()

	c.mu.Lock()
	defer c.mu.Unlock()
	return c.linkSubscriptions(ctx, c.subName)
}

// linkSubscriptions you must have the lock to call this method.
func (c *influxdbCluster) linkSubscriptions(ctx context.Context, subName string) error {
	if c.disableSubs {
		return nil
	}

	c.logger.Println("D! linking subscriptions for cluster", c.clusterName)
	err := c.validateClientWithBackoff(ctx)
	if err != nil {
		return err
	}

	// Get all databases and retention policies
	var allSubs []subEntry
	resp, err := c.execQuery(&influxql.ShowDatabasesStatement{})
	if err != nil {
		return err
	}

	if len(resp.Results) == 1 && len(resp.Results[0].Series) == 1 && len(resp.Results[0].Series[0].Values) > 0 {
		dbs := resp.Results[0].Series[0].Values
		for _, v := range dbs {
			db := v[0].(string)

			rpResp, err := c.execQuery(&influxql.ShowRetentionPoliciesStatement{
				Database: db,
			})
			if err != nil {
				return err
			}
			if len(rpResp.Results) == 1 && len(rpResp.Results[0].Series) == 1 && len(rpResp.Results[0].Series[0].Values) > 0 {
				rps := rpResp.Results[0].Series[0].Values
				for _, v := range rps {
					rpname := v[0].(string)

					se := subEntry{
						db:   db,
						rp:   rpname,
						name: subName,
					}
					allSubs = append(allSubs, se)
				}
			}
		}
	}

	// Get all existing subscriptions
	resp, err = c.execQuery(&influxql.ShowSubscriptionsStatement{})
	if err != nil {
		return err
	}
	// Populate a set of existing subscriptions
	existingSubs := make(map[subEntry]subInfo)
	for _, res := range resp.Results {
		for _, series := range res.Series {
			for _, v := range series.Values {
				se := subEntry{
					db: series.Name,
				}
				si := subInfo{}
				for i, c := range series.Columns {
					switch c {
					case "retention_policy":
						se.rp = v[i].(string)
					case "name":
						se.name = v[i].(string)
					case "mode":
						si.Mode = v[i].(string)
					case "destinations":
						destinations := v[i].([]interface{})
						si.Destinations = make([]string, len(destinations))
						for i := range destinations {
							si.Destinations[i] = destinations[i].(string)
						}
					}
				}
				if se.name == legacySubName {
					// This is an old-style subscription,
					// drop it and recreate with new name.
					err := c.dropSub(se.name, se.db, se.rp)
					if err != nil {
						return err
					}
					se.name = subName
					err = c.createSub(se.name, se.db, se.rp, si.Mode, si.Destinations)
					if err != nil {
						return err
					}
					existingSubs[se] = si
				} else if se.name == c.ider.ClusterID().String() {
					// This is an just the cluster ID
					// drop it and recreate with new name.
					err := c.dropSub(se.name, se.db, se.rp)
					if err != nil {
						return err
					}
					se.name = subName
					err = c.createSub(se.name, se.db, se.rp, si.Mode, si.Destinations)
					if err != nil {
						return err
					}
					existingSubs[se] = si
				} else if se.name == subName {
					// Check if the something has changed or is invalid.
					if c.changedOrInvalid(se, si) {
						// Something changed or is invalid, drop the sub and let it get recreated
						c.dropSub(se.name, se.db, se.rp)
						c.closeSub(se)
					} else {
						existingSubs[se] = si
					}
				}
			}
		}
	}

	// start any missing subscriptions
	// and drop any extra subs
	for se, si := range existingSubs {
		shouldExist := c.shouldSubExist(se)
		if shouldExist && !c.runningSubs[se] {
			// Check if this kapacitor instance is in the list of hosts
			for _, dest := range si.Destinations {
				u, err := url.Parse(dest)
				if err != nil {
					c.logger.Println("E! invalid URL in subscription destinations:", err)
					continue
				}
				host, port, err := net.SplitHostPort(u.Host)
				if err != nil {
					c.logger.Println("E! invalid host in subscription:", err)
					continue
				}
				if host == c.hostname {
					if u.Scheme == "udp" {
						_, err := c.startUDPListener(se, port)
						if err != nil {
							c.logger.Println("E! failed to start UDP listener:", err)
						}
					}
					c.runningSubs[se] = true
					break
				}
			}
		} else if !shouldExist {
			// Drop extra sub
			c.dropSub(se.name, se.db, se.rp)
			// Remove from existing list
			delete(existingSubs, se)
		}
	}

	// create and start any new subscriptions
	for _, se := range allSubs {
		_, exists := existingSubs[se]
		// If we have been configured to subscribe and the subscription is not created/started yet.
		if c.shouldSubExist(se) && !(c.runningSubs[se] && exists) {
			var destination string
			switch c.protocol {
			case "http", "https":
				if c.useTokens {
					// Generate token
					token, err := c.generateRandomToken()
					if err != nil {
						return errors.Wrap(err, "generating token")
					}
					err = c.AuthService.GrantSubscriptionAccess(token, se.db, se.rp)
					if err != nil {
						return err
					}
					u := url.URL{
						Scheme: c.protocol,
						User:   url.UserPassword(httpd.SubscriptionUser, token),
						Host:   fmt.Sprintf("%s:%d", c.hostname, c.httpPort),
					}
					destination = u.String()
				} else {
					u := url.URL{
						Scheme: c.protocol,
						Host:   fmt.Sprintf("%s:%d", c.hostname, c.httpPort),
					}
					destination = u.String()
				}
			case "udp":
				addr, err := c.startUDPListener(se, "0")
				if err != nil {
					c.logger.Println("E! failed to start UDP listener:", err)
				}
				destination = fmt.Sprintf("udp://%s:%d", c.hostname, addr.Port)
			}

			mode := "ANY"
			destinations := []string{destination}
			err = c.createSub(se.name, se.db, se.rp, mode, destinations)
			if err != nil {
				return err
			}
			// Mark as running
			c.runningSubs[se] = true
			// Update exiting set
			existingSubs[se] = subInfo{
				Mode:         mode,
				Destinations: destinations,
			}
		}
	}

	// revoke any extra tokens
	tokens, err := c.AuthService.ListSubscriptionTokens()
	if err != nil {
		return errors.Wrap(err, "getting existing subscription tokens")
	}
	// populate set of existing tokens.
	existingTokens := make(map[string]bool, len(existingSubs))
	if c.useTokens {
		for _, si := range existingSubs {
			u, err := url.Parse(si.Destinations[0])
			if err != nil || u.User == nil {
				continue
			}
			if t, ok := u.User.Password(); ok {
				existingTokens[t] = true
			}
		}
	}
	// Check all tokens against existing tokens
	for _, token := range tokens {
		clusterName, _, err := splitToken(token)
		// Skip invalid token or token from another cluster
		if err != nil || clusterName != c.clusterName {
			continue
		}
		// If the token is not part of the existing set we need to revoke
		if !existingTokens[token] {
			c.AuthService.RevokeSubscriptionAccess(token)
		}
	}

	// Close any subs for dbs that have been dropped
	for se, running := range c.runningSubs {
		if !running {
			continue
		}
		if _, exists := existingSubs[se]; !exists {
			err := c.closeSub(se)
			if err != nil {
				c.logger.Printf("E! failed to close service for %v: %s", se, err)
			}
		}
	}

	vars.NumSubscriptionsVar.Set(c.clusterName, int64(len(existingSubs)))
	return nil
}

func (c *influxdbCluster) shouldSubExist(se subEntry) bool {
	return (len(c.configSubs) == 0 || c.configSubs[se]) && !c.exConfigSubs[se]
}

// Determine whether a subscription has differing values from the config.
func (c *influxdbCluster) changedOrInvalid(se subEntry, si subInfo) bool {
	// Validate destinations
	if len(si.Destinations) == 0 {
		return true
	}
	u, err := url.Parse(si.Destinations[0])
	if err != nil {
		return true
	}
	if u.Scheme != c.protocol {
		return true
	}

	host, port, err := net.SplitHostPort(u.Host)
	if err != nil {
		return true
	}
	if host != c.hostname {
		return true
	}

	// Further checks for HTTP protocols
	if u.Scheme == "http" || u.Scheme == "https" {
		// Check the port
		pn, err := strconv.ParseInt(port, 10, 64)
		if err != nil {
			return true
		}
		if int(pn) != c.httpPort {
			return true
		}
		// Further checks for the user token
		if !c.useTokens && u.User != nil {
			return true
		}
		if c.useTokens {
			if u.User == nil || u.User.Username() != httpd.SubscriptionUser {
				return true
			}
			t, ok := u.User.Password()
			if !ok {
				return true
			}
			clusterName, _, err := splitToken(t)
			if err != nil || clusterName != c.clusterName {
				return true
			}
		}
	}
	return false
}

// Close the service and stop tracking it.
func (c *influxdbCluster) closeSub(se subEntry) (err error) {
	if service, ok := c.services[se]; ok {
		err = service.Close()
	}
	delete(c.runningSubs, se)
	delete(c.services, se)
	return
}

const (
	// Delimiter used to separate cluster name and token data in a token.
	tokenDelimiter = ';'
)

// Split a token into its cluster name and token data.
func splitToken(token string) (string, string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return "", "", errors.Wrap(err, "base64 decode")
	}
	rawStr := string(raw)
	i := strings.IndexRune(rawStr, tokenDelimiter)
	if i <= 0 {
		return "", "", errors.New("invalid token")
	}
	return rawStr[:i], rawStr[i+1:], nil
}

// Generate a token tagged with the cluster name.
// <clustername>;<token>
func (c *influxdbCluster) generateRandomToken() (string, error) {
	l := len(c.clusterName)
	tokenBytes := make([]byte, l+tokenSize+1)
	copy(tokenBytes[:l], []byte(c.clusterName))
	tokenBytes[l] = tokenDelimiter
	if _, err := io.ReadFull(c.randReader, tokenBytes[l+1:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(tokenBytes), nil
}

func (c *influxdbCluster) createSub(name, cluster, rp, mode string, destinations []string) error {
	var buf bytes.Buffer
	for i, dst := range destinations {
		if i != 0 {
			buf.Write([]byte(", "))
		}
		buf.Write([]byte("'"))
		buf.Write([]byte(dst))
		buf.Write([]byte("'"))
	}
	_, err := c.execQuery(
		&influxql.CreateSubscriptionStatement{
			Name:            name,
			Database:        cluster,
			RetentionPolicy: rp,
			Destinations:    destinations,
			Mode:            strings.ToUpper(mode),
		},
	)
	return errors.Wrapf(err, "creating sub %s for db %q and rp %q", name, cluster, rp)

}
func (c *influxdbCluster) dropSub(name, cluster, rp string) (err error) {
	_, err = c.execQuery(
		&influxql.DropSubscriptionStatement{
			Name:            name,
			Database:        cluster,
			RetentionPolicy: rp,
		},
	)
	return
}

func (c *influxdbCluster) startUDPListener(se subEntry, port string) (*net.UDPAddr, error) {
	conf := udp.Config{}
	conf.Enabled = true
	conf.BindAddress = fmt.Sprintf("%s:%s", c.udpBind, port)
	conf.Database = se.db
	conf.RetentionPolicy = se.rp
	conf.Buffer = c.udpBuffer
	conf.ReadBuffer = c.udpReadBuffer

	l := c.LogService.NewLogger(fmt.Sprintf("[udp:%s.%s] ", se.db, se.rp), log.LstdFlags)
	service := udp.NewService(conf, l)
	service.PointsWriter = c.PointsWriter
	err := service.Open()
	if err != nil {
		return nil, err
	}
	c.services[se] = service
	c.logger.Println("I! started UDP listener for", se.db, se.rp)
	return service.Addr(), nil
}

func (c *influxdbCluster) execQuery(q influxql.Statement) (*influxdb.Response, error) {
	query := influxdb.Query{
		Command: q.String(),
	}
	resp, err := c.client.Query(query)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// getTLSConfig creates a tls.Config object from the given certs, key, and CA files.
// you must give the full path to the files.
func getTLSConfig(
	SSLCA, SSLCert, SSLKey string,
	InsecureSkipVerify bool,
) (*tls.Config, error) {
	t := &tls.Config{
		InsecureSkipVerify: InsecureSkipVerify,
	}
	if SSLCert != "" && SSLKey != "" {
		cert, err := tls.LoadX509KeyPair(SSLCert, SSLKey)
		if err != nil {
			return nil, fmt.Errorf(
				"Could not load TLS client key/certificate: %s",
				err)
		}
		t.Certificates = []tls.Certificate{cert}
	} else if SSLCert != "" {
		return nil, errors.New("Must provide both key and cert files: only cert file provided.")
	} else if SSLKey != "" {
		return nil, errors.New("Must provide both key and cert files: only key file provided.")
	}

	if SSLCA != "" {
		caCert, err := ioutil.ReadFile(SSLCA)
		if err != nil {
			return nil, fmt.Errorf("Could not load TLS CA: %s",
				err)
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
		t.RootCAs = caCertPool
	}
	return t, nil
}
