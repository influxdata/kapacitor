package influxdb

import (
	"bytes"
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
	"github.com/influxdata/kapacitor"
	"github.com/influxdata/kapacitor/influxdb"
	"github.com/influxdata/kapacitor/services/httpd"
	"github.com/influxdata/kapacitor/services/udp"
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

// Handles requests to write or read from an InfluxDB cluster
type Service struct {
	defaultInfluxDB string
	clusters        map[string]*influxdbCluster
	routes          []httpd.Route

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
		Create(influxdb.HTTPConfig) (influxdb.Client, error)
	}
	AuthService interface {
		GrantSubscriptionAccess(token, db, rp string) error
		ListSubscriptionTokens() ([]string, error)
		RevokeSubscriptionAccess(token string) error
	}
	RandReader io.Reader
	logger     *log.Logger
}

func NewService(configs []Config, httpPort int, hostname string, useTokens bool, l *log.Logger) *Service {
	clusterID := kapacitor.ClusterIDVar.StringValue()
	subName := subNamePrefix + clusterID
	clusters := make(map[string]*influxdbCluster, len(configs))
	var defaultInfluxDBName string
	for _, c := range configs {
		if !c.Enabled {
			// Skip disabled configs
			continue
		}
		urls := make([]influxdb.HTTPConfig, len(c.URLs))
		// Config should have been validated already, ignore error
		tlsConfig, _ := getTLSConfig(c.SSLCA, c.SSLCert, c.SSLKey, c.InsecureSkipVerify)
		if c.InsecureSkipVerify {
			l.Printf("W! Using InsecureSkipVerify when connecting to InfluxDB @ %v this is insecure!", c.URLs)
		}
		var credentials *influxdb.Credentials
		if c.Username != "" {
			credentials = &influxdb.Credentials{
				Method:   influxdb.UserAuthentication,
				Username: c.Username,
				Password: c.Password,
			}
		}
		for i, u := range c.URLs {
			urls[i] = influxdb.HTTPConfig{
				URL:         u,
				Credentials: credentials,
				UserAgent:   "Kapacitor",
				Timeout:     time.Duration(c.Timeout),
				TLSConfig:   tlsConfig,
			}
		}
		subs := make(map[subEntry]bool, len(c.Subscriptions))
		for cluster, rps := range c.Subscriptions {
			for _, rp := range rps {
				se := subEntry{cluster, rp, subName}
				subs[se] = true
			}
		}
		exSubs := make(map[subEntry]bool, len(c.ExcludedSubscriptions))
		for cluster, rps := range c.ExcludedSubscriptions {
			for _, rp := range rps {
				se := subEntry{cluster, rp, subName}
				exSubs[se] = true
			}
		}
		runningSubs := make(map[subEntry]bool, len(c.Subscriptions))
		services := make(map[subEntry]openCloser, len(c.Subscriptions))
		port := httpPort
		if c.HTTPPort != 0 {
			port = c.HTTPPort
		}
		host := hostname
		if c.KapacitorHostname != "" {
			host = c.KapacitorHostname
		}
		clusters[c.Name] = &influxdbCluster{
			clusterName:              c.Name,
			configs:                  urls,
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
			clusterID:                clusterID,
			subName:                  subName,
			disableSubs:              c.DisableSubscriptions,
			protocol:                 c.SubscriptionProtocol,
			runningSubs:              runningSubs,
			services:                 services,
			// Do not use tokens for non http protocols
			useTokens: useTokens && (c.SubscriptionProtocol == "http" || c.SubscriptionProtocol == "https"),
		}
		if c.Default {
			defaultInfluxDBName = c.Name
		}
	}
	return &Service{
		defaultInfluxDB: defaultInfluxDBName,
		clusters:        clusters,
		logger:          l,
		RandReader:      rand.Reader,
	}
}

func (s *Service) Open() error {
	for _, cluster := range s.clusters {
		cluster.PointsWriter = s.PointsWriter
		cluster.LogService = s.LogService
		cluster.AuthService = s.AuthService
		cluster.ClientCreator = s.ClientCreator
		cluster.randReader = s.RandReader
		err := cluster.Open()
		if err != nil {
			return err
		}
	}

	// Define API routes
	s.routes = []httpd.Route{
		{
			Name:        "subscriptions",
			Method:      "POST",
			Pattern:     subscriptionsPath,
			HandlerFunc: s.handleSubscriptions,
		},
	}

	err := s.HTTPDService.AddRoutes(s.routes)
	if err != nil {
		return errors.Wrap(err, "adding API routes")
	}
	// Revoke any tokens for removed clusters.
	err = s.revokeClusterTokens()
	return errors.Wrap(err, "revoking old cluster tokens")
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

// Trigger a linkSubscriptions event for all clusters
func (s *Service) LinkSubscriptions() error {
	for clusterName, cluster := range s.clusters {
		err := cluster.linkSubscriptions()
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
			// Revoke token for old non existant cluster
			s.AuthService.RevokeSubscriptionAccess(token)
		}
	}
	return nil
}

func (s *Service) Close() error {
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

func (s *Service) NewDefaultClient() (influxdb.Client, error) {
	return s.clusters[s.defaultInfluxDB].NewClient()
}

func (s *Service) NewNamedClient(name string) (influxdb.Client, error) {
	cluster, ok := s.clusters[name]
	if !ok {
		return nil, fmt.Errorf("no such InfluxDB config %s", name)

	}
	return cluster.NewClient()
}

type influxdbCluster struct {
	clusterName              string
	configs                  []influxdb.HTTPConfig
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

	clusterID     string
	subName       string
	subSyncTicker *time.Ticker
	services      map[subEntry]openCloser

	PointsWriter interface {
		WritePoints(database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, points []models.Point) error
	}
	LogService interface {
		NewLogger(string, int) *log.Logger
	}
	ClientCreator interface {
		Create(influxdb.HTTPConfig) (influxdb.Client, error)
	}
	AuthService interface {
		GrantSubscriptionAccess(token, db, rp string) error
		ListSubscriptionTokens() ([]string, error)
		RevokeSubscriptionAccess(token string) error
	}

	randReader io.Reader

	mu sync.Mutex
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

func (s *influxdbCluster) Open() error {
	s.mu.Lock()
	if !s.disableSubs {
		if s.subscriptionSyncInterval != 0 {
			s.subSyncTicker = time.NewTicker(s.subscriptionSyncInterval)
			go func() {
				for _ = range s.subSyncTicker.C {
					s.linkSubscriptions()
				}
			}()
		}
	}
	// Release lock so we can call linkSubscriptions.
	s.mu.Unlock()
	return s.linkSubscriptions()
}

func (s *influxdbCluster) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.subSyncTicker != nil {
		s.subSyncTicker.Stop()
	}

	var lastErr error
	for _, service := range s.services {
		err := service.Close()
		if err != nil {
			lastErr = err
		}
	}
	return lastErr
}

func (s *influxdbCluster) Addr() string {
	config := s.configs[s.i]
	s.i = (s.i + 1) % len(s.configs)
	return config.URL
}

func (s *influxdbCluster) NewClient() (c influxdb.Client, err error) {
	tries := 0
	for tries < len(s.configs) {
		tries++
		config := s.configs[s.i]
		s.i = (s.i + 1) % len(s.configs)
		c, err = s.ClientCreator.Create(config)
		if err != nil {
			continue
		}
		_, _, err = c.Ping(config.Timeout)
		if err != nil {
			continue
		}
		return
	}
	return
}

func (s *influxdbCluster) linkSubscriptions() error {
	if s.disableSubs {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.logger.Println("D! linking subscriptions for cluster", s.clusterName)
	b := backoff.NewExponentialBackOff()
	b.MaxElapsedTime = s.startupTimeout
	ticker := backoff.NewTicker(b)
	var err error
	var cli influxdb.Client
	for range ticker.C {
		cli, err = s.NewClient()
		if err != nil {
			s.logger.Println("D! failed to connect to InfluxDB, retrying... ", err)
			continue
		}
		ticker.Stop()
		break
	}
	if err != nil {
		return err
	}

	numSubscriptions := int64(0)

	// Get all databases and retention policies
	var allSubs []subEntry
	resp, err := s.execQuery(cli, &influxql.ShowDatabasesStatement{})
	if err != nil {
		return err
	}

	if len(resp.Results) == 1 && len(resp.Results[0].Series) == 1 && len(resp.Results[0].Series[0].Values) > 0 {
		dbs := resp.Results[0].Series[0].Values
		for _, v := range dbs {
			db := v[0].(string)

			rpResp, err := s.execQuery(cli, &influxql.ShowRetentionPoliciesStatement{
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
						name: s.subName,
					}
					allSubs = append(allSubs, se)
				}
			}
		}
	}

	// Get all existing subscriptions
	resp, err = s.execQuery(cli, &influxql.ShowSubscriptionsStatement{})
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
					err := s.dropSub(cli, se.name, se.db, se.rp)
					if err != nil {
						return err
					}
					se.name = s.subName
					err = s.createSub(cli, se.name, se.db, se.rp, si.Mode, si.Destinations)
					if err != nil {
						return err
					}
					existingSubs[se] = si
				} else if se.name == s.clusterID {
					// This is an just the cluster ID
					// drop it and recreate with new name.
					err := s.dropSub(cli, se.name, se.db, se.rp)
					se.name = s.subName
					err = s.createSub(cli, se.name, se.db, se.rp, si.Mode, si.Destinations)
					if err != nil {
						return err
					}
					existingSubs[se] = si
				} else if se.name == s.subName {
					// Check if the something has changed or is invalid.
					if s.changedOrInvalid(se, si) {
						// Something changed or is invalid, drop the sub and let it get recreated
						s.dropSub(cli, se.name, se.db, se.rp)
						s.closeSub(se)
					} else {
						existingSubs[se] = si
					}
				}
			}
		}
	}

	// Compare existing subs to configured list
	all := len(s.configSubs) == 0
	for se, si := range existingSubs {
		if (s.configSubs[se] || all) && !s.exConfigSubs[se] && !s.runningSubs[se] {
			// Check if this kapacitor instance is in the list of hosts
			for _, dest := range si.Destinations {
				u, err := url.Parse(dest)
				if err != nil {
					s.logger.Println("E! invalid URL in subscription destinations:", err)
					continue
				}
				host, port, err := net.SplitHostPort(u.Host)
				if host == s.hostname {
					numSubscriptions++
					if u.Scheme == "udp" {
						_, err := s.startUDPListener(se, port)
						if err != nil {
							s.logger.Println("E! failed to start UDP listener:", err)
						}
					}
					s.runningSubs[se] = true
					break
				}
			}
		}
	}
	// create and start any new subscriptions
	// stop any removed subscriptions
	for _, se := range allSubs {
		_, exists := existingSubs[se]
		// If we have been configured to subscribe and the subscription is not started yet.
		if (s.configSubs[se] || all) && !s.exConfigSubs[se] && !(s.runningSubs[se] && exists) {
			var destination string
			switch s.protocol {
			case "http", "https":
				if s.useTokens {
					// Generate token
					token, err := s.generateRandomToken()
					if err != nil {
						return errors.Wrap(err, "generating token")
					}
					err = s.AuthService.GrantSubscriptionAccess(token, se.db, se.rp)
					if err != nil {
						return err
					}
					u := url.URL{
						Scheme: s.protocol,
						User:   url.UserPassword(httpd.SubscriptionUser, token),
						Host:   fmt.Sprintf("%s:%d", s.hostname, s.httpPort),
					}
					destination = u.String()
				} else {
					u := url.URL{
						Scheme: s.protocol,
						Host:   fmt.Sprintf("%s:%d", s.hostname, s.httpPort),
					}
					destination = u.String()
				}
			case "udp":
				addr, err := s.startUDPListener(se, "0")
				if err != nil {
					s.logger.Println("E! failed to start UDP listener:", err)
				}
				destination = fmt.Sprintf("udp://%s:%d", s.hostname, addr.Port)
			}

			numSubscriptions++

			mode := "ANY"
			destinations := []string{destination}
			err = s.createSub(cli, se.name, se.db, se.rp, mode, destinations)
			if err != nil {
				return err
			}
			// Mark as running
			s.runningSubs[se] = true
			// Update exiting set
			existingSubs[se] = subInfo{
				Mode:         mode,
				Destinations: destinations,
			}
		}
	}

	// revoke any extra tokens
	tokens, err := s.AuthService.ListSubscriptionTokens()
	if err != nil {
		return errors.Wrap(err, "getting existing subscription tokens")
	}
	// populate set of existing tokens.
	existingTokens := make(map[string]bool, len(existingSubs))
	if s.useTokens {
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
		if err != nil || clusterName != s.clusterName {
			continue
		}
		// If the token is not part of the existing set we need to revoke
		if !existingTokens[token] {
			s.AuthService.RevokeSubscriptionAccess(token)
		}
	}

	// Close any subs for dbs that have been dropped
	for se, running := range s.runningSubs {
		if !running {
			continue
		}
		if _, exists := existingSubs[se]; !exists {
			err := s.closeSub(se)
			if err != nil {
				s.logger.Printf("E! failed to close service for %v: %s", se, err)
			}
		}
	}

	kapacitor.NumSubscriptionsVar.Set(numSubscriptions)
	return nil
}

// Determine whether a subscription has differing values from the config.
func (s *influxdbCluster) changedOrInvalid(se subEntry, si subInfo) bool {
	// Validate destinations
	if len(si.Destinations) == 0 {
		return true
	}
	u, err := url.Parse(si.Destinations[0])
	if err != nil {
		return true
	}
	if u.Scheme != s.protocol {
		return true
	}

	host, port, err := net.SplitHostPort(u.Host)
	if err != nil {
		return true
	}
	if host != s.hostname {
		return true
	}

	// Further checks for HTTP protocols
	if u.Scheme == "http" || u.Scheme == "https" {
		// Check the port
		pn, err := strconv.ParseInt(port, 10, 64)
		if err != nil {
			return true
		}
		if int(pn) != s.httpPort {
			return true
		}
		// Further checks for the user token
		if !s.useTokens && u.User != nil {
			return true
		}
		if s.useTokens {
			if u.User == nil || u.User.Username() != httpd.SubscriptionUser {
				return true
			}
			t, ok := u.User.Password()
			if !ok {
				return true
			}
			clusterName, _, err := splitToken(t)
			if err != nil || clusterName != s.clusterName {
				return true
			}
		}
	}
	return false
}

// Close the service and stop tracking it.
func (s *influxdbCluster) closeSub(se subEntry) (err error) {
	if service, ok := s.services[se]; ok {
		s.logger.Println("D! closing service for", se)
		err = service.Close()
	}
	delete(s.runningSubs, se)
	delete(s.services, se)
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
func (s *influxdbCluster) generateRandomToken() (string, error) {
	l := len(s.clusterName)
	tokenBytes := make([]byte, l+tokenSize+1)
	copy(tokenBytes[:l], []byte(s.clusterName))
	tokenBytes[l] = tokenDelimiter
	if _, err := io.ReadFull(s.randReader, tokenBytes[l+1:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(tokenBytes), nil
}

func (s *influxdbCluster) createSub(cli influxdb.Client, name, cluster, rp, mode string, destinations []string) error {
	var buf bytes.Buffer
	for i, dst := range destinations {
		if i != 0 {
			buf.Write([]byte(", "))
		}
		buf.Write([]byte("'"))
		buf.Write([]byte(dst))
		buf.Write([]byte("'"))
	}
	_, err := s.execQuery(
		cli,
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
func (s *influxdbCluster) dropSub(cli influxdb.Client, name, cluster, rp string) (err error) {
	_, err = s.execQuery(
		cli,
		&influxql.DropSubscriptionStatement{
			Name:            name,
			Database:        cluster,
			RetentionPolicy: rp,
		},
	)
	return
}

func (s *influxdbCluster) startUDPListener(se subEntry, port string) (*net.UDPAddr, error) {
	c := udp.Config{}
	c.Enabled = true
	c.BindAddress = fmt.Sprintf("%s:%s", s.udpBind, port)
	c.Database = se.db
	c.RetentionPolicy = se.rp
	c.Buffer = s.udpBuffer
	c.ReadBuffer = s.udpReadBuffer

	l := s.LogService.NewLogger(fmt.Sprintf("[udp:%s.%s] ", se.db, se.rp), log.LstdFlags)
	service := udp.NewService(c, l)
	service.PointsWriter = s.PointsWriter
	err := service.Open()
	if err != nil {
		return nil, err
	}
	s.services[se] = service
	s.logger.Println("I! started UDP listener for", se.db, se.rp)
	return service.Addr(), nil
}

func (s *influxdbCluster) execQuery(cli influxdb.Client, q influxql.Statement) (*influxdb.Response, error) {
	query := influxdb.Query{
		Command: q.String(),
	}
	resp, err := cli.Query(query)
	if err != nil {
		return nil, err
	}
	if err := resp.Error(); err != nil {
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
