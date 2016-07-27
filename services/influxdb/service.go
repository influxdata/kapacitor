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
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	client "github.com/influxdata/influxdb/client/v2"
	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/kapacitor"
	"github.com/influxdata/kapacitor/services/udp"
	"github.com/pkg/errors"
)

const (
	// Legacy name given to all subscriptions.
	legacySubName = "kapacitor"
	subNamePrefix = "kapacitor-"

	// Size in bytes of a token for subscription authentication
	tokenSize = 64
)

// Handles requests to write or read from an InfluxDB cluster
type Service struct {
	defaultInfluxDB string
	clusters        map[string]*influxdb

	PointsWriter interface {
		WritePoints(database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, points []models.Point) error
	}
	LogService interface {
		NewLogger(string, int) *log.Logger
	}

	AuthService interface {
		GrantSubscriptionAccess(token, db, rp string) error
		RevokeSubscriptionAccess(token string) error
	}
	logger *log.Logger
}

func NewService(configs []Config, defaultInfluxDB, httpPort int, hostname string, useTokens bool, l *log.Logger) *Service {
	clusterID := kapacitor.ClusterIDVar.StringValue()
	subName := subNamePrefix + clusterID
	clusters := make(map[string]*influxdb, len(configs))
	var defaultInfluxDBName string
	for i, c := range configs {
		urls := make([]client.HTTPConfig, len(c.URLs))
		tlsConfig, err := getTLSConfig(c.SSLCA, c.SSLCert, c.SSLKey, c.InsecureSkipVerify)
		if err != nil {
			// Config should have been validated already
			panic(err)
		}
		if c.InsecureSkipVerify {
			l.Printf("W! Using InsecureSkipVerify when connecting to InfluxDB @ %v this is insecure!", c.URLs)
		}
		for i, u := range c.URLs {
			urls[i] = client.HTTPConfig{
				Addr:      u,
				Username:  c.Username,
				Password:  c.Password,
				UserAgent: "Kapacitor",
				Timeout:   time.Duration(c.Timeout),
				TLSConfig: tlsConfig,
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
		clusters[c.Name] = &influxdb{
			configs:                  urls,
			configSubs:               subs,
			exConfigSubs:             exSubs,
			hostname:                 hostname,
			httpPort:                 httpPort,
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
			useTokens:                useTokens,
		}
		if defaultInfluxDB == i {
			defaultInfluxDBName = c.Name
		}
	}
	return &Service{
		defaultInfluxDB: defaultInfluxDBName,
		clusters:        clusters,
		logger:          l,
	}
}

func (s *Service) Open() error {
	for _, cluster := range s.clusters {
		cluster.PointsWriter = s.PointsWriter
		cluster.LogService = s.LogService
		cluster.AuthService = s.AuthService
		err := cluster.Open()
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) Close() error {
	var lastErr error
	for _, cluster := range s.clusters {
		err := cluster.Close()
		if err != nil {
			lastErr = err
		}
	}
	return lastErr
}

func (s *Service) NewDefaultClient() (client.Client, error) {
	return s.clusters[s.defaultInfluxDB].NewClient()
}

func (s *Service) NewNamedClient(name string) (client.Client, error) {
	cluster, ok := s.clusters[name]
	if !ok {
		return nil, fmt.Errorf("no such InfluxDB config %s", name)

	}
	return cluster.NewClient()
}

type influxdb struct {
	configs                  []client.HTTPConfig
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

	PointsWriter interface {
		WritePoints(database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, points []models.Point) error
	}
	LogService interface {
		NewLogger(string, int) *log.Logger
	}
	AuthService interface {
		GrantSubscriptionAccess(token, db, rp string) error
		RevokeSubscriptionAccess(token string) error
	}

	services []interface {
		Open() error
		Close() error
	}

	mu sync.Mutex
}

type subEntry struct {
	cluster string
	rp      string
	name    string
}

type subInfo struct {
	Mode         string
	Destinations []string
}

func (s *influxdb) Open() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.disableSubs {
		err := s.linkSubscriptions()
		if s.subscriptionSyncInterval != 0 {
			s.subSyncTicker = time.NewTicker(s.subscriptionSyncInterval)
			go func() {
				for _ = range s.subSyncTicker.C {
					s.linkSubscriptions()
				}
			}()
		}
		return err
	}
	return nil
}

func (s *influxdb) Close() error {
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

func (s *influxdb) Addr() string {
	config := s.configs[s.i]
	s.i = (s.i + 1) % len(s.configs)
	return config.Addr
}

func (s *influxdb) NewClient() (c client.Client, err error) {
	tries := 0
	for tries < len(s.configs) {
		tries++
		config := s.configs[s.i]
		s.i = (s.i + 1) % len(s.configs)
		c, err = client.NewHTTPClient(config)
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

func (s *influxdb) linkSubscriptions() error {
	s.logger.Println("D! linking subscriptions")
	b := backoff.NewExponentialBackOff()
	b.MaxElapsedTime = s.startupTimeout
	ticker := backoff.NewTicker(b)
	var err error
	var cli client.Client
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
		clusters := resp.Results[0].Series[0].Values
		for _, v := range clusters {
			clustername := v[0].(string)

			rpResp, err := s.execQuery(cli, &influxql.ShowRetentionPoliciesStatement{
				Database: clustername,
			})
			if err != nil {
				return err
			}
			if len(rpResp.Results) == 1 && len(rpResp.Results[0].Series) == 1 && len(rpResp.Results[0].Series[0].Values) > 0 {
				rps := rpResp.Results[0].Series[0].Values
				for _, v := range rps {
					rpname := v[0].(string)

					se := subEntry{
						cluster: clustername,
						rp:      rpname,
						name:    s.subName,
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
	existingSubs := make(map[subEntry]subInfo)
	for _, res := range resp.Results {
		for _, series := range res.Series {
			for _, v := range series.Values {
				se := subEntry{
					cluster: series.Name,
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
					err := s.dropSub(cli, se.name, se.cluster, se.rp)
					if err != nil {
						return err
					}
					se.name = s.subName
					err = s.createSub(cli, se.name, se.cluster, se.rp, si.Mode, si.Destinations)
					if err != nil {
						return err
					}
					existingSubs[se] = si
				} else if se.name == s.clusterID {
					// This is an just the cluster ID
					// drop it and recreate with new name.
					err := s.dropSub(cli, se.name, se.cluster, se.rp)
					se.name = s.subName
					err = s.createSub(cli, se.name, se.cluster, se.rp, si.Mode, si.Destinations)
					if err != nil {
						return err
					}
					existingSubs[se] = si
				} else if se.name == s.subName {
					if len(si.Destinations) == 0 {
						s.logger.Println("E! found subscription without any destinations:", se)
						continue
					}
					u, err := url.Parse(si.Destinations[0])
					if err != nil {
						s.logger.Println("E! found subscription with invalid destinations:", si)
						continue
					}
					host, port, err := net.SplitHostPort(u.Host)
					if err != nil {
						s.logger.Println("E! found subscription with invalid destinations:", si)
						continue
					}
					pn, err := strconv.ParseInt(port, 10, 64)
					if err != nil {
						s.logger.Println("E! found subscription with invalid destinations:", si)
						continue
					}
					// Check if the hostname, port or protocol have changed
					if host != s.hostname ||
						u.Scheme != s.protocol ||
						((u.Scheme == "http" || u.Scheme == "https") && int(pn) != s.httpPort) ||
						(s.useTokens && u.User == nil) {
						// Remove access for changing subscriptions.
						if u.User != nil {
							s.AuthService.RevokeSubscriptionAccess(u.User.Username())
						}
						// Something changed, drop the sub and let it get recreated
						s.dropSub(cli, se.name, se.cluster, se.rp)
					} else {
						existingSubs[se] = si
					}
				}
			}
		}
	}

	// Compare to configured list
	startedSubs := make(map[subEntry]bool)
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
						_, err := s.startUDPListener(se.cluster, se.rp, port)
						if err != nil {
							s.logger.Println("E! failed to start UDP listener:", err)
						}
					}
					startedSubs[se] = true
					s.runningSubs[se] = true
					break
				}
			}
		}
	}
	// create and start any new subscriptions
	for _, se := range allSubs {
		// If we have been configured to subscribe and the subscription is not started yet.
		if (s.configSubs[se] || all) && !startedSubs[se] && !s.exConfigSubs[se] && !s.runningSubs[se] {
			var destination string
			switch s.protocol {
			case "http", "https":
				if s.useTokens {
					// Generate token
					token, err := s.generateRandomToken()
					if err != nil {
						return errors.Wrap(err, "generating token")
					}
					err = s.AuthService.GrantSubscriptionAccess(token, se.cluster, se.rp)
					if err != nil {
						return err
					}
					destination = fmt.Sprintf("%s://%s@%s:%d", s.protocol, token, s.hostname, s.httpPort)
				} else {
					destination = fmt.Sprintf("%s://%s:%d", s.protocol, s.hostname, s.httpPort)
				}
			case "udp":
				addr, err := s.startUDPListener(se.cluster, se.rp, "0")
				if err != nil {
					s.logger.Println("E! failed to start UDP listener:", err)
				}
				destination = fmt.Sprintf("udp://%s:%d", s.hostname, addr.Port)
			}

			numSubscriptions++

			err = s.createSub(cli, se.name, se.cluster, se.rp, "ANY", []string{destination})
			if err != nil {
				return err
			}
			s.runningSubs[se] = true
		}
	}

	kapacitor.NumSubscriptionsVar.Set(numSubscriptions)
	return nil
}

func (s *influxdb) generateRandomToken() (string, error) {
	tokenBytes := make([]byte, tokenSize)
	if _, err := io.ReadFull(rand.Reader, tokenBytes); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(tokenBytes), nil
}

func (s *influxdb) createSub(cli client.Client, name, cluster, rp, mode string, destinations []string) (err error) {
	var buf bytes.Buffer
	for i, dst := range destinations {
		if i != 0 {
			buf.Write([]byte(", "))
		}
		buf.Write([]byte("'"))
		buf.Write([]byte(dst))
		buf.Write([]byte("'"))
	}
	_, err = s.execQuery(
		cli,
		&influxql.CreateSubscriptionStatement{
			Name:            name,
			Database:        cluster,
			RetentionPolicy: rp,
			Destinations:    destinations,
			Mode:            strings.ToUpper(mode),
		},
	)
	return

}
func (s *influxdb) dropSub(cli client.Client, name, cluster, rp string) (err error) {
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

func (s *influxdb) startUDPListener(cluster, rp, port string) (*net.UDPAddr, error) {
	c := udp.Config{}
	c.Enabled = true
	c.BindAddress = fmt.Sprintf("%s:%s", s.udpBind, port)
	c.Database = cluster
	c.RetentionPolicy = rp
	c.Buffer = s.udpBuffer
	c.ReadBuffer = s.udpReadBuffer

	l := s.LogService.NewLogger(fmt.Sprintf("[udp:%s.%s] ", cluster, rp), log.LstdFlags)
	service := udp.NewService(c, l)
	service.PointsWriter = s.PointsWriter
	err := service.Open()
	if err != nil {
		return nil, err
	}
	s.services = append(s.services, service)
	s.logger.Println("I! started UDP listener for", cluster, rp)
	return service.Addr(), nil
}

func (s *influxdb) execQuery(cli client.Client, q influxql.Statement) (*client.Response, error) {
	query := client.Query{
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
