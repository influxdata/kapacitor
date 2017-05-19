package scraper

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/influxdata/influxdb/toml"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
)

// Config is the scraper configuration
type Config struct {
	Enabled bool `toml:"enabled" override:"enabled"`
	// The job name to which the job label is set by default.
	Name string `toml:"name" override:"name"`
	// Type of the scraper
	Type string `toml:"type" override:"type"`
	// Database this data will be associated with
	Database string `toml:"db" override:"db"`
	// RetentionPolicyt this data will be associated with
	RetentionPolicy string `toml:"rp" override:"rp"`
	// The URL scheme with which to fetch metrics from targets.
	Scheme string `toml:"scheme" override:"scheme"`
	// The HTTP resource path on which to fetch metrics from targets.
	MetricsPath string `toml:"metrics-path" override:"metrics-path"`
	// A set of query parameters with which the target is scraped.
	Params url.Values `toml:"params" override:"params"`
	// How frequently to scrape the targets of this scrape config.
	ScrapeInterval toml.Duration `toml:"scrape-interval" override:"scrape-interval"`
	// The timeout for scraping targets of this config.
	ScrapeTimeout toml.Duration `toml:"scrape-timeout" override:"scrape-timeout"`
	// The HTTP basic authentication credentials for the targets.
	Username string `toml:"username" override:"username"`
	Password string `toml:"password" override:"password,redact"`

	// Path to CA file
	SSLCA string `toml:"ssl-ca" override:"ssl-ca"`
	// Path to host cert file
	SSLCert string `toml:"ssl-cert" override:"ssl-cert"`
	// Path to cert key file
	SSLKey string `toml:"ssl-key" override:"ssl-key"`
	// SSLServerName is used to verify the hostname for the targets.
	SSLServerName string `toml:"ssl-server-name" override:"ssl-server-name"`
	// Use SSL but skip chain & host verification
	InsecureSkipVerify bool `toml:"insecure-skip-verify" override:"insecure-skip-verify"`

	// The bearer token for the targets.
	BearerToken string `toml:"bearer-token" override:"bearer-token,redact"`
	// HTTP proxy server to use to connect to the targets.
	ProxyURL *url.URL `toml:"proxy-url" override:"proxy-url"`
	// DiscoverID is the id of the discoverer that generates hosts for the scraper
	DiscoverID string `toml:"discoverer-id" override:"discoverer-id"`
	// DiscoverService is the type of the discoverer that generates hosts for the scraper
	DiscoverService string `toml:"discoverer-service" override:"discoverer-service"`

	// Blacklist is a list of hosts to ignore and not scrape
	Blacklist []string `toml:"blacklist" override:"blacklist"`
}

// Init adds default values to Config scraper
func (c *Config) Init() {
	c.Type = "prometheus"
	c.ScrapeInterval = toml.Duration(time.Minute)
	c.ScrapeTimeout = toml.Duration(10 * time.Second)
	c.MetricsPath = "/metrics"
	c.Scheme = "http"
}

// Validate validates the configuration of the Scraper
func (c *Config) Validate() error {
	if c.Name == "" {
		return fmt.Errorf("scraper config must be given a name")
	}
	if c.Database == "" {
		return fmt.Errorf("scraper config must be given a db")
	}
	if c.RetentionPolicy == "" {
		return fmt.Errorf("scraper config must be given an rp")
	}
	if c.Type != "prometheus" {
		return fmt.Errorf("Unknown scraper type")
	}

	return nil
}

// Prom generates the prometheus configuration for the scraper
func (c *Config) Prom() *config.ScrapeConfig {
	sc := &config.ScrapeConfig{
		JobName:        encodeJobName(c.Database, c.RetentionPolicy, c.Name),
		Scheme:         c.Scheme,
		MetricsPath:    c.MetricsPath,
		Params:         c.Params,
		ScrapeInterval: model.Duration(c.ScrapeInterval),
		ScrapeTimeout:  model.Duration(c.ScrapeTimeout),
		HTTPClientConfig: config.HTTPClientConfig{
			BasicAuth: &config.BasicAuth{
				Username: c.Username,
				Password: c.Password,
			},
			BearerToken: c.BearerToken,
			ProxyURL: config.URL{
				URL: c.ProxyURL,
			},
			TLSConfig: config.TLSConfig{
				CAFile:             c.SSLCA,
				CertFile:           c.SSLCert,
				KeyFile:            c.SSLKey,
				ServerName:         c.SSLServerName,
				InsecureSkipVerify: c.InsecureSkipVerify,
			},
		},
	}
	return sc
}

func encodeJobName(db, rp, name string) string {
	// Because I cannot add label information to my scraped targets
	// I'm abusing the JobName by encoding database, retention policy,
	// and name.
	return fmt.Sprintf("%s|%s|%s", db, rp, name)
}

func decodeJobName(job string) (string, string, string, error) {
	split := strings.Split(job, "|")
	if len(split) != 3 {
		return "", "", "", fmt.Errorf("unable to decode job name %q", job)
	}
	return split[0], split[1], split[2], nil
}
