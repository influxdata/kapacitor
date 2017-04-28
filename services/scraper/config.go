package scraper

import (
	"net/url"
	"time"

	"github.com/influxdata/influxdb/toml"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
)

// Config is the scraper configuration
type Config struct {
	Enabled bool `toml:"enabled" override:"enabled"`
	// The job name to which the job label is set by default.
	Name            string `toml:"name" override:"name"`
	Database        string `toml:"db" override:"db"`
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
	// The CA cert to use for the targets.
	CAFile string `toml:"ca-file" override:"ca-file"`
	// The client cert file for the targets.
	CertFile string `toml:"cert-file" override:"cert-file"`
	// The client key file for the targets.
	KeyFile string `toml:"key-file" override:"key-file"`
	// Used to verify the hostname for the targets.
	ServerName string `toml:"server-name" override:"server-name"`
	// Disable target certificate validation.
	InsecureSkipVerify bool `toml:"insecure-skip-verify" override:"insecure-skip-verify"`
	// The bearer token for the targets.
	BearerToken string `toml:"bearer-token" override:"bearer-token"`
	// HTTP proxy server to use to connect to the targets.
	ProxyURL *url.URL `toml:"proxy-url" override:"proxy-url"`
	// DiscoverID is the id of the discoverer that generates hosts for the scraper
	DiscoverID string `toml:"discoverer-id" override:"discoverer-id"`
	// DiscoverService is the type of the discoverer that generates hosts for the scraper
	DiscoverService string `toml:"discoverer-service" override:"discoverer-service"`
}

// NewConfig creates a new configuration with default values
func NewConfig() Config {
	return Config{
		Enabled:        false,
		ScrapeInterval: toml.Duration(time.Minute),
		ScrapeTimeout:  toml.Duration(10 * time.Second),
		MetricsPath:    "/metrics",
		Scheme:         "http",
		Name:           "name",
	}
}

// Init adds default values to Config scraper
func (c *Config) Init() {
	c.ScrapeInterval = toml.Duration(time.Minute)
	c.ScrapeTimeout = toml.Duration(10 * time.Second)
	c.MetricsPath = "/metrics"
	c.Scheme = "http"
}

// Validate validates the configuration of the Scraper
func (c *Config) Validate() error {
	return nil
}

// Prom generates the prometheus configuration for the scraper
func (c *Config) Prom() *config.ScrapeConfig {
	sc := &config.ScrapeConfig{
		JobName:        c.Name,
		Scheme:         c.Scheme,
		MetricsPath:    c.MetricsPath,
		Params:         c.Params,
		ScrapeInterval: model.Duration(c.ScrapeInterval),
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
				CAFile:             c.CAFile,
				CertFile:           c.CertFile,
				KeyFile:            c.KeyFile,
				ServerName:         c.ServerName,
				InsecureSkipVerify: c.InsecureSkipVerify,
			},
		},
	}
	return sc
}

type KubernetesRole string

const (
	KubernetesRoleNode     = "node"
	KubernetesRolePod      = "pod"
	KubernetesRoleService  = "service"
	KubernetesRoleEndpoint = "endpoints"
)

// Kubernetes is Kubernetes service discovery configuration
type Kubernetes struct {
	APIServer url.URL        `toml:"api_server" override:"api_server"`
	Role      KubernetesRole `toml:"role" override:"role"`
}

func (k Kubernetes) Prom(c *config.ScrapeConfig) {
	// TODO: auth token tls
	c.ServiceDiscoveryConfig.KubernetesSDConfigs = []*config.KubernetesSDConfig{
		&config.KubernetesSDConfig{
			APIServer: config.URL{
				URL: &k.APIServer,
			},
			Role: config.KubernetesRole(k.Role),
		},
	}
}

func NewKubernetes() Kubernetes {
	return Kubernetes{}
}
