package consul

import (
	"fmt"
	"strings"

	config2 "github.com/prometheus/common/config"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/discovery/consul"
)

// Config is a Consul service discovery configuration
type Config struct {
	Enabled      bool     `toml:"enabled" override:"enabled"`
	ID           string   `toml:"id" override:"id"`
	Address      string   `toml:"address" override:"address"`
	Token        string   `toml:"token" override:"token,redact"`
	Datacenter   string   `toml:"datacenter" override:"datacenter"`
	TagSeparator string   `toml:"tag-separator" override:"tag-separator"`
	Scheme       string   `toml:"scheme" override:"scheme"`
	Username     string   `toml:"username" override:"username"`
	Password     string   `toml:"password" override:"password,redact"`
	Services     []string `toml:"services" override:"services"`
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
}

// Init adds defaults to Consul
func (c *Config) Init() {
	c.Address = "127.0.0.1:8500"
	c.TagSeparator = ","
	c.Scheme = "http"
}

// Validate validates the consul configuration
func (c Config) Validate() error {
	if c.ID == "" {
		return fmt.Errorf("consul discovery must be given a ID")
	}
	if strings.TrimSpace(c.Address) == "" {
		return fmt.Errorf("consul discovery requires a server address")
	}
	return nil
}

// Prom writes the prometheus configuration for discoverer into ScrapeConfig
func (c Config) Prom(conf *config.ScrapeConfig) {
	conf.ServiceDiscoveryConfigs = append(conf.ServiceDiscoveryConfigs, c.PromConfig())
}

// PromConfig returns the prometheus configuration for this discoverer
func (c Config) PromConfig() *consul.SDConfig {
	sd := consul.DefaultSDConfig
	sd.Server = c.Address
	sd.Token = config2.Secret(c.Token)
	sd.Datacenter = c.Datacenter
	sd.TagSeparator = c.TagSeparator
	sd.Scheme = c.Scheme
	// Prometheus only honors the consul username and password by converting
	// them to basic auth while unmarshaling its YAML config, so do the same.
	if c.Username != "" || c.Password != "" {
		sd.HTTPClientConfig.BasicAuth = &config2.BasicAuth{
			Username: c.Username,
			Password: config2.Secret(c.Password),
		}
	}
	sd.Services = c.Services
	sd.HTTPClientConfig.TLSConfig = config2.TLSConfig{
		CAFile:             c.SSLCA,
		CertFile:           c.SSLCert,
		KeyFile:            c.SSLKey,
		ServerName:         c.SSLServerName,
		InsecureSkipVerify: c.InsecureSkipVerify,
	}
	return &sd
}

// Service return discoverer type
func (c Config) Service() string {
	return "consul"
}

// ServiceID returns the discoverers name
func (c Config) ServiceID() string {
	return c.ID
}
