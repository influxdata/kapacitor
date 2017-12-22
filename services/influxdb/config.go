package influxdb

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"time"

	"github.com/influxdata/influxdb/toml"
	"github.com/influxdata/kapacitor/services/stats"
	"github.com/influxdata/kapacitor/services/udp"
)

const (
	// Maximum time to try and connect to InfluxDB during startup.
	DefaultStartUpTimeout           = 5 * time.Minute
	DefaultSubscriptionSyncInterval = 1 * time.Minute

	DefaultSubscriptionProtocol = "http"
)

type SubscriptionMode int

const (
	// ClusterMode means that there will be one set of subscriptions per cluster.
	ClusterMode SubscriptionMode = iota
	// ServerMode means that there will be one set of subscriptions per server.
	ServerMode
)

type Config struct {
	Enabled  bool     `toml:"enabled" override:"enabled"`
	Name     string   `toml:"name" override:"name"`
	Default  bool     `toml:"default" override:"default"`
	URLs     []string `toml:"urls" override:"urls"`
	Username string   `toml:"username" override:"username"`
	Password string   `toml:"password" override:"password,redact"`
	// Path to CA file
	SSLCA string `toml:"ssl-ca" override:"ssl-ca"`
	// Path to host cert file
	SSLCert string `toml:"ssl-cert" override:"ssl-cert"`
	// Path to cert key file
	SSLKey string `toml:"ssl-key" override:"ssl-key"`
	// Use SSL but skip chain & host verification
	InsecureSkipVerify bool `toml:"insecure-skip-verify" override:"insecure-skip-verify"`

	Timeout                  toml.Duration       `toml:"timeout" override:"timeout"`
	DisableSubscriptions     bool                `toml:"disable-subscriptions" override:"disable-subscriptions"`
	SubscriptionProtocol     string              `toml:"subscription-protocol" override:"subscription-protocol"`
	SubscriptionMode         SubscriptionMode    `toml:"subscription-mode" override:"subscription-mode"`
	Subscriptions            map[string][]string `toml:"subscriptions" override:"subscriptions"`
	ExcludedSubscriptions    map[string][]string `toml:"excluded-subscriptions" override:"excluded-subscriptions"`
	KapacitorHostname        string              `toml:"kapacitor-hostname" override:"kapacitor-hostname"`
	HTTPPort                 int                 `toml:"http-port" override:"http-port"`
	UDPBind                  string              `toml:"udp-bind" override:"udp-bind"`
	UDPBuffer                int                 `toml:"udp-buffer" override:"udp-buffer"`
	UDPReadBuffer            int                 `toml:"udp-read-buffer" override:"udp-read-buffer"`
	StartUpTimeout           toml.Duration       `toml:"startup-timeout" override:"startup-timeout"`
	SubscriptionSyncInterval toml.Duration       `toml:"subscriptions-sync-interval" override:"subscriptions-sync-interval"`
}

func NewConfig() Config {
	c := &Config{}
	c.Init()
	c.Enabled = true
	return *c
}

func (c *Config) Init() {
	c.Name = "default"
	c.URLs = []string{"http://localhost:8086"}
	c.ExcludedSubscriptions = map[string][]string{
		stats.DefaultDatabse: []string{stats.DefaultRetentionPolicy},
	}
	c.UDPBuffer = udp.DefaultBuffer
	c.StartUpTimeout = toml.Duration(DefaultStartUpTimeout)
	c.SubscriptionProtocol = DefaultSubscriptionProtocol
	c.SubscriptionSyncInterval = toml.Duration(DefaultSubscriptionSyncInterval)
	c.SubscriptionMode = ClusterMode
}

func (c *Config) ApplyConditionalDefaults() {
	if c.UDPBuffer == 0 {
		c.UDPBuffer = udp.DefaultBuffer
	}
	if c.StartUpTimeout == 0 {
		c.StartUpTimeout = toml.Duration(DefaultStartUpTimeout)
	}
	if c.SubscriptionProtocol == "" {
		c.SubscriptionProtocol = DefaultSubscriptionProtocol
	}
	if c.SubscriptionSyncInterval == toml.Duration(0) {
		c.SubscriptionSyncInterval = toml.Duration(DefaultSubscriptionSyncInterval)
	}
}

var validNamePattern = regexp.MustCompile(`^[-\._\p{L}0-9]+$`)

func (c Config) Validate() error {
	if c.Name == "" {
		return errors.New("influxdb cluster must be given a name")
	}
	if !validNamePattern.MatchString(c.Name) {
		return errors.New("influxdb cluster name must contain only numbers, letters or '.', '-', and '_' characters")
	}
	if len(c.URLs) == 0 {
		return errors.New("must specify at least one InfluxDB URL")
	}
	for _, u := range c.URLs {
		_, err := url.Parse(u)
		if err != nil {
			return err
		}
		_, err = getTLSConfig(c.SSLCA, c.SSLCert, c.SSLKey, c.InsecureSkipVerify)
		if err != nil {
			return err
		}
	}
	switch c.SubscriptionProtocol {
	case "http", "https", "udp":
	default:
		return fmt.Errorf("invalid subscription protocol, must be one of 'udp', 'http' or 'https', got %q: %v", c.SubscriptionProtocol, c)
	}
	return nil
}

func (m SubscriptionMode) MarshalText() ([]byte, error) {
	switch m {
	case ClusterMode:
		return []byte("cluster"), nil
	case ServerMode:
		return []byte("server"), nil
	default:
		return nil, fmt.Errorf("unknown subscription mode %q", m)
	}
}
func (m *SubscriptionMode) UnmarshalText(text []byte) error {
	switch s := string(text); s {
	case "cluster":
		*m = ClusterMode
	case "server":
		*m = ServerMode
	default:
		return fmt.Errorf("unknown subscription mode %q", s)
	}
	return nil
}
