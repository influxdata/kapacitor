package influxdb

import (
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/influxdata/influxdb/toml"
	"github.com/influxdata/kapacitor/services/stats"
	"github.com/influxdata/kapacitor/services/udp"
)

const (
	// Maximum time to try and connect to InfluxDB during startup.
	DefaultStartUpTimeout = time.Minute * 5

	DefaultSubscriptionProtocol = "http"
)

type Config struct {
	Enabled  bool     `toml:"enabled"`
	Name     string   `toml:"name"`
	Default  bool     `toml:"default"`
	URLs     []string `toml:"urls"`
	Username string   `toml:"username"`
	Password string   `toml:"password"`
	// Path to CA file
	SSLCA string `toml:"ssl-ca"`
	// Path to host cert file
	SSLCert string `toml:"ssl-cert"`
	// Path to cert key file
	SSLKey string `toml:"ssl-key"`
	// Use SSL but skip chain & host verification
	InsecureSkipVerify bool `toml:"insecure-skip-verify"`

	Timeout               toml.Duration       `toml:"timeout"`
	DisableSubscriptions  bool                `toml:"disable-subscriptions"`
	SubscriptionProtocol  string              `toml:"subscription-protocol"`
	Subscriptions         map[string][]string `toml:"subscriptions"`
	ExcludedSubscriptions map[string][]string `toml:"excluded-subscriptions"`
	UDPBind               string              `toml:"udp-bind"`
	UDPBuffer             int                 `toml:"udp-buffer"`
	UDPReadBuffer         int                 `toml:"udp-read-buffer"`
	StartUpTimeout        toml.Duration       `toml:"startup-timeout"`
}

func NewConfig() Config {
	return Config{
		Enabled: true,
		// Cannot initialize slice
		// See: https://github.com/BurntSushi/toml/pull/68
		//URLs:          []string{"http://localhost:8086"},
		Username:      "",
		Password:      "",
		Subscriptions: make(map[string][]string),
		ExcludedSubscriptions: map[string][]string{
			stats.DefaultDatabse: []string{stats.DefaultRetentionPolicy},
		},
		UDPBuffer:            udp.DefaultBuffer,
		StartUpTimeout:       toml.Duration(DefaultStartUpTimeout),
		SubscriptionProtocol: DefaultSubscriptionProtocol,
	}
}

func (c *Config) SetDefaultValues() {
	if c.UDPBuffer == 0 {
		c.UDPBuffer = udp.DefaultBuffer
	}
	if c.StartUpTimeout == toml.Duration(0) {
		c.StartUpTimeout = toml.Duration(DefaultStartUpTimeout)
	}
	if c.SubscriptionProtocol == "" {
		c.SubscriptionProtocol = DefaultSubscriptionProtocol
	}
}

func (c Config) Validate() error {
	if c.Name == "" {
		return errors.New("influxdb cluster must be given a name")
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
		return fmt.Errorf("invalid subscription protocol, must be one of 'udp', 'http' or 'https', got %s", c.SubscriptionProtocol)
	}
	return nil
}
