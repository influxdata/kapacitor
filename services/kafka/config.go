package kafka

import (
	"crypto/tls"
	"time"

	"github.com/influxdata/influxdb/toml"
	"github.com/influxdata/kapacitor/tlsconfig"
	"github.com/pkg/errors"
	kafka "github.com/segmentio/kafka-go"
)

const (
	DefaultTimeout      = 10 * time.Second
	DefaultBatchSize    = 100
	DefaultBatchTimeout = 1 * time.Second
	DefaultID           = "default"
)

type Config struct {
	Enabled bool `toml:"enabled" override:"enabled"`
	// ID is a unique identifier for this Kafka config
	ID string `toml:"id" override:"id"`
	// Brokers is a list of host:port addresses of Kafka brokers.
	Brokers []string `toml:"brokers" override:"brokers"`
	// Timeout on network operations with the brokers.
	// If 0 a default of 10s will be used.
	Timeout toml.Duration `toml:"timeout" override:"timeout"`
	// BatchSize is the number of messages that are batched before being sent to Kafka
	// If 0 a default of 100 will be used.
	BatchSize int `toml:"batch-size" override:"batch-size"`
	// BatchTimeout is the maximum amount of time to wait before flushing an incomplete batch.
	// If 0 a default of 1s will be used.
	BatchTimeout toml.Duration `toml:"batch-timeout" override:"batch-timeout"`
	// UseSSL enable ssl communication
	// Must be true for the other ssl options to take effect.
	UseSSL bool `toml:"use-ssl" override:"use-ssl"`
	// Path to CA file
	SSLCA string `toml:"ssl-ca" override:"ssl-ca"`
	// Path to host cert file
	SSLCert string `toml:"ssl-cert" override:"ssl-cert"`
	// Path to cert key file
	SSLKey string `toml:"ssl-key" override:"ssl-key"`
	// Use SSL but skip chain & host verification
	InsecureSkipVerify bool `toml:"insecure-skip-verify" override:"insecure-skip-verify"`
}

func NewConfig() Config {
	return Config{ID: DefaultID}
}

func (c Config) Validate() error {
	if !c.Enabled {
		return nil
	}
	// ID must not be empty
	if c.ID == "" {
		return errors.New("id must not be empty")
	}
	if len(c.Brokers) == 0 {
		return errors.New("no brokers specified, must provide at least one broker URL")
	}
	return nil
}

func (c *Config) ApplyConditionalDefaults() {
	if c.Timeout == 0 {
		c.Timeout = toml.Duration(DefaultTimeout)
	}
	if c.BatchSize == 0 {
		c.BatchSize = DefaultBatchSize
	}
	if c.BatchTimeout == 0 {
		c.BatchTimeout = toml.Duration(DefaultBatchTimeout)
	}
}

func (c Config) WriterConfig() (kafka.WriterConfig, error) {
	var tlsCfg *tls.Config
	if c.UseSSL {
		t, err := tlsconfig.Create(c.SSLCA, c.SSLCert, c.SSLKey, c.InsecureSkipVerify)
		if err != nil {
			return kafka.WriterConfig{}, err
		}
		tlsCfg = t
	}
	dialer := &kafka.Dialer{
		Timeout: time.Duration(c.Timeout),
		TLS:     tlsCfg,
	}
	return kafka.WriterConfig{
		Brokers:      c.Brokers,
		Balancer:     &kafka.LeastBytes{},
		Dialer:       dialer,
		ReadTimeout:  time.Duration(c.Timeout),
		WriteTimeout: time.Duration(c.Timeout),
		BatchTimeout: time.Duration(c.BatchTimeout),
	}, nil
}

type Configs []Config

func (cs Configs) Validate() error {
	for _, c := range cs {
		if err := c.Validate(); err != nil {
			return err
		}
	}
	return nil
}
