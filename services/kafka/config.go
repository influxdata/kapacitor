package kafka

import (
	"crypto/tls"
	"fmt"
	"time"

	"github.com/influxdata/influxdb/toml"
	"github.com/influxdata/kapacitor/tlsconfig"
	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
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

type WriteTarget struct {
	Topic              string
	PartitionById      bool
	PartitionAlgorithm string
}

func (c Config) WriterConfig(diagnostic Diagnostic, target WriteTarget) (kafka.WriterConfig, error) {
	if target.Topic == "" {
		return kafka.WriterConfig{}, errors.New("topic must not be empty")
	}
	var balancer kafka.Balancer
	if target.PartitionById {
		switch target.PartitionAlgorithm {
		case "crc32c", "":
			balancer = &kafka.CRC32Balancer{}
		case "murmur2":
			balancer = &kafka.Murmur2Balancer{}
		case "fnv-1a":
			balancer = &kafka.Hash{}
		default:
			return kafka.WriterConfig{}, fmt.Errorf("invalid partition algorithm: %q", target.PartitionAlgorithm)
		}
	} else {
		balancer = &kafka.LeastBytes{}
	}

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

	baseConfig := kafka.WriterConfig{
		Brokers:      c.Brokers,
		Dialer:       dialer,
		ReadTimeout:  time.Duration(c.Timeout),
		WriteTimeout: time.Duration(c.Timeout),
		BatchSize:    c.BatchSize,
		BatchTimeout: time.Duration(c.BatchTimeout),
		// Async=true allows internal batching of the messages to take place.
		// It also means that no errors will be captured from the WriteMessages method.
		// As such we track the WriteStats for errors and report them with Kapacitor's normal diagnostics.
		Async: true,
		ErrorLogger: kafka.LoggerFunc(func(s string, x ...interface{}) {
			diagnostic.Error("kafka client error", fmt.Errorf(s, x...))
		}),
		Topic:    target.Topic,
		Balancer: balancer,
	}

	return baseConfig, nil
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
