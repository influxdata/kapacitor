package kafka

import (
	"fmt"
	"time"

	kafka "github.com/IBM/sarama"
	"github.com/influxdata/influxdb/toml"
	"github.com/influxdata/kapacitor/tlsconfig"
	"github.com/pkg/errors"
	"github.com/spaolacci/murmur3"
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
	// Authentication using SASL
	SASLAuth
}

func NewConfig() Config {
	return Config{ID: DefaultID, SASLAuth: SASLAuth{SASLOAUTHExpiryMargin: 1 * time.Second}}
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

func (c Config) writerConfig(diagnostic Diagnostic, target WriteTarget) (*kafka.Config, error) {
	cfg := kafka.NewConfig()

	if target.Topic == "" {
		return cfg, errors.New("topic must not be empty")
	}
	var partitioner kafka.PartitionerConstructor
	if target.PartitionById {
		switch target.PartitionAlgorithm {
		case "crc32", "":
			// can't use kafka.NewCustomHashPartitioner, because we need to maintain compatibility
			partitioner = newCRCPartitioner
		case "murmur2":
			// can't use kafka.NewCustomHashPartitioner here either, because we need to maintain compatibility
			partitioner = newMurmur2
		case "murmur3":
			partitioner = kafka.NewCustomHashPartitioner(murmur3.New32)
		case "fnv-1a":
			partitioner = kafka.NewHashPartitioner
		default:
			return cfg, fmt.Errorf("invalid partition algorithm: %q", target.PartitionAlgorithm)
		}
		cfg.Producer.Partitioner = partitioner
	}

	cfg.Producer.Return.Errors = true // so we can display errors
	cfg.Producer.Return.Successes = false
	cfg.ClientID = c.ID
	cfg.Metadata.Full = false // we only want to grab metadata for the topics we care about

	if c.UseSSL {
		var err error
		cfg.Net.TLS.Enable = true
		cfg.Net.TLS.Config, err = tlsconfig.Create(c.SSLCA, c.SSLCert, c.SSLKey, c.InsecureSkipVerify)
		if err != nil {
			return nil, err
		}
	}
	if c.Timeout > 0 {
		cfg.Net.DialTimeout = time.Duration(c.Timeout)
		cfg.Net.WriteTimeout = time.Duration(c.Timeout)
		cfg.Net.ReadTimeout = time.Duration(c.Timeout)
	}

	cfg.Producer.Flush.MaxMessages = c.BatchSize
	if cfg.Producer.Flush.MaxMessages <= 0 {
		cfg.Producer.Flush.MaxMessages = DefaultBatchSize
	}
	cfg.Producer.Flush.Frequency = time.Duration(c.BatchTimeout)

	// SASL
	if err := c.SASLAuth.SetSASLConfig(cfg); err != nil {
		return nil, err
	}
	return cfg, cfg.Validate()
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
