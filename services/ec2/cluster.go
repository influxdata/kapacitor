package ec2

import (
	"sync/atomic"

	"github.com/influxdata/kapacitor/services/ec2/client"
	"github.com/pkg/errors"
)

type Cluster struct {
	configValue atomic.Value // Config
	client      client.Client
	diag        Diagnostic
}

func NewCluster(c Config, d Diagnostic) (*Cluster, error) {
	clientConfig, err := c.ClientConfig()
	if err != nil {
		return nil, errors.Wrap(err, "failed to create ec2 client config")
	}
	cli, err := client.New(clientConfig)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create ec2 client")
	}

	s := &Cluster{
		client: cli,
		diag:   d,
	}
	s.configValue.Store(c)
	return s, nil
}

func (s *Cluster) Open() error {
	return nil
}

func (s *Cluster) Close() error {
	return nil
}

func (s *Cluster) Update(c Config) error {
	s.configValue.Store(c)
	clientConfig, err := c.ClientConfig()
	if err != nil {
		return errors.Wrap(err, "failed to create ec2client config")
	}
	return s.client.Update(clientConfig)
}

func (s *Cluster) Test() error {
	cli, err := s.Client()
	if err != nil {
		return errors.Wrap(err, "failed to get client")
	}
	version, err := cli.Version()
	if err != nil {
		return errors.Wrap(err, "failed to query server version")
	}
	if version == "" {
		return errors.New("got empty version from server")
	}
	return nil
}
func (s *Cluster) config() Config {
	return s.configValue.Load().(Config)
}

func (s *Cluster) Client() (client.Client, error) {
	config := s.config()
	if !config.Enabled {
		return nil, errors.New("service is not enabled")
	}
	return s.client, nil
}
