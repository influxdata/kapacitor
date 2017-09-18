package k8s

import (
	"sync/atomic"

	"github.com/influxdata/kapacitor/services/k8s/client"
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
		return nil, errors.Wrap(err, "failed to create k8s client config")
	}
	cli, err := client.New(clientConfig)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create k8s client")
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
		return errors.Wrap(err, "failed to create k8s client config")
	}
	return s.client.Update(clientConfig)
}

func (s *Cluster) Test() error {
	cli, err := s.Client()
	if err != nil {
		return errors.Wrap(err, "failed to get client")
	}
	_, err = cli.Versions()
	if err != nil {
		return errors.Wrap(err, "failed to query server versions")
	}
	return nil
}

func (s *Cluster) Client() (client.Client, error) {
	config := s.configValue.Load().(Config)
	if !config.Enabled {
		return nil, errors.New("service is not enabled")
	}
	return s.client, nil
}
