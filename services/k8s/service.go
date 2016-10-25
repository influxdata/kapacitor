package k8s

import (
	"fmt"
	"log"
	"sync/atomic"

	"github.com/influxdata/kapacitor/services/k8s/client"
	"github.com/pkg/errors"
)

type Service struct {
	configValue atomic.Value // Config
	client      client.Client
	logger      *log.Logger
}

func NewService(c Config, l *log.Logger) (*Service, error) {
	clientConfig, err := c.ClientConfig()
	if err != nil {
		return nil, errors.Wrap(err, "failed to create k8s client config")
	}
	cli, err := client.New(clientConfig)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create k8s client")
	}

	s := &Service{
		client: cli,
		logger: l,
	}
	s.configValue.Store(c)
	return s, nil
}

func (s *Service) Open() error {
	return nil
}
func (s *Service) Close() error {
	return nil
}

func (s *Service) Update(newConfig []interface{}) error {
	if l := len(newConfig); l != 1 {
		return fmt.Errorf("expected only one new config object, got %d", l)
	}
	c, ok := newConfig[0].(Config)
	if !ok {
		return fmt.Errorf("expected config object to be of type %T, got %T", c, newConfig[0])
	}

	s.configValue.Store(c)
	clientConfig, err := c.ClientConfig()
	if err != nil {
		return errors.Wrap(err, "failed to create k8s client config")
	}
	return s.client.Update(clientConfig)
}

func (s *Service) TestOptions() interface{} {
	return nil
}

func (s *Service) Test(options interface{}) error {
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

func (s *Service) Client() (client.Client, error) {
	config := s.configValue.Load().(Config)
	if !config.Enabled {
		return nil, errors.New("service is not enabled")
	}
	return s.client, nil
}
