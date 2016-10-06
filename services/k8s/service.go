package k8s

import (
	"log"

	"github.com/influxdata/kapacitor/services/k8s/client"
	"github.com/pkg/errors"
)

type Service struct {
	client client.Client
	logger *log.Logger
}

func NewService(c Config, l *log.Logger) (*Service, error) {
	clientConfig := c.ClientConfig()
	cli, err := client.New(clientConfig)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create k8s client")
	}

	return &Service{
		client: cli,
		logger: l,
	}, nil
}

func (s *Service) Open() error {
	return nil
}
func (s *Service) Close() error {
	return nil
}

func (s *Service) Client() client.Client {
	return s.client
}
