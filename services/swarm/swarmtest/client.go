package swarmtest

import (
	"github.com/docker/docker/api/types/swarm"
	"github.com/influxdata/kapacitor/services/swarm/client"
)

type Client struct {
	ServiceFunc       func(name string) (*swarm.Service, error)
	UpdateServiceFunc func(service *swarm.Service) error
}

func (c Client) Client(string) (client.Client, error) {
	return c, nil
}
func (Client) Update(client.Config) error {
	return nil
}
func (Client) Version() (string, error) {
	return "test client", nil
}

func (c Client) Service(name string) (*swarm.Service, error) {
	return c.ServiceFunc(name)
}
func (c Client) UpdateService(service *swarm.Service) error {
	return c.UpdateServiceFunc(service)
}
