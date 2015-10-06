package influxdb

import (
	"net/url"

	"github.com/influxdb/influxdb/client"
)

// Handles requests to write or read from an InfluxDB cluster
type Service struct {
	configs []client.Config
	i       int
}

func NewService(c Config) *Service {
	configs := make([]client.Config, len(c.URLs))
	for i, u := range c.URLs {
		host, _ := url.Parse(u)
		configs[i] = client.Config{
			URL:       *host,
			Username:  c.Username,
			Password:  c.Password,
			UserAgent: "Kapacitor",
			Timeout:   c.Timeout,
			Precision: c.Precision,
		}
	}
	return &Service{configs: configs}
}

func (s *Service) Open() error {
	return nil
}

func (s *Service) Close() error {
	return nil
}

func (s *Service) NewClient() (c *client.Client, err error) {

	tries := 0
	for tries < len(s.configs) {
		tries++
		config := s.configs[s.i]
		s.i = (s.i + 1) % len(s.configs)
		c, err = client.NewClient(config)
		if err != nil {
			continue
		}
		_, _, err = c.Ping()
		if err != nil {
			continue
		}
		return
	}
	return
}
