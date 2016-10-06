package k8s

import "github.com/influxdata/kapacitor/services/k8s/client"

type Config struct {
	Enabled    bool     `toml:"enabled"`
	APIServers []string `toml:"api-servers"`
}

func NewConfig() Config {
	return Config{}
}

func (c Config) ClientConfig() client.Config {
	return client.Config{
		URLs: c.APIServers,
	}
}
