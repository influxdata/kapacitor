package k8s

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"net/url"

	"github.com/influxdata/kapacitor/listmap"
	"github.com/influxdata/kapacitor/services/k8s/client"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/config"
)

type Config struct {
	Enabled    bool     `toml:"enabled" override:"enabled"`
	ID         string   `toml:"id" override:"id"`
	InCluster  bool     `toml:"in-cluster" override:"in-cluster"`
	APIServers []string `toml:"api-servers" override:"api-servers"`
	Token      string   `toml:"token" override:"token,redact"`
	CAPath     string   `toml:"ca-path" override:"ca-path"`
	Namespace  string   `toml:"namespace" override:"namespace"`
	Resource   string   `toml:"resource" override:"resource"`
}

func NewConfig() Config {
	return Config{}
}

func (c Config) Validate() error {
	if !c.Enabled {
		return nil
	}
	if c.InCluster {
		if len(c.APIServers) > 0 {
			return errors.New("in-cluster specified, cannot also specify api-servers")
		}
		if c.Token != "" {
			return errors.New("in-cluster specified, cannot also specify token")
		}
		if c.CAPath != "" {
			return errors.New("in-cluster specified, cannot also specify ca-path")
		}
		if c.Namespace != "" {
			return errors.New("in-cluster specified, cannot also specify namespace")
		}
	} else if len(c.APIServers) == 0 {
		return errors.New("no api-servers specified, must provide at least one server URL")
	} else {
		for _, s := range c.APIServers {
			_, err := url.Parse(s)
			if err != nil {
				return err
			}
		}
	}
	if c.Resource != "" {
		switch c.Resource {
		case "node", "pod", "service", "endpoint":
		default:
			return errors.New("Resource must be one of node, pod, service, or endpoints")
		}

	}
	return nil
}

func (c Config) ClientConfig() (client.Config, error) {
	if c.InCluster {
		return client.NewConfigInCluster()
	}
	var t *tls.Config
	if c.CAPath != "" {
		t = &tls.Config{}
		caCert, err := ioutil.ReadFile(c.CAPath)
		if err != nil {
			return client.Config{}, errors.Wrapf(err, "failed to read ca-path %q", c.CAPath)
		}
		caCertPool := x509.NewCertPool()
		successful := caCertPool.AppendCertsFromPEM(caCert)
		if !successful {
			return client.Config{}, errors.New("failed to parse ca certificate as PEM encoded content")
		}
		t.RootCAs = caCertPool
	}
	return client.Config{
		URLs:      c.APIServers,
		Namespace: c.Namespace,
		Token:     c.Token,
		TLSConfig: t,
	}, nil
}

type Configs []Config

func (cs *Configs) UnmarshalTOML(data interface{}) error {
	return listmap.DoUnmarshalTOML(cs, data)
}

func (cs Configs) Validate() error {
	l := len(cs)
	for _, c := range cs {
		if err := c.Validate(); err != nil {
			return err
		}
		// ID must not be empty when we have more than one.
		if l > 1 && c.ID == "" {
			return errors.New("id must not be empty")
		}
	}
	return nil
}

// Prom writes the prometheus configuration for discoverer into ScrapeConfig
func (c Config) Prom(conf *config.ScrapeConfig) {
	if len(c.APIServers) == 0 {
		conf.ServiceDiscoveryConfig.KubernetesSDConfigs = []*config.KubernetesSDConfig{
			&config.KubernetesSDConfig{
				Role:        config.KubernetesRole(c.Resource),
				BearerToken: c.Token,
				TLSConfig: config.TLSConfig{
					CAFile: c.CAPath,
				},
			},
		}
		return
	}

	sds := make([]*config.KubernetesSDConfig, len(c.APIServers))
	for i, srv := range c.APIServers {
		url, _ := url.Parse(srv)
		sds[i] = &config.KubernetesSDConfig{
			APIServer: config.URL{
				URL: url,
			},
			Role:        config.KubernetesRole(c.Resource),
			BearerToken: c.Token,
			TLSConfig: config.TLSConfig{
				CAFile: c.CAPath,
			},
		}
	}
	conf.ServiceDiscoveryConfig.KubernetesSDConfigs = sds
}

// Service return discoverer type
func (c Config) Service() string {
	return "kubernetes"
}

// ServiceID returns the discoverers name
func (c Config) ServiceID() string {
	return c.ID
}
