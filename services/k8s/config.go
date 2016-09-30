package k8s

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"

	"github.com/influxdata/kapacitor/services/k8s/client"
	"github.com/pkg/errors"
)

type Config struct {
	Enabled    bool     `toml:"enabled" override:"enabled"`
	InCluster  bool     `toml:"in-cluster" override:"in-cluster"`
	APIServers []string `toml:"api-servers" override:"api-servers"`
	Token      string   `toml:"token" override:"token,redact"`
	CAPath     string   `toml:"ca-path" override:"ca-path"`
	Namespace  string   `toml:"namespace" override:"namespace"`
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
