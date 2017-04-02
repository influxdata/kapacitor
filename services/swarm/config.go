package swarm

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"

	"github.com/influxdata/kapacitor/services/swarm/client"
	"github.com/pkg/errors"
)

type Config struct {
	Enabled            bool     `toml:"enabled" override:"enabled"`
	APIServers         []string `toml:"api-servers" override:"api-servers"`
	CAFile             string   `toml:"ca-file" override:"ca-path"`
	CertFile           string   `toml:"cert-file" override:"cert-file"`
	KeyFile            string   `toml:"key-file" override:"key-file"`
	APIVersion         string   `toml:"api-version" override:"api-version"`
	InsecureSkipVerify bool     `toml:"insecure-skip-verify" override:"insecure-skip-verify"`
}

func NewConfig() Config {
	return Config{}
}

func (c Config) Validate() error {
	if !c.Enabled {
		return nil
	}

	if len(c.APIServers) == 0 {
		return errors.New("no api-servers specified, must provide at least one server URL")
	}

	if len(c.APIVersion) == 0 {
		return errors.New("no api-version specified")
	}

	return nil
}

func (c Config) ClientConfig() (client.Config, error) {
	var t *tls.Config

	if !c.InsecureSkipVerify && c.CAFile != "" {
		t = &tls.Config{}
		caCert, err := ioutil.ReadFile(c.CAFile)
		if err != nil {
			return client.Config{}, errors.Wrapf(err, "failed to read ca-path %q", c.CAFile)
		}
		caCertPool := x509.NewCertPool()
		successful := caCertPool.AppendCertsFromPEM(caCert)
		if !successful {
			return client.Config{}, errors.New("failed to parse ca certificate as PEM encoded content")
		}
		t.RootCAs = caCertPool
	}
	if c.CertFile != "" || c.KeyFile != "" {
		tlsCert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
		if err != nil {
			return client.Config{}, errors.Wrapf(err, "Could not load X509 key pair: %v. Make sure the key is not encrypted")
		}
		t.Certificates = []tls.Certificate{tlsCert}
	}
	return client.Config{
		URLs:       c.APIServers,
		APIVersion: c.APIVersion,
		TLSConfig:  t,
	}, nil
}
