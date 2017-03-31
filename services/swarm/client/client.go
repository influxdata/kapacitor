package client

import (
        "crypto/tls"
	"net/http"
	"net/url"
	"sync"
 	"sync/atomic"
        "fmt"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"github.com/docker/docker/api/types"
         swarmclient "github.com/docker/docker/api/types/swarm"
	"github.com/docker/docker/client"
)

type Config struct {
	URLs []string
        APIVersion string
	TLSConfig *tls.Config
}

type Client interface {
	Get(name string) (swarmclient.Service, error)
	Update(scale swarmclient.Service) error
        Updateconfig(c Config) error
}

type httpClient struct {
	mu     sync.RWMutex
	config Config
	urls   []url.URL
	client *http.Client
	index int32
}

func New(c Config) (Client, error) {
	urls, err := parseURLs(c.URLs)
	if err != nil {
		return nil, err
	}
	return &httpClient{
		config: c,
		urls:   urls,
	}, nil
}

func (c *httpClient) pickURL(urls []url.URL) url.URL {
	i := atomic.LoadInt32(&c.index)
	i = (i + 1) % int32(len(urls))
	atomic.StoreInt32(&c.index, i)
	return urls[i]
}

func parseURLs(urlStrs []string) ([]url.URL, error) {
	urls := make([]url.URL, len(urlStrs))
        for i, urlStr := range urlStrs {
		u, err := url.Parse(urlStr)
		if err != nil {
			return nil, errors.Wrapf(err, "invalid url %q", urlStr)
		}
		urls[i] = *u
	}
	return urls, nil
}

func (c *httpClient) Updateconfig(new Config) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	old := c.config
	c.config = new
	//if c.config.Namespace == "" {
	//	c.config.Namespace = NamespaceDefault
	//}
	// Replace urls
	urls, err := parseURLs(new.URLs)
	if err != nil {
		return err
	}
	c.urls = urls

	if old.TLSConfig != new.TLSConfig {
		c.client = &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: new.TLSConfig,
			},
		}
	}
	return nil
}

func (c *httpClient) getCli() (*client.Client, error) {
    u := c.pickURL(c.urls)
    apiversion := c.config.APIVersion
    swarmhttpclient := c.client
    cli, err := client.NewClient(u.String(), apiversion, swarmhttpclient, nil)
    if err != nil {
        return nil, errors.Wrap(err, "unable to get swarm client")
    }
    return cli, nil
}

func (c *httpClient) Get(name string) (swarmclient.Service, error) {
        cli, err := c.getCli()
        if err != nil {
            return swarmclient.Service{}, errors.Wrap(err, "swarm client request failed")
        }
	service, _, err := cli.ServiceInspectWithRaw(context.Background(), name)
	if err != nil {
		return swarmclient.Service{}, errors.Wrap(err, "swarm service request failed")
	}
	return service, nil
}

func (c *httpClient) Update(service swarmclient.Service) error {
        cli, err := c.getCli()
        if err != nil {
            return errors.Wrap(err, "swarm client request failed")
        }
	serviceupdate, err := cli.ServiceUpdate(context.Background(), service.ID, service.Version, service.Spec, types.ServiceUpdateOptions{})
	if err != nil {
		return err
        }
        fmt.Println(serviceupdate)
	return nil
}
