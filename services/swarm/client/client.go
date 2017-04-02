package client

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
)

type Config struct {
	URLs       []string
	APIVersion string
	TLSConfig  *tls.Config
}

type Client interface {
	Scales(name string) ScalesInterface
	Update(c Config) error
}

type httpClient struct {
	mu     sync.RWMutex
	config Config
	urls   []url.URL
	client *http.Client
	index  int32
}

func New(c Config) (Client, error) {
	urls, err := parseURLs(c.URLs)
	if err != nil {
		return nil, err
	}
	return &httpClient{
		config: c,
		urls:   urls,
		client: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: c.TLSConfig,
			},
		},
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

func (c *httpClient) Update(new Config) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	old := c.config
	c.config = new
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

type ScalesInterface interface {
	Get(name string) (*Service, error)
	Update(name string, service *Service) error
}

type Scales struct {
	c *httpClient
}

func (c *httpClient) Do(r http.Request) (*http.Response, error) {
	c.mu.RLock()
	u := c.pickURL(c.urls)
	client := c.client
	c.mu.RUnlock()
	r.URL.Host = u.Host
	r.URL.Scheme = u.Scheme

	resp, err := client.Do(&r)
	return resp, errors.Wrap(err, "swarm client request failed")
}

func (c *httpClient) Get(name string, service *Service) error {
	getpath := fmt.Sprintf("/%s/services/%s", c.config.APIVersion, name)
	r, err := http.NewRequest("GET", getpath, nil)
	if err != nil {
		return errors.Wrapf(err, "failed create GET request for %q", getpath)
	}

	resp, err := c.Do(*r)

	if err != nil {
		return errors.Wrapf(err, "failed create GET request for %q", getpath)
	}
	defer resp.Body.Close()
	return c.decodeResponse(resp, service)
}

func (c *httpClient) Post(name string, service *Service) error {
	servicepost, err := json.Marshal(service.Spec)
	if err != nil {
		return errors.Wrapf(err, "failed to marshal service object %s", name)
	}

	updatepath := fmt.Sprintf("/%s/services/%s/update?", c.config.APIVersion, name)
	params := make(url.Values)
	params.Set("version", strconv.FormatUint(service.Version.Index, 10))

	r, err := http.NewRequest("POST", updatepath+params.Encode(), bytes.NewBuffer(servicepost))
	if err != nil {
		return errors.Wrapf(err, "failed to create post request for %s", name)
	}

	resp, err := c.Do(*r)
	if err != nil {
		return errors.Wrapf(err, "failed create POST request for %q", updatepath)
	}

	defer resp.Body.Close()
	return c.decodeResponse(resp, nil)

}

func (c *httpClient) decodeResponse(resp *http.Response, service *Service) error {
	body, _ := ioutil.ReadAll(resp.Body)

	if resp.StatusCode != 200 {
		err := &ServiceError{}
		return errors.Wrapf(err, string(body))
	}

	if service != nil {
		// Decode response body into provided response object
		if err := json.Unmarshal(body, &service); err != nil {
			return errors.Wrapf(err, "failed to unmarshal swarm server response into %T: Code: %d", service, resp.StatusCode)
		}
	}
	return nil
}

func (c *httpClient) Scales(name string) ScalesInterface {
	return Scales{c: c}
}

type ServiceError struct {
	Name string
	Err  error
}

func (err *ServiceError) Error() string {
	if err.Err != nil {
		return err.Err.Error()
	}
	return "Service Error: " + err.Name
}

func (s Scales) Get(name string) (*Service, error) {
	service := &Service{}
	err := s.c.Get(name, service)
	if err != nil {
		return service, errors.Wrapf(err, "failed to inspect %s", name)
	}
	return service, nil
}

func (s Scales) Update(name string, service *Service) error {
	err := s.c.Post(name, service)
	if err != nil {
		return errors.Wrapf(err, "failed to update service %s", name)
	}

	return nil
}
